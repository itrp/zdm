require 'spec_helper'

describe Zdm do

  before(:example) {
    Zdm.io = false
    Zdm.cleanup

    conn = ActiveRecord::Base.connection
    conn.execute(%[TRUNCATE people])
    conn.execute(%[INSERT INTO people(account_id, name, code, created_at) VALUES (10,'foo','bar','2017-03-01 23:59:59')])
    conn.execute(%[INSERT INTO people(account_id, name, code, created_at) VALUES (20,'foo2','bar2','2017-03-02 23:59:59')])
  }

  it 'requires an autoincrement primary key `id` field' do
    expect{Zdm.change_table(:people_teams) {}}.to raise_error('Cannot migrate table `people_teams`, missing auto increment primary key `id`')
  end

  it 'sends output to stderr' do
    Zdm.io = nil
    filename = "test_stderr.#{$$}.log"
    at_exit { File.unlink(filename) rescue nil }
    orig_err = STDERR.dup
    STDERR.reopen(filename, 'a')
    STDERR.sync = true
    begin
      Zdm.change_table(:people) {}
      expect(File.read(filename).strip).to eq('people: Completed (0 secs)')
    ensure
      STDERR.reopen(orig_err)
    end
  end

  it 'migrates live tables' do
    Zdm.change_table(:people) do |m|
      m.alter("DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci")
      m.add_column('test', "varchar(32) DEFAULT 'foo'")
      m.change_column('name', 'varchar(99) NOT NULL')
    end

    conn = ActiveRecord::Base.connection
    stmt = conn.select_rows('show create table people')[0][1]
    expect(stmt.squish).to eq(<<-EOS.squish)
      CREATE TABLE `people` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `account_id` int(11) DEFAULT NULL,
        `name` varchar(99) COLLATE utf8_unicode_ci NOT NULL,
        `code` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `created_at` datetime DEFAULT NULL,
        `test` varchar(32) COLLATE utf8_unicode_ci DEFAULT 'foo',
        PRIMARY KEY (`id`), UNIQUE KEY `index_people_on_name` (`name`),
        KEY `index_people_on_account_id_and_code` (`account_id`,`code`(191)) USING BTREE,
        KEY `index_people_on_created_at` (`created_at`) USING BTREE
      ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
    EOS

    archive_tables = conn.send(Zdm.tables_method).select { |name| name.starts_with?('zdma_') }
    expect(archive_tables.length).to eq(1)
    rows = conn.select_rows("SELECT * FROM #{archive_tables[0]}")
    expect(rows).to eq([
      [1, 10, 'foo', 'bar', '2017-03-01 23:59:59 UTC'],
      [2, 20, 'foo2', 'bar2', '2017-03-02 23:59:59 UTC']
    ])

    rows = conn.select_rows("SELECT * FROM `people`")
    expect(rows).to eq([
      [1, 10, 'foo', 'bar', '2017-03-01 23:59:59 UTC', 'foo'],
      [2, 20, 'foo2', 'bar2', '2017-03-02 23:59:59 UTC', 'foo']
    ])

    Zdm.change_table(:people) do |m|
      m.alter("DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
      m.remove_column('test')
      m.change_column('name', 'varchar(30)')
    end

    stmt = conn.select_rows('show create table people')[0][1]
    expect(stmt.squish).to eq(<<-EOS.squish)
        CREATE TABLE `people` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `account_id` int(11) DEFAULT NULL,
          `name` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `code` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `created_at` datetime DEFAULT NULL, PRIMARY KEY (`id`),
          UNIQUE KEY `index_people_on_name` (`name`),
          KEY `index_people_on_account_id_and_code` (`account_id`,`code`(191)) USING BTREE,
          KEY `index_people_on_created_at` (`created_at`) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      EOS

    archive_tables = conn.send(Zdm.tables_method).select { |name| name.starts_with?('zdma_') }
    expect(archive_tables.length).to eq(2)
  end

  context 'execute_in_batches' do
    before(:example) do
      @conn = ActiveRecord::Base.connection
      (1..20).each do |idx|
        @conn.execute(%[INSERT INTO people(account_id, name, code, created_at) VALUES (10,'person-#{idx}','P#{idx}','2017-03-01 23:59:59')])
      end
      Zdm.io = StringIO.new
      @sql = "UPDATE people SET code = CONCAT(code, 'U') WHERE id BETWEEN %s AND %s"
    end

    after(:example) do
      @conn.execute(%[DELETE FROM people WHERE name LIKE 'person%'])
    end

    it 'updates a table in batches' do
      Zdm.execute_in_batches('people', batch_size: 4, progress_every: 1) do |batch_start, batch_end|
        sleep(0.6)
        @sql % [batch_start, batch_end]
      end
      expect(Zdm.io.string).to eq(%[people: 36.36% (8/22)\npeople: 72.73% (16/22)\npeople: Completed (3 secs)\n])
      expect(@conn.select_value(%[SELECT COUNT(*) FROM people WHERE code LIKE '%U'])).to eq(22)
    end

    it 'updates part of a table in batches' do
      batches = []
      Zdm.execute_in_batches('people', start: 5, finish: 18, batch_size: 4, progress_every: 1) do |batch_start, batch_end|
        sleep(0.6)
        batches << @sql % [batch_start, batch_end]
        @sql % [batch_start, batch_end]
      end
      expect(Zdm.io.string).to eq(%[people: 57.14% (8/14)\npeople: Completed (2 secs)\n])
      expect(batches).to eq([
        %[UPDATE people SET code = CONCAT(code, 'U') WHERE id BETWEEN 5 AND 8],
        %[UPDATE people SET code = CONCAT(code, 'U') WHERE id BETWEEN 9 AND 12],
        %[UPDATE people SET code = CONCAT(code, 'U') WHERE id BETWEEN 13 AND 16],
        %[UPDATE people SET code = CONCAT(code, 'U') WHERE id BETWEEN 17 AND 18],
      ])
      expect(@conn.select_values(%[SELECT code FROM people WHERE code LIKE '%U'])).to eq([
        'P3U', 'P4U', 'P5U', 'P6U', 'P7U', 'P8U', 'P9U', 'P10U', 'P11U', 'P12U', 'P13U', 'P14U', 'P15U', 'P16U'
      ])
    end

  end


end
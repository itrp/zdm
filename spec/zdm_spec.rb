require 'spec_helper'

describe Zdm do

  before(:example) {
    Zdm.io = false
    Zdm.cleanup
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
        KEY `index_people_on_account_id_and_code` (`account_id`,`code`(191)) USING BTREE
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
          KEY `index_people_on_account_id_and_code` (`account_id`,`code`(191)) USING BTREE
        ) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      EOS

    archive_tables = conn.send(Zdm.tables_method).select { |name| name.starts_with?('zdma_') }
    expect(archive_tables.length).to eq(2)
  end

end
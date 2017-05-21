module Zdm
  require 'version'

  class << self
    attr_accessor :io

    def change_table(name, &block)
      table = Table.new(name)
      yield table
      Migrator.new(table).migrate!
      cleanup if defined?(Rails) && Rails.env.development?
    end

    def cleanup(before: nil)
      zdm_tables = connection.send(tables_method).select { |name| name.starts_with?('zdm_') }
      zdm_tables.each { |name| Migrator.new(Table.new(name.sub(/^zdm_/, ''))).cleanup }

      zdm_archive_tables = connection.send(tables_method).select { |name| name.starts_with?('zdma_') }
      if before
        zdm_archive_tables.select! { |table|
          Time.strptime(table, 'zdma_%Y%m%d_%H%M%S%N') <= before
        }
      end
      zdm_archive_tables.each { |name| execute('DROP TABLE `%s`' % name) }
    end

    def tables_method
      ActiveRecord.version.to_s =~ /^5/ ? :data_sources : :tables
    end

    BATCH_SIZE = 40_000
    DECREASE_THROTTLER = 4  # seconds
    DECREASE_SIZE = 5_000
    MIN_BATCH_SIZE = 10_000
    PROGRESS_EVERY = 30  # seconds
    def execute_in_batches(table_name, start: nil, finish: nil, batch_size: BATCH_SIZE, progress_every: PROGRESS_EVERY, &block)
      min = start || connection.select_value('SELECT MIN(`id`) FROM %s' % table_name)
      return unless min

      max = finish || connection.select_value('SELECT MAX(`id`) FROM %s' % table_name)
      todo = max - min + 1
      return unless todo > 0

      batch_end = min - 1
      start_time = last_progress = Time.now
      while true
        batch_start = batch_end + 1
        batch_end = [batch_start + batch_size - 1, max].min
        start_batch_time = Time.now

        sql = yield batch_start, batch_end
        execute(sql) if sql

        if $exit
          write(table_name, 'Received SIGTERM, exiting...')
          cleanup
          exit 1
        end

        # The end!
        break if batch_end >= max

        # Throttle
        current_time = Time.now
        if (current_time - start_batch_time) > DECREASE_THROTTLER
          batch_size = [(batch_size - DECREASE_SIZE).to_i, MIN_BATCH_SIZE].max
        end

        # Periodically show progress
        if (current_time - last_progress) >= progress_every
          last_progress = current_time
          done = batch_end - min + 1
          write(table_name, "%.2f%% (#{done}/#{todo})" % (done.to_f / todo * 100.0))
        end
      end

      duration = Time.now - start_time
      duration = (duration < 2*60) ? "#{duration.to_i} secs" : "#{(duration / 60).to_i} mins"
      write(table_name, "Completed (#{duration})")
    end

    private

    def connection
      ActiveRecord::Base.connection
    end

    def execute(stmt)
      connection.execute(stmt)
    end

    def write(table_name, msg)
      return if Zdm.io == false
      io = Zdm.io || $stderr
      io.puts("#{table_name}: #{msg}")
      io.flush
    end
  end

  class Table
    attr_reader :origin, :copy, :archive, :statements

    def initialize(name)
      @origin = name
      @copy = "zdm_#{name}"
      @archive = "zdma_#{Time.now.strftime("%Y%m%d_%H%M%S%N")}_#{name}"[0..64]
      @statements = []
    end

    def ddl(statement)
      @statements << statement
    end

    def alter(definition)
      ddl('ALTER TABLE `%s` %s' % [@copy, definition])
    end

    def add_column(name, definition)
      ddl('ALTER TABLE `%s` ADD COLUMN `%s` %s' % [@copy, name, definition])
    end

    def change_column(name, definition)
      ddl('ALTER TABLE `%s` MODIFY COLUMN `%s` %s' % [@copy, name, definition])
    end

    def remove_column(name)
      ddl('ALTER TABLE `%s` DROP `%s`' % [@copy, name])
    end

    def rename_column(old_name, new_name)
      raise "Unsupported: you must first run a migration adding the column `#{new_name}`, deploy the code live, then run another migration at a later time to remove the column `#{old_name}`"
    end
  end

  class Migrator
    attr_reader :table

    def initialize(table)
      @table = table
    end

    def migrate!
      validate
      set_session_lock_wait_timeouts
      cleanup
      create_copy_table
      # drop_copy_indexes
      apply_ddl_statements
      create_triggers
      copy_in_batches
      # create_copy_indexes
      atomic_switcharoo!
    ensure
      cleanup
    end

    def cleanup
      drop_triggers
      execute('DROP TABLE IF EXISTS `%s`' % table.copy)
    end

    private

      def connection
        ActiveRecord::Base.connection
      end

      def execute(stmt)
        connection.execute(stmt)
      end

      def columns(table)
        connection.columns(table).map(&:name)
      end

      def common_columns
        @common_columns ||= (columns(table.origin) & columns(table.copy))
      end

      def validate
        unless connection.columns(table.origin).detect {|c| c.name == 'id'}&.extra == 'auto_increment'
          raise 'Cannot migrate table `%s`, missing auto increment primary key `id`' % table.origin
        end
      end

      LOCK_WAIT_TIMEOUT_DELTA = -2  # seconds
      def set_session_lock_wait_timeouts
        timeout = connection.select_one("SHOW GLOBAL VARIABLES LIKE 'innodb_lock_wait_timeout'")
        if timeout
          execute('SET SESSION innodb_lock_wait_timeout=%d' % (timeout['Value'].to_i + LOCK_WAIT_TIMEOUT_DELTA))
        end
      end

      def create_copy_table
        execute('CREATE TABLE `%s` LIKE `%s`' % [table.copy, table.origin])
      end

      def apply_ddl_statements
        table.statements.each { |statement| execute(statement) }
      end

      def atomic_switcharoo!
        execute('RENAME TABLE `%s` to `%s`, `%s` to `%s`' % [table.origin, table.archive, table.copy, table.origin])
      end

      def create_triggers
        create_delete_trigger
        create_insert_trigger
        create_update_trigger
      end

      def create_delete_trigger
        execute(<<-SQL.squish)
          CREATE TRIGGER `#{trigger_name(:del)}`
          AFTER DELETE ON `#{table.origin}` FOR EACH ROW
          DELETE IGNORE FROM `#{table.copy}` WHERE `#{table.copy}`.`id` = `OLD`.`id`
        SQL
      end

      def create_insert_trigger
        execute(<<-SQL.squish)
          CREATE TRIGGER `#{trigger_name(:ins)}`
          AFTER INSERT ON `#{table.origin}` FOR EACH ROW
          REPLACE INTO `#{table.copy}` SET #{trigger_column_setters}
        SQL
      end

      def create_update_trigger
        execute(<<-SQL.squish)
          CREATE TRIGGER `#{trigger_name(:upd)}`
          AFTER UPDATE ON `#{table.origin}` FOR EACH ROW
          REPLACE INTO `#{table.copy}` SET #{trigger_column_setters}
        SQL
      end

      def trigger_column_setters
        common_columns.map { |name| "`#{name}`=`NEW`.`#{name}`"}.join(', ')
      end

      def drop_triggers
        execute('DROP TRIGGER IF EXISTS `%s`' % trigger_name(:del))
        execute('DROP TRIGGER IF EXISTS `%s`' % trigger_name(:ins))
        execute('DROP TRIGGER IF EXISTS `%s`' % trigger_name(:upd))
      end

      def trigger_name(trigger_type)
        "zdmt_#{trigger_type}_#{table.origin}"[0...64]
      end

      # Drop indexes to speed up copy_in_batches.
      #
      # "Online DDL support for adding secondary indexes means that you can
      # generally speed the overall process of creating and loading a table
      # and associated indexes by creating the table without any secondary
      # indexes, then adding the secondary indexes after the data is loaded."
      # https://dev.mysql.com/doc/refman/5.7/en/innodb-create-index-overview.html#idm140602200949744
      def drop_copy_indexes
        @indexes = connection.indexes(table.copy).reject(&:unique)
        @indexes.each do |index_def|
          execute('ALTER TABLE `%s` DROP INDEX `%s`' % [table.copy, index_def.name])
        end
      end

      # Recreate the indexes previously dropped, using 1 statement so the table
      # is read through once.
      def create_copy_indexes
        return if @indexes.empty?
        indexes = @indexes.map do |index_def|
          lengths = if index_def.lengths.is_a?(Hash)
            index_def.lengths
          elsif index_def.lengths.compact.any?
            Hash[index_def.columns.map.with_index { |col, idx| [col, index_def.lengths[idx]] }]
          end
          index_columns = index_def.columns.map.with_index do |col, idx|
            index_col_name = '`%s`' % col
            column_length = lengths && lengths[col]
            index_col_name << '(%s)' % column_length if column_length
            index_col_name
          end
          "ADD INDEX `#{index_def.name}` (#{index_columns.join(',')}) USING #{index_def.using}"
        end
        execute('ALTER TABLE `%s` %s' % [table.copy, indexes.join(', ')])
      end

      def copy_in_batches
        insert_columns = common_columns.map {|c| "`#{c}`"}.join(', ')
        select_columns = common_columns.map {|c| "`#{table.origin}`.`#{c}`"}.join(', ')
        sql = <<-SQL.squish
          INSERT IGNORE INTO `#{table.copy}` (#{insert_columns})
          SELECT #{select_columns}
          FROM `#{table.origin}`
          WHERE `#{table.origin}`.`id` BETWEEN %s AND %s
        SQL

        Zdm.execute_in_batches(table.origin) do |batch_start, batch_end|
          sql % [batch_start, batch_end]
        end
      end

  end
end
trap('TERM') { $exit = true }
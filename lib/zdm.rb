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
      conn = ActiveRecord::Base.connection
      zdm_tables = conn.send(tables_method).select { |name| name.starts_with?('zdm_') }
      zdm_tables.each { |name| Migrator.new(Table.new(name.sub(/^zdm_/, ''))).cleanup }

      zdm_archive_tables = conn.send(tables_method).select { |name| name.starts_with?('zdma_') }
      if before
        zdm_archive_tables.select! { |table|
          Time.strptime(table, 'zdma_%Y%m%d_%H%M%S%N') <= before
        }
      end
      zdm_archive_tables.each { |name| conn.execute('DROP TABLE `%s`' % name) }
    end

    def tables_method
      ActiveRecord.version.to_s =~ /^5/ ? :data_sources : :tables
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
      create_destination_table
      drop_destination_indexes
      apply_ddl_statements
      create_triggers
      batched_copy
      create_destination_indexes
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

      def create_destination_table
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

      # Drop indexes to speed up batched_copy
      def drop_destination_indexes
        @indexes = connection.indexes(table.copy).reject(&:unique)
        @indexes.each do |index_def|
          execute('ALTER TABLE `%s` DROP INDEX `%s`' % [table.copy, index_def.name])
        end
      end

      # Recreate the indexes previously dropped
      def create_destination_indexes
        @indexes.each do |index_def|
          opts = { name: index_def.name, using: index_def.using }
          if index_def.lengths.compact.any?
            opts[:length] = Hash[index_def.columns.map.with_index { |col, idx| [col, index_def.lengths[idx]] }]
          end
          connection.add_index(table.copy, index_def.columns, opts)
        end
      end

      BATCH_SIZE = 40_000
      DECREASE_THROTTLER = 4  # seconds
      DECREASE_SIZE = 5_000
      MIN_BATCH_SIZE = 10_000
      PROGRESS_EVERY = 30  # seconds
      def batched_copy
        min = connection.select_value('SELECT MIN(`id`) FROM %s' % table.origin)
        return unless min

        max = connection.select_value('SELECT MAX(`id`) FROM %s' % table.origin)
        todo = max - min + 1

        insert_columns = common_columns.map {|c| "`#{c}`"}.join(', ')
        select_columns = common_columns.map {|c| "`#{table.origin}`.`#{c}`"}.join(', ')

        batch_size = BATCH_SIZE
        batch_end = min - 1
        start_time = last_progress = Time.now
        while true
          batch_start = batch_end + 1
          batch_end = [batch_start + batch_size - 1, max].min
          start_batch_time = Time.now

          execute(<<-SQL.squish)
            INSERT IGNORE INTO `#{table.copy}` (#{insert_columns})
            SELECT #{select_columns}
            FROM `#{table.origin}`
            WHERE `#{table.origin}`.`id` BETWEEN #{batch_start} AND #{batch_end}
          SQL

          if $exit
            write('Received SIGTERM, exiting...')
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
          if (current_time - last_progress) >= PROGRESS_EVERY
            last_progress = current_time
            done = batch_end - min + 1
            write("%.2f%% (#{done}/#{todo})" % (done.to_f / todo * 100.0))
          end
        end

        duration = Time.now - start_time
        duration = (duration < 2*60) ? "#{duration.to_i} secs" : "#{(duration / 60).to_i} mins"
        write("Completed (#{duration})")
      end

      def write(msg)
        return if Zdm.io == false
        io = Zdm.io || $stderr
        io.puts("#{table.origin}: #{msg}")
        io.flush
      end
  end
end
trap('TERM') { $exit = true }
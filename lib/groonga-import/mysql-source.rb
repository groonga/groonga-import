# Copyright (C) 2021  Sutou Kouhei <kou@clear-code.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require "fileutils"
require "json"

require "groonga/command"
require "mysql2"
begin
  require "mysql2-replication"
rescue LoadError
end
require "mysql_binlog"

module GroongaImport
  class MySQLSource
    def initialize(dir: ".", output: $stdout)
      config = Config.new(File.join(dir, "config.yaml"))
      @config = config.mysql
      @mapping = config.mapping
      @secret = Config.new(File.join(dir, "secret.yaml")).mysql
      @status = Status.new(File.join(dir, "status.yaml")).mysql
      @binlog_dir = File.join(dir, "binlog")
      @output = output
      @tables = {}
    end

    def import
      case ENV["GROONGA_IMPORT_MYSQL_SOURCE_BACKEND"]
      when "mysqlbinlog"
        import_mysqlbinlog
      when "mysql2-replication"
        import_mysql2_replication
      else
        if Object.const_defined?(:Mysql2Replication)
          import_mysql2_replication
        else
          import_mysqlbinlog
        end
      end
    end

    private
    def import_mysqlbinlog
      file, position = read_current_status
      FileUtils.mkdir_p(@binlog_dir)
      local_file = File.join(@binlog_dir, file)
      mysqlbinlog_start_file = file
      loop do
        next_file = mysqlbinlog_start_file.succ
        next_local_file = File.join(@binlog_dir, next_file)
        break unless File.exist?(next_local_file)
        mysqlbinlog_start_file = next_file
      end
      command_line = ["mysqlbinlog"]
      command_line << "--host=#{@config.host}" if @config.host
      command_line << "--port=#{@config.port}" if @config.port
      command_line << "--socket=#{@config.socket}" if @config.socket
      if @config.replication_slave_user
        command_line << "--user=#{@config.replication_slave_user}"
      end
      password = @secret.replication_slave_password ||
                 @config.replication_slave_password
      command_line << "--password=#{password}" if password
      command_line << "--read-from-remote-server"
      command_line << "--stop-never"
      command_line << "--raw"
      command_line << "--result-file=#{@binlog_dir}/"
      command_line << mysqlbinlog_start_file
      spawn_process(*command_line) do |pid, output_read, error_read|
        reader = MysqlBinlog::BinlogFileReader.new(local_file)
        reader.tail = true
        binlog = MysqlBinlog::Binlog.new(reader)
        binlog.checksum = @config.checksum
        binlog.each_event do |event|
          next if event[:position] < position
          case event[:type]
          when :rotate_event
            file = event[:event][:name]
            position = event[:event][:pos]
          when :write_rows_event_v1,
               :write_rows_event_v2,
               :update_rows_event_v1,
               :update_rows_event_v2,
               :delete_rows_event_v1,
               :delete_rows_event_v2
            normalized_type = event[:type].to_s.gsub(/_v\d\z/, "").to_sym
            import_rows_event(normalized_type,
                              event[:event][:table][:db],
                              event[:event][:table][:table],
                              file,
                              event[:header][:next_position]) do
              case normalized_type
              when :write_rows_event,
                   :update_rows_event
                event[:event][:row_image].collect do |row_image|
                  build_row(row_image[:after][:image])
                end
              when :delete_rows_event
                event[:event][:row_image].collect do |row_image|
                  build_row(row_image[:before][:image])
                end
              end
            end
            position = event[:header][:next_position]
          end
        end
      end
    end

    def import_mysql2_replication
      file, position = read_current_status
      mysql(@config.replication_slave_user,
            @secret.replication_slave_password ||
            @config.replication_slave_password) do |client|
        replication_client = Mysql2Replication::Client.new(client)
        replication_client.file_name = file
        replication_client.start_position = position
        replication_client.open do
          replication_client.each do |event|
            case event
            when Mysql2Replication::RotateEvent
              file = event.file_name unless event.next_position.zero?
            when Mysql2Replication::RowsEvent
              event_name = event.class.name.split("::").last
              normalized_type =
              event_name.scan(/[A-Z][a-z]+/).
                collect(&:downcase).
                join("_").
                to_sym
              import_rows_event(normalized_type,
                                event.table_map.database,
                                event.table_map.table,
                                file,
                                event.next_position) do
                case normalized_type
                when :update_rows_event
                  event.updated_rows
                else
                  event.rows
                end
              end
            end
          end
        end
      end
    end

    def import_rows_event(type,
                          database_name,
                          table_name,
                          file,
                          next_position,
                          &block)
      table = find_table(database_name, table_name)
      groonga_table = @mapping.groonga_table(table_name)
      return if groonga_table.nil?

      target_rows = block.call
      groonga_records = target_rows.collect do |row|
        record = build_record(table, row)
        @mapping.generate_groonga_record(table_name, record)
      end
      return if groonga_records.empty?

      case type
      when :write_rows_event,
           :update_rows_event
        @output.puts("load --table #{groonga_table}")
        @output.puts("[")
        @output.puts(groonga_records.collect(&:to_json).join(",\n"))
        @output.puts("]")
      when :delete_rows_event
        groonga_records.each do |groonga_record|
          delete = Groonga::Command::Delete.new
          delete[:table] = groonga_table
          delete[:key] = groonga_record[:_key].to_s
          @output.puts(delete.to_command_format)
        end
      end
      @status.update("file" => file,
                     "position" => next_position)
    end

    def spawn_process(*command_line)
      env = {
        "LC_ALL" => "C",
      }
      output_read, output_write = IO.pipe
      error_read, error_write = IO.pipe
      options = {
        :out => output_write,
        :err => error_write,
      }
      pid = spawn(env, *command_line, options)
      output_write.close
      error_write.close
      if block_given?
        begin
          yield(pid, output_read, error_read)
        rescue
          begin
            Process.kill(:TERM, pid)
            _, status = Process.waitpid(pid)
          rescue SystemCallError
          end
        ensure
          begin
            _, status = Process.waitpid2(pid)
          rescue SystemCallError
          else
            unless status.success?
              message = "Failed to run: #{command_line.join(' ')}\n"
              message << "--- output ---\n"
              message << output_read.read
              message << "--------------\n"
              message << "--- error ----\n"
              message << error_read.read
              message << "--------------\n"
              raise message
            end
          end
          output_read.close unless output_read.closed?
          error_read.close unless error_read.closed?
        end
      else
        [pid, output_read, error_read]
      end
    end

    def mysql(user, password)
      options = {}
      options[:host] = @config.host if @config.host
      options[:port] = @config.port if @config.port
      options[:socket] = @config.socket if @config.socket
      options[:username] = user if user
      options[:password] = password if password
      yield(Mysql2::Client.new(**options))
    end

    def read_current_status
      if @status.file
        [@status.file, @status.position]
      else
        file = nil
        position = 0
        mysql(@config.replication_client_user,
              @secret.replication_client_password ||
              @config.replication_client_password) do |client|
          result = client.query("SHOW MASTER STATUS").first
          file = result["File"]
          position = result["Position"]
        end
        [file, position]
      end
    end

    def find_table(database_name, table_name)
      return @tables[table_name] if @tables.key?(table_name)

      mysql(@config.select_user,
            @secret.select_password || @config.select_password) do |client|
        statement = client.prepare(<<~SQL)
          SELECT column_name,
                 ordinal_position,
                 data_type,
                 column_key
          FROM information_schema.columns
          WHERE
            table_schema = ? AND
            table_name = ?
        SQL
        results = statement.execute(database_name, table_name)
        columns = results.collect do |column|
          {
            name: column["column_name"],
            ordinal_position: column["ordinal_position"],
            data_type: column["data_type"],
            is_primary_key: column["column_key"] == "PRI",
          }
        end
        @tables[table_name] = columns.sort_by do |column|
          column[:ordinal_position]
        end
      end
    end

    def build_row(value_pairs)
      row = {}
      value_pairs.each do |value_pair|
        value_pair.each do |column_index, value|
          row[column_index] = value
        end
      end
      row
    end

    def build_record(table, row)
      record = {}
      row.each do |column_index, value|
        record[table[column_index][:name].to_sym] = value
      end
      record
    end
  end
end

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

require "json"

require "groonga/command"
require "mysql2"
require "mysql_binlog"

module GroongaImport
  class MySQLSource
    def initialize(dir: ".", output: $stdout)
      config = Config.new(File.join(dir, "config.yaml"))
      @config = config.mysql
      @mapping = config.mapping
      @secret = Config.new(File.join(dir, "secret.yaml")).mysql
      @status = Status.new(File.join(dir, "status.yaml")).mysql
      @output = output
      @tables = {}
    end

    def import
      file, position = read_current_status
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
      command_line << "--start-position=#{position}" unless position.zero?
      command_line << "--read-from-remote-server"
      command_line << "--raw"
      command_line << "--result-file=#{position}-"
      command_line << file
      spawn_process(*command_line) do |pid, output_read, error_read|
        _, status = Process.waitpid2(pid)
        unless status.success?
          message = "Failed to read binlog: #{command_line.join(' ')}\n"
          message << error_read.read
          raise message
        end
      end
      local_file = "#{position}-#{file}"
      if position.zero?
        reader = MysqlBinlog::BinlogFileReader.new(local_file)
      else
        reader = PartialFileReader.new(local_file)
      end
      binlog = MysqlBinlog::Binlog.new(reader)
      binlog.checksum = @config.checksum
      binlog.each_event do |event|
        case event[:type]
        when :write_rows_event_v1,
             :write_rows_event_v2,
             :update_rows_event_v1,
             :update_rows_event_v2
          table_name = event[:event][:table][:table]
          table = find_table(event[:event][:table][:db],
                             table_name)
          groonga_table = @mapping.groonga_table(table_name)
          next if groonga_table.nil?
          groonga_records = event[:event][:row_image].collect do |row_image|
            record = build_record(table,
                                  event[:event][:table][:columns],
                                  row_image[:after][:image])
            @mapping.generate_groonga_record(table_name, record)
          end
          next if groonga_records.empty?
          @output.puts("load --table #{groonga_table}")
          @output.puts("[")
          @output.puts(groonga_records.collect(&:to_json).join(",\n"))
          @output.puts("]")
        when :delete_rows_event_v1,
             :delete_rows_event_v2
          table_name = event[:event][:table][:table]
          table = find_table(event[:event][:table][:db],
                             table_name)
          groonga_table = @mapping.groonga_table(table_name)
          next if groonga_table.nil?
          groonga_records = event[:event][:row_image].collect do |row_image|
            record = build_record(table,
                                  event[:event][:table][:columns],
                                  row_image[:before][:image])
            @mapping.generate_groonga_record(table_name, record)
          end
          groonga_records.each do |groonga_record|
            delete = Groonga::Command::Delete.new
            delete[:table] = groonga_table
            delete[:key] = groonga_record[:_key]
            @output.puts(delete.to_command_format)
          end
        end
      end
    end

    private
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
        ensure
          output_read.close unless output_read.closed?
          error_read.close unless error_read.closed?
          begin
            Process.waitpid2(pid)
          rescue SystemCallError
          end
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
          # position = Integer(result["Position"], 10)
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

    def build_record(table,
                     record_columns,
                     record_values)
      record = {}
      record_values.each do |value_pair|
        value_pair.each do |column_index, value|
          record[table[column_index][:name].to_sym] = value
        end
      end
      record
    end

    class PartialFileReader < MysqlBinlog::BinlogFileReader
      def verify_magic
      end

      def rewind
        seek(0)
      end
    end
  end
end

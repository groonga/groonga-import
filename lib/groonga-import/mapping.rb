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

module GroongaImport
  class Mapping
    def initialize(data)
      @data = data
      build_source_databases
    end

    def source_databases
      @source_databases
    end

    def [](source_database_name, source_table_name=nil)
      if source_table_name.nil?
        @source_databases_index[source_database_name]
      else
        @source_tables_index[[source_database_name, source_table_name]]
      end
    end

    private
    def build_source_databases
      raw_source_databases = {}
      @data.each do |groonga_table_name, details|
        (details["sources"] || []).each do |source|
          raw_groonga_columns = source["columns"]
          groonga_columns = []
          source_column_names = []
          raw_groonga_columns.each do |name, template|
            if template.is_a?(Hash)
              options = template
              template = options["template"]
              type = options["type"]
            else
              type = nil
            end
            groonga_columns << GroongaColumn.new(name, template, type)
            template.scan(/%{(.*?)}/).flatten.each do |source_column_name|
              source_column_names << source_column_name.to_sym
            end
          end
          source_column_names.uniq!
          groonga_table = GroongaTable.new(groonga_table_name,
                                           groonga_columns)
          source_table = SourceTable.new(source["table"],
                                         source_column_names,
                                         groonga_table)
          source_tables = (raw_source_databases[source["database"]] ||= [])
          source_tables << source_table
        end
      end
      @source_databases = []
      @source_databases_index = {}
      @source_tables_index = {}
      raw_source_databases.each do |source_database_name, source_tables|
        source_database = SourceDatabase.new(source_database_name,
                                             source_tables)
        @source_databases << source_database
        @source_databases_index[source_database.name] = source_database
        source_tables.each do |source_table|
          @source_tables_index[[source_database.name, source_table.name]] =
            source_table
        end
      end
    end

    class SourceDatabase
      attr_reader :name
      attr_reader :source_tables
      def initialize(name, source_tables)
        @name = name
        @source_tables = source_tables
      end
    end

    class SourceTable
      attr_reader :name
      attr_reader :source_column_names
      attr_reader :groonga_table
      def initialize(name, source_column_names, groonga_table)
        @name = name
        @source_column_names = source_column_names
        @groonga_table = groonga_table
      end
    end

    class GroongaTable
      attr_reader :name
      attr_reader :groonga_columns
      def initialize(name, groonga_columns)
        @name = name
        @groonga_columns = groonga_columns
      end

      def generate_record(source_record)
        record = {}
        @groonga_columns.each do |groonga_column|
          value = groonga_column.generate_value(source_record)
          record[groonga_column.name.to_sym] = value
        end
        record
      end
    end

    class GroongaColumn
      attr_reader :name
      attr_reader :template
      attr_reader :type
      def initialize(name, template, type)
        @name = name
        @template = template
        @type = type
      end

      def generate_value(source_record)
        cast(@template % source_record)
      end

      private
      def cast(value)
        case @type
        when nil, "ShortText", "Text", "LongText"
          value
        when /\AU?Int(?:8|16|32|64)\z/
          return 0 if value.empty?
          Integer(value, 10)
        when "Float"
          return 0.0 if value.empty?
          Float(value)
        when "Bool"
          return false if value.empty?
          case value
          when "0"
            false
          else
            true
          end
        else
          raise "Unknown type: #{@type}"
        end
      end
    end
  end
end

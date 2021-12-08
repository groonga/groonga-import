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
      build_index
    end

    def groonga_table(mysql_name)
      (@index[mysql_name] || {})[:groonga_table]
    end

    def generate_groonga_record(mysql_name, mysql_data)
      columns = (@index[mysql_name] || {})[:columns]
      return nil if columns.nil?
      record = {}
      columns.each do |name, options|
        if options.is_a?(String)
          value_template = options
          type = nil
        else
          value_template = options["template"]
          type = options["type"]
        end
        value = value_template % mysql_data
        record[name.to_sym] = cast(value, type)
      end
      record
    end

    def mysql
      MySQL.new(self, @data["mysql"] || {})
    end

    private
    def build_index
      # MySQL -> Groonga
      @index = {}
      @data.each do |groonga_table_name, details|
        (details["sources"] || []).each do |source|
          @index[source["table"]] = {
            groonga_table: groonga_table_name,
            columns: source["columns"],
          }
        end
      end
    end

    def cast(value, type)
      case type
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
        raise "Unknown type: #{type}"
      end
    end
  end
end

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
      columns.each do |name, value_template|
        record[name.to_sym] = value_template % mysql_data
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
  end
end

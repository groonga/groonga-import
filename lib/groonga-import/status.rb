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

require "yaml"

module GroongaImport
  class Status
    def initialize(path)
      @path = path
      if File.exist?(path)
        @data = YAML.load(File.read(path))
      else
        @data = {}
      end
    end

    def update(data)
      @data.update(data)
      File.open(@path, "w") do |output|
        output.puts(YAML.dump(@data))
      end
    end

    def mysql
      MySQL.new(self, @data["mysql"] || {})
    end

    class MySQL
      def initialize(config, data)
        @config = config
        @data = data
      end

      def update(data)
        @config.update("mysql" => data)
      end

      def file
        @data["file"]
      end

      def position
        @data["position"]
      end
    end
  end
end

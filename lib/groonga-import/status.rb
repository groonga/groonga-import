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
require "yaml"

module GroongaImport
  class Status
    def initialize(dir)
      @dir = dir
      @path = File.join(@dir, "status.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
    end

    def [](key)
      @data[key]
    end

    def update(data)
      @data.update(data)
      FileUtils.mkdir_p(@dir)
      File.open(@path, "w") do |output|
        output.puts(YAML.dump(@data))
      end
    end

    def mysql
      MySQL.new(self)
    end

    def local
      Local.new(self)
    end

    class MySQL
      def initialize(status)
        @status = status
      end

      def [](key)
        (@status["mysql"] || {})[key]
      end

      def update(new_data)
        @status.update("mysql" => new_data)
      end

      def file
        self["file"]
      end

      def position
        self["position"]
      end
    end

    class Local
      def initialize(status)
        @status = status
      end

      def [](key)
        (@status["local"] || {})[key]
      end

      def update(new_data)
        @status.update("local" => new_data)
      end

      def number
        self["number"]
      end
    end
  end
end

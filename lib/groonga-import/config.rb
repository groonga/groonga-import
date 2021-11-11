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

require_relative "mapping"

module GroongaImport
  class Config
    def initialize(path)
      @data = YAML.load(File.read(path))
    end

    def mysql
      MySQL.new(@data["mysql"] || {})
    end

    def mapping
      Mapping.new(@data["mapping"] || {})
    end

    class MySQL
      def initialize(data)
        @data = data
      end

      def host
        @data["host"] || "127.0.0.1"
      end

      def port
        @data["port"] || 3306
      end

      def user
        @data["user"]
      end

      def password
        @data["password"]
      end

      def replication_client
        @data["replication_client"] || @data
      end

      def replication_client_user
        replication_client["user"]
      end

      def replication_client_password
        replication_client["password"]
      end

      def replication_slave
        @data["replication_slave"] || @data
      end

      def replication_slave_user
        replication_slave["user"]
      end

      def replication_slave_password
        replication_slave["password"]
      end

      def select
        @data["select"] || @data
      end

      def select_user
        select["user"]
      end

      def select_password
        select["password"]
      end

      def checksum
        _checksum = @data["checksum"]
        return nil if _checksum.nil?
        _checksum.to_sym
      end
    end
  end
end

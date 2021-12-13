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
require "logger"
require "yaml"

require_relative "mapping"

module GroongaImport
  class Config
    def initialize(path)
      @path = path
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
    end

    def mysql
      MySQL.new(@data["mysql"] || {})
    end

    def mapping
      Mapping.new(@data["mapping"] || {})
    end

    def binlog_dir
      @data["binlog_dir"] || "binlog"
    end

    def delta_dir
      @data["delta_dir"] || "delta"
    end

    def logger
      @logger ||= create_logger
    end

    def log_path
      path = File.join(@data["log_dir"] || "log",
                       "groonga-import.log")
      File.expand_path(path, File.dirname(@path))
    end

    def log_age
      @data["log_age"] || 7
    end

    def log_max_size
      @data["log_max_size"] || (1024 * 1024)
    end

    def log_level
      @data["log_level"] || "info"
    end

    private
    def create_logger
      path = log_path
      FileUtils.mkdir_p(File.dirname(path))
      Logger.new(path,
                 log_age,
                 log_max_size,
                 datetime_format: "%Y-%m-%dT%H:%M:%S.%N",
                 level: log_level,
                 progname: "groonga-import")
    end

    class MySQL
      def initialize(data)
        @data = data
      end

      def host
        @data["host"] || "localhost"
      end

      def port
        @data["port"] || 3306
      end

      def socket
        @data["socket"]
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

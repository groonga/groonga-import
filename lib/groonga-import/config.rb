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
    module PathResolvable
      private
      def resolve_path(path)
        File.expand_path(path, @dir)
      end
    end

    include PathResolvable

    def initialize(dir)
      @dir = dir
      @path = File.join(@dir, "config.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
      @secret_path = File.join(@dir, "secret.yaml")
      if File.exist?(@secret_path)
        @secret_data = YAML.load(File.read(@secret_path))
      else
        @secret_data = {}
      end
    end

    def mysql
      return nil unless @data["mysql"]
      MySQL.new(@data["mysql"],
                @secret_data["mysql"] || {})
    end

    def local
      return nil unless @data["local"]
      Local.new(@dir, @data["local"])
    end

    def mapping
      Mapping.new(@data["mapping"] || {})
    end

    def binlog_dir
      resolve_path(@data["binlog_dir"] || "binlog")
    end

    def delta_dir
      resolve_path(@data["delta_dir"] || "delta")
    end

    def logger
      @logger ||= create_logger
    end

    def log_path
      resolve_path(File.join(@data["log_dir"] || "log",
                             "groonga-import.log"))
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

    def polling_interval
      Float(@data["polling_interval"] || "60")
    end

    private
    def resolve_path(path)
      File.expand_path(path, @dir)
    end

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
      def initialize(data, secret_data)
        @data = data
        @secret_data = secret_data
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
        @secret_data["password"] || @data["password"]
      end

      def replication_client
        @data["replication_client"] || @data
      end

      def replication_client_user
        replication_client["user"]
      end

      def replication_client_password
        (@secret_data["replication_client"] || @secret_data)["password"] ||
          replication_client["password"]
      end

      def replication_slave
        @data["replication_slave"] || @data
      end

      def replication_slave_user
        replication_slave["user"]
      end

      def replication_slave_password
        (@secret_data["replication_slave"] || @secret_data)["password"] ||
          replication_slave["password"]
      end

      def select
        @data["select"] || @data
      end

      def select_user
        select["user"]
      end

      def select_password
        (@secret_data["select"] || @secret_data)["password"] ||
          select["password"]
      end

      def checksum
        _checksum = @data["checksum"]
        return nil if _checksum.nil?
        _checksum.to_sym
      end
    end

    class Local
      include PathResolvable

      def initialize(dir, data)
        @dir = dir
        @data = data
      end

      def dir
        resolve_path(@data["dir"] || "local")
      end
    end
  end
end

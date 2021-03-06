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

require "optparse"

module GroongaImport
  class CommandLine
    def initialize
      @dir = "."
      @server = false
    end

    def run(args)
      catch do |tag|
        parse_args(args, tag)
        config = Config.new(@dir)
        status = Status.new(@dir)
        sources = []
        if config.local
          require_relative "local-source"
          sources << LocalSource.new(config, status)
        end
        if config.mysql
          require_relative "mysql-source"
          sources << MySQLSource.new(config, status)
        end
        loop do
          sources.each do |source|
            source.import
          end
          break unless @server
          sleep(config.polling_interval)
        end
        true
      end
    end

    private
    def parse_args(args, tag)
      parser = OptionParser.new
      parser.on("--dir=DIR",
                "Use DIR as directory that has configuration files",
                "(#{@dir})") do |dir|
        @dir = dir
      end
      parser.on("--server",
                "Run as a server") do
        @server = true
      end
      parser.on("--version",
                "Show version and exit") do
        puts(VERSION)
        throw(tag, true)
      end
      parser.on("--help",
                "Show this message and exit") do
        puts(parser.help)
        throw(tag, true)
      end
      parser.parse!(args.dup)
    end
  end
end

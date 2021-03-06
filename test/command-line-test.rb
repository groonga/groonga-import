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

class CommandLineTest < Test::Unit::TestCase
  def run_command(*args)
    command_line = GroongaImport::CommandLine.new
    Dir.chdir(@dir) do
      command_line.run(["--dir=#{@dir}", *args])
    end
  end

  def setup
    Dir.mktmpdir do |dir|
      @dir = dir
      yield
    end
  end

  def fixture_path(*components)
    File.join(__dir__, "fixture", *components)
  end

  def generate_config(port, checksum)
    data = {
      "mysql" => {
        "host" => "127.0.0.1",
        "port" => port,
        "user" => "replicator",
        "password" => "replicator-password",
        "replication_client" => {
          "user" => "c-replicator",
          "password" => "client-replicator-password",
        },
        "select" => {
          "user" => "selector",
          "password" => "selector-password",
        },
        "checksum" => checksum,
      },
      "mapping" => {
        "items" => {
          "sources" => [
            {
              "database" => "importer",
              "table" => "shoes",
              "columns" => {
                "_key" => "shoes-%{id}",
                "id" => "%{id}",
                "name" => "%{name}",
                "name_text" => {
                  "expression" => "html_untag(name)",
                },
                "source" => "shoes",
              },
            },
          ],
        },
      }
    }
    yield(data) if block_given?
    File.open(File.join(@dir, "config.yaml"), "w") do |output|
      output.puts(data.to_yaml)
    end
  end

  def extract_port(service)
    Integer(service["ports"].first.split(":")[1], 10)
  end

  def run_mysqld(version)
    config = YAML.load(File.read(fixture_path("docker-compose.yml")))
    case version
    when "5.5"
      target_service = "mysql-#{version}-replica"
      source_service = "mysql-#{version}-source"
      up_service = "mysql-#{version}-nested-replica"
      checksum = nil
    else
      target_service = "mysql-#{version}-source"
      source_service = target_service
      up_service = target_service
      checksum = "crc32"
    end
    target_port = extract_port(config["services"][target_service])
    source_port = extract_port(config["services"][source_service])
    up_port = extract_port(config["services"][up_service])
    pid = Dir.chdir(fixture_path) do
      system("docker-compose", "down")
      spawn("docker-compose", "up", up_service)
    end
    begin
      loop do
        begin
          Mysql2::Client.new(host: "127.0.0.1",
                             port: up_port,
                             username: "root")
        rescue Mysql2::Error::ConnectionError
          sleep(0.1)
        else
          break
        end
      end
      yield(target_port, source_port, checksum)
    ensure
      Dir.chdir(fixture_path) do
        system("docker-compose", "down")
      end
      Process.waitpid(pid)
    end
  end

  def setup_initial_records(source_port)
    client = Mysql2::Client.new(host: "127.0.0.1",
                                port: source_port,
                                username: "root")
    client.query("CREATE DATABASE importer")
    client.query("USE importer")
    client.query(<<-SQL)
      CREATE TABLE shoes (
        id int PRIMARY KEY,
        name text
      );
    SQL
    client.query("INSERT INTO shoes VALUES " +
                 "(1, 'shoes <br> a'), " +
                 "(2, 'shoes <br> b'), " +
                 "(3, 'shoes <br> c')");
  end

  def setup_changes(source_port)
    client = Mysql2::Client.new(host: "127.0.0.1",
                                port: source_port,
                                username: "root",
                                database: "importer")
    client.query("INSERT INTO shoes VALUES " +
                 "(10, 'shoes <br> A'), " +
                 "(20, 'shoes <br> B'), " +
                 "(30, 'shoes <br> C')");
    client.query("DELETE FROM shoes WHERE id >= 20")
    client.query("INSERT INTO shoes VALUES (40, 'shoes <br> D')");
    client.query("UPDATE shoes SET name = 'shoes <br> X' WHERE id = 40");
  end

  def read_packed_table_files(table)
    data = ""
    Dir.glob("#{@dir}/delta/data/#{table}/packed/*/*.grn").sort.each do |file|
      data << File.read(file)
    end
    data
  end

  def read_table_files(table)
    data = ""
    Dir.glob("#{@dir}/delta/data/#{table}/*.grn").sort.each do |file|
      data << File.read(file)
    end
    data
  end

  data(:version, ["5.5", "5.7"])
  def test_mysql
    run_mysqld(data[:version]) do |target_port, source_port, checksum|
      generate_config(target_port, checksum)
      setup_initial_records(source_port)
      assert_true(run_command)
      assert_equal(<<-UPSERT, read_packed_table_files("items"))
load --table items
[
{"_key":"shoes-1","id":"1","name":"shoes <br> a","name_text":"shoes  a","source":"shoes"},
{"_key":"shoes-2","id":"2","name":"shoes <br> b","name_text":"shoes  b","source":"shoes"},
{"_key":"shoes-3","id":"3","name":"shoes <br> c","name_text":"shoes  c","source":"shoes"}
]
      UPSERT
      setup_changes(source_port)
      assert_true(run_command)
      assert_equal(<<-DELTA, read_table_files("items"))
load --table items
[
{"_key":"shoes-10","id":"10","name":"shoes <br> A","name_text":"shoes  A","source":"shoes"},
{"_key":"shoes-20","id":"20","name":"shoes <br> B","name_text":"shoes  B","source":"shoes"},
{"_key":"shoes-30","id":"30","name":"shoes <br> C","name_text":"shoes  C","source":"shoes"}
]
delete --key "shoes-20" --table "items"
delete --key "shoes-30" --table "items"
load --table items
[
{"_key":"shoes-40","id":"40","name":"shoes <br> D","name_text":"shoes  D","source":"shoes"}
]
load --table items
[
{"_key":"shoes-40","id":"40","name":"shoes <br> X","name_text":"shoes  X","source":"shoes"}
]
      DELTA
    end
  end
end

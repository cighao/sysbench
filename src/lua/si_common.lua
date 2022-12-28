-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- -----------------------------------------------------------------------------
-- Common code for OLTP benchmarks.
-- -----------------------------------------------------------------------------

function init()
  assert(event ~= nil,
         "this script is meant to be included by other OLTP scripts and " ..
            "should not be called directly.")
  if(sysbench.opt.masters_num > 0) then
     assert(sysbench.opt.master_id > 0 and
            sysbench.opt.master_id <= sysbench.opt.masters_num, "invalid master id")
     assert(sysbench.opt.tables_per_master > 0, "invalid tables_per_master")
     assert(sysbench.opt.master_interleave <= 1, "invalid master_interleave")
     assert(sysbench.opt.masters_num *  sysbench.opt.tables_per_master <=
            sysbench.opt.tables, "number of tables not enough")
  end
  print(string.format("Tables: %dx%d", sysbench.opt.tables, sysbench.opt.table_size));
  print("Number of masters: " .. sysbench.opt.masters_num);
  print("Tables per masters: " .. sysbench.opt.tables_per_master);
  print("Master ID: " .. sysbench.opt.master_id);
  print("Master interleave: " .. sysbench.opt.master_interleave .. "\n");
  print("index number: " .. sysbench.opt.index_num .. "\n");
end

if sysbench.cmdline.command == nil then
  error("Command is required. Supported commands: prepare, warmup, run, " ..
           "cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
  table_size =
     {"Number of rows per table", 10000},
  tables =
     {"Number of tables", 1},
  tables_per_master =
     {"Number of tables per master", 1},
  masters_num =
     {"Number of masters", 0},
  master_interleave =
     {"master interleave", 0},
  master_id =
     {"master id", 0},
  auto_inc =
     {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
      "or its alternatives in other DBMS. When disabled, use " ..
      "client-generated IDs", true},
  create_table_options =
     {"Extra CREATE TABLE options", ""},
  reconnect =
     {"Reconnect after every N events. The default (0) is to not reconnect",
      0},
  mysql_storage_engine =
     {"Storage engine, if MySQL is used", "innodb"},
  pgsql_variant =
     {"Use this PostgreSQL variant when running with the " ..
         "PostgreSQL driver. The only currently supported " ..
         "variant is 'redshift'. When enabled, " ..
         "create_secondary is automatically disabled, and " ..
         "delete_inserts is set to 0"},
  index_num =
      {"Number of secondary index", 0}
}

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
  local drv = sysbench.sql.driver()
  local con = drv:connect()

  for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
  sysbench.opt.threads do
    create_table(drv, con, i)
  end
end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
--
-- PS. Currently, this command is only meaningful for MySQL/InnoDB benchmarks
function cmd_warmup()
  local drv = sysbench.sql.driver()
  local con = drv:connect()

  assert(drv:name() == "mysql", "warmup is currently MySQL only")

  -- Do not create on disk tables for subsequent queries
  con:query("SET tmp_table_size=2*1024*1024*1024")
  con:query("SET max_heap_table_size=2*1024*1024*1024")

  for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
  sysbench.opt.threads do
     local t = "sbtest" .. i
     print("Preloading table " .. t)
     con:query("ANALYZE TABLE sbtest" .. i)
     con:query(string.format(
                  "SELECT AVG(id) FROM " ..
                     "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                     "LIMIT %u) t",
                  t, sysbench.opt.table_size))
     con:query(string.format(
                  "SELECT COUNT(*) FROM " ..
                     "(SELECT * FROM %s WHERE k LIKE '%%0%%' LIMIT %u) t",
                  t, sysbench.opt.table_size))
  end
end

-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
  prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
  warmup = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND},
  prewarm = {cmd_warmup, sysbench.cmdline.PARALLEL_COMMAND}
}


-- Template strings of random digits with 11-digit groups separated by dashes

-- 10 groups, 119 characters
local c_value_template = "###########-###########-###########-" ..
  "###########-###########-###########-" ..
  "###########-###########-###########-" ..
  "###########"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" ..
  "###########-###########"

function get_c_value()
  return sysbench.rand.string(c_value_template)
end

function get_pad_value()
  return sysbench.rand.string(pad_value_template)
end

function create_table(drv, con, table_num)
  local id_def
  local engine_def = ""
  local extra_table_options = ""
  local query

  if drv:name() == "mysql"
  then
     if sysbench.opt.auto_inc then
        id_def = "bigint NOT NULL AUTO_INCREMENT"
     else
        id_def = "bigint NOT NULL"
     end
     engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
  elseif drv:name() == "pgsql"
  then
     if not sysbench.opt.auto_inc then
        id_def = "INTEGER NOT NULL"
     elseif pgsql_variant == 'redshift' then
       id_def = "INTEGER IDENTITY(1,1)"
     else
       id_def = "SERIAL"
     end
  else
     error("Unsupported database driver:" .. drv:name())
  end

  print(string.format("Creating table 'sbtest%d'...", table_num))

  query = string.format([[
         CREATE TABLE sbtest%d(
         id %s,
         k INTEGER DEFAULT '0' NOT NULL,
         k1 int NOT NULL ,
         k2 int NOT NULL ,
         k3 int NOT NULL ,
         k4 int NOT NULL ,
         k5 int NOT NULL ,
         k6 int NOT NULL ,
         k7 int NOT NULL ,
         k8 int NOT NULL ,
         c CHAR(120) DEFAULT '' NOT NULL,
         pad CHAR(60) DEFAULT '' NOT NULL,
         PRIMARY KEY (id)
         ) %s %s]],
     table_num, id_def, engine_def,
     sysbench.opt.create_table_options)

  con:query(query)

  for i = 1, sysbench.opt.index_num do
     print(string.format("Create secondary index %d for table 'sbtest%d'...", i, table_num))
     con:query(string.format("CREATE INDEX k%d_%d ON sbtest%d(k%d)", table_num, i, table_num, i))
  end

  if (sysbench.opt.table_size > 0) then
     print(string.format("Inserting %d records into 'sbtest%d'",
                          sysbench.opt.table_size, table_num))
  end

  if sysbench.opt.auto_inc then
     query = "INSERT INTO sbtest" .. table_num .. "(k, k1,k2,k3,k4,k5,k6,k7,k8, c, pad) VALUES"
  else
     query = "INSERT INTO sbtest" .. table_num .. "(id, k, k1,k2,k3,k4,k5,k6,k7,k8, c, pad) VALUES"
  end


  con:bulk_insert_init(query)

  local c_val
  local pad_val
  local id

  for i = 1, sysbench.opt.table_size do
     c_val = get_c_value()
     pad_val = get_pad_value()

     if (sysbench.opt.auto_inc) then
        query = string.format("(%d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s')",
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 c_val, pad_val)
     else
        id = sysbench.rand.unique()
        query = string.format("(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s')",
                                 id,
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 sysbench.rand.default(1, sysbench.opt.table_size),
                                 c_val, pad_val)
     end
     con:bulk_insert_next(query)
  end
  con:bulk_insert_done()
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
  con:disconnect()
end

function cleanup()
  local drv = sysbench.sql.driver()
  local con = drv:connect()

  for i = 1, sysbench.opt.tables do
     print(string.format("Dropping table 'sbtest%d'...", i))
     con:query("DROP TABLE IF EXISTS sbtest" .. i )
  end
end

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
  if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
     errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
     errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
     errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
  then
     prepare_statements()
  end
end

function check_reconnect()
  if sysbench.opt.reconnect > 0 then
     transactions = (transactions or 0) + 1
     if transactions % sysbench.opt.reconnect == 0 then
        con:reconnect()
        prepare_statements()
     end
  end
end

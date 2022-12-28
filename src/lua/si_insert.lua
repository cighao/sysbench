#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

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

-- ----------------------------------------------------------------------
-- Insert-Only OLTP benchmark
-- ----------------------------------------------------------------------

require("si_common")

sysbench.cmdline.commands.prepare = {
   function ()
      if (not sysbench.opt.auto_inc) then
         -- Create empty tables on prepare when --auto-inc is off, since IDs
         -- generated on prepare may collide later with values generated by
         -- sysbench.rand.unique()
         -- sysbench.opt.table_size=0
         -- print("Reset table size to 0 when enable auto_inc during prepare");
      end
      cmd_prepare()
   end,
   sysbench.cmdline.PARALLEL_COMMAND
}

local function get_table_num()
   if (sysbench.opt.masters_num == 0) then
      return sysbench.rand.uniform(1, sysbench.opt.tables)
   end
   if (sysbench.rand.uniform_double() < sysbench.opt.master_interleave ) then

      return sysbench.rand.uniform(1, sysbench.opt.tables_per_master *
                                      sysbench.opt.masters_num)
   end
   local table_num = (sysbench.opt.master_id - 1) * sysbench.opt.tables_per_master + 1;
   return sysbench.rand.uniform(table_num,
                                table_num + sysbench.opt.tables_per_master - 1);
end

function event()
   local table_name = "sbtest" .. get_table_num()
   local c_val = get_c_value()
   local pad_val = get_pad_value()

   if (drv:name() == "pgsql" and sysbench.opt.auto_inc) then
      con:query(string.format("INSERT INTO %s (k, c, pad) VALUES " ..
                                 "(%d, '%s', '%s')",
                              table_name, k_val, c_val, pad_val))
   else
      if (sysbench.opt.auto_inc) then
         con:query(string.format("INSERT INTO %s (k, k1,k2,k3,k4,k5,k6,k7,k8, c, pad) VALUES " ..
            "(%d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s')", 
               table_name,
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               sysbench.rand.default(1, 2^31),
               c_val, pad_val))
      else
         id =  sysbench.rand.unique()
         con:query(string.format("INSERT INTO %s " ..
            "(id, k, k1, k2, k3, k4, k5, k6, k7, k8, c, pad) VALUES " ..
            "(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', '%s')", 
            table_name, id,
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            sysbench.rand.default(1, 2^31),
            c_val, pad_val))
      end
   end

   check_reconnect()
end

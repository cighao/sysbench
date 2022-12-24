-- Command line options
sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   tables =
      {"Number of tables", 1},
   auto_inc =
   {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
       "or its alternatives in other DBMS. When disabled, use " ..
       "client-generated IDs", true},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false}
}

function random_choose(array, num)
   local len = #array
   if(num >= 1 and num < len) then
      for i = 1,num do
         local ri = sysbench.rand.uniform(i, len);
         local tmp = array[i]
         array[i] = array[ri]
         array[ri] = tmp
      end
   end
end

function create_table_and_load_data(table_id, drv, con)
   print("Creating table " .. table_id .. "...")

   local query = [[ CREATE TABLE Subscriber_]] .. table_id .. [[ 
                     (s_id INTEGER NOT NULL PRIMARY KEY,
                     sub_nbr VARCHAR(15) NOT NULL UNIQUE,
                     bit_1 TINYINT, bit_2 TINYINT, bit_3 TINYINT,
                     bit_4 TINYINT, bit_5 TINYINT, bit_6 TINYINT,
                     bit_7 TINYINT, bit_8 TINYINT, bit_9 TINYINT,
                     bit_10 TINYINT, hex_1 TINYINT, hex_2 TINYINT,
                     hex_3 TINYINT, hex_4 TINYINT, hex_5 TINYINT,
                     hex_6 TINYINT, hex_7 TINYINT, hex_8 TINYINT,
                     hex_9 TINYINT, hex_10 TINYINT, byte2_1 SMALLINT,
                     byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT,
                     byte2_5 SMALLINT, byte2_6 SMALLINT, byte2_7 SMALLINT,
                     byte2_8 SMALLINT, byte2_9 SMALLINT, byte2_10 SMALLINT,
                     msc_location INTEGER, vlr_location INTEGER); ]]
   con:query(query)

   query = [[CREATE TABLE Access_Info_]] .. table_id ..  [[
                     (s_id INTEGER NOT NULL,
                     ai_type TINYINT NOT NULL,
                     data1 SMALLINT, data2 SMALLINT,
                     data3 CHAR(3), data4 CHAR(5),
                     PRIMARY KEY(s_id, ai_type),
                     FOREIGN KEY (s_id) REFERENCES Subscriber_]] .. table_id ..  [[ (s_id));]]
   con:query(query)

   query =  [[ CREATE TABLE Special_Facility_]] .. table_id ..  [[
                     (s_id INTEGER NOT NULL,
                     sf_type TINYINT NOT NULL,
                     is_active TINYINT NOT NULL,
                     error_cntrl SMALLINT,
                     data_a SMALLINT,
                     data_b CHAR(5),
                     PRIMARY KEY (s_id, sf_type),
                     FOREIGN KEY (s_id) REFERENCES Subscriber_]] .. table_id ..  [[ (s_id));]]
   con:query(query)

   query = [[CREATE TABLE Call_Forwarding_]] .. table_id ..  [[
                     (s_id INTEGER NOT NULL,
                     sf_type TINYINT NOT NULL,
                     start_time TINYINT NOT NULL,
                     end_time TINYINT,
                     numberx VARCHAR(15),
                     PRIMARY KEY (s_id, sf_type, start_time),
                     FOREIGN KEY (s_id, sf_type)
                     REFERENCES Special_Facility_]] .. table_id ..  [[(s_id, sf_type));]]
   con:query(query)

   --load data
   local query1 = [[INSERT INTO Subscriber_]] .. table_id .. [[ 
                     (s_id, sub_nbr, bit_1, bit_2, bit_3, bit_4, 
                     bit_5, bit_6, bit_7, bit_8, bit_9, bit_10, 
                     hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, 
                     hex_7, hex_8, hex_9, hex_10, byte2_1, byte2_2,
                     byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, 
                     byte2_8, byte2_9, byte2_10,
                     msc_location, vlr_location) VALUES]]
   local query2 = "INSERT INTO Access_Info_" .. table_id ..
                  "(s_id, ai_type, data1, data2, data3, data4) VALUES"
   local query3 = "INSERT INTO Special_Facility_" .. table_id ..
                  "(s_id, sf_type, is_active, error_cntrl, data_a, data_b) VALUES"
   local query4 = "INSERT INTO Call_Forwarding_" .. table_id ..
                  "(s_id, sf_type, start_time, end_time, numberx) VALUES"

   local con1 = drv:connect();
   local con2 = drv:connect();
   local con3 = drv:connect();
   local con4 = drv:connect();

   con1:bulk_insert_init(query1)
   con2:bulk_insert_init(query2)
   con3:bulk_insert_init(query3)
   con4:bulk_insert_init(query4)

   local uniform = sysbench.rand.uniform;

   for i = 1, sysbench.opt.table_size do
      -- table Subscriber
      local s_id = i
      local sub_nbr = string.format("%015d",s_id)
      query = string.format("(%d, '%s', " .. 
                              "%d, 0, 0, 0, 0, 0, 0, 0, 0, 0, " ..
                              "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, " ..
                              "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, " ..
                              "%d, %d)", s_id, sub_nbr, uniform(0,1),
                              uniform(1,2^32-1), uniform(1,2^32-1));
      con1:bulk_insert_next(query)

      -- table Access_Info
      local num = uniform(1, 4)
      local types = {1, 2, 3, 4}
      random_choose(types, num)
      for j=1, num do
         query = string.format("(%d, %d, %d, %d, 'AAA', 'BBBBB')", s_id, types[j],
                              uniform(0, 255), uniform(0, 255));
         con2:bulk_insert_next(query)
      end

      -- table Special_Facility
      local num = uniform(1, 4)
      types = {1, 2, 3, 4}
      random_choose(types, num)
      for j=1, num do
         local active = 1;
         if uniform(1, 100) > 85  then
            active = 0
         end
         query = string.format("(%d, %d, %d, %d, %d, 'AAAAA')", s_id, types[j], active, 
                              uniform(0, 255), uniform(0, 255));
         con3:bulk_insert_next(query)

         -- table Call_Forwarding
         local num2 = uniform(0, 3)
         local stimes = {0, 8, 16}
         random_choose(stimes, num2)
         for k=1, num2 do
            etime = stimes[k] + uniform(1,8)
            numberx = uniform(1, 2^32-1)
            numberx = string.format("%015d", numberx)
            query = string.format("(%d, %d, %d, %d, '%s')", s_id, types[j], stimes[k], 
                                    etime, numberx);
            con4:bulk_insert_next(query)
         end
      end
   end

   con1:bulk_insert_done()
   con2:bulk_insert_done()
   con3:bulk_insert_done()
   con4:bulk_insert_done()
end

function get_table_id()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

function get_subscriber_data()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size)

   query = string.format("SELECT s_id, sub_nbr," ..
         "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7," ..
         "bit_8, bit_9, bit_10," ..
         "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7," ..
         "hex_8, hex_9, hex_10," ..
         "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5," ..
         "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10," ..
         "msc_location, vlr_location " ..
         "FROM Subscriber_%u " ..
         "WHERE s_id = %d", table_id, s_id)

   local rs = con:query(query)
   assert(rs.nrows == 1)
   -- local row = rs:fetch_row()
   -- print(tostring(row[1]))
end

start_time={0, 6, 8}
function get_new_destination()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size)
   local sf_type = sysbench.rand.uniform(1, 4)
   local stime = start_time[sysbench.rand.uniform(1, 3)]
   local etime = sysbench.rand.uniform(1, 24)

   query = string.format("SELECT cf.numberx " ..
               "FROM Special_Facility_%u AS sf, " ..
               "Call_Forwarding_%u AS cf " ..
               "WHERE " ..
               "sf.s_id = %d " ..
               "AND sf.sf_type = %d " ..
               "AND sf.is_active = 1  " ..
               "AND cf.s_id = sf.s_id " ..
               "AND cf.sf_type = sf.sf_type " ..
               "AND cf.start_time <= %d " ..
               "AND cf.end_time > %d",
               table_id, table_id, s_id, sf_type, stime, etime)

   local rs = con:query(query)
   -- print("found ", rs.nrows)
end

function get_access_data()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size)
   local ai_type = sysbench.rand.uniform(1, 4)

   query = string.format("SELECT data1, data2, data3, data4 " ..
               "FROM Access_Info_%u " ..
               "WHERE s_id = %d " ..
               "AND ai_type = %d ",
               table_id, s_id, ai_type)
   local rs = con:query(query)
   assert(rs.nrows <= 1)
   -- print("found ", rs.nrows)
end

function update_subscriber_data()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size)
   local sf_type = sysbench.rand.uniform(1, 4)
   local bit = sysbench.rand.uniform(0, 1)
   local data_a = sysbench.rand.uniform(0, 255)

   local query = string.format("UPDATE Subscriber_%u " ..
            "SET bit_1 = %d WHERE s_id = %d ",
             table_id, bit, s_id)
   local rs = con:query(query)

   query = string.format("UPDATE Special_Facility_%u " ..
            "SET data_a = %d WHERE s_id = %d and sf_type= %d",
             table_id, data_a, s_id , sf_type)
   local rs = con:query(query)
end

function update_location()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size)
   local sub_nbr = string.format("%015d", s_id)
   local vlr_location = sysbench.rand.uniform(1, 2^32-1)

   local query = string.format("UPDATE Subscriber_%u " ..
                  "SET vlr_location = %d WHERE sub_nbr = '%s' ",
                  table_id, vlr_location, sub_nbr)
   local rs = con:query(query)
end

function insert_call_forwarding()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size);
   local sub_nbr = string.format("%015d", s_id)

   -- Get the s_id
   local query = string.format("SELECT s_id FROM Subscriber_%u " ..
                  "WHERE sub_nbr = '%s' ", table_id, sub_nbr)
   local rs = con:query(query)
   assert(rs.nrows == 1)
   local row = rs:fetch_row()
   assert(tonumber(row[1]) == s_id);


   -- Get the the sf_type (randomly chose one)
   query = string.format("SELECT sf_type FROM Special_Facility_%u " ..
                         "WHERE s_id = '%s' ", table_id, s_id)
   local rs = con:query(query)
   assert(rs.nrows >= 1)
   local sf_type = 0;
   local index  = sysbench.rand.uniform(1, rs.nrows)
   for i = 1, rs.nrows do
      local row = rs:fetch_row()
      if(i == index) then
         sf_type = tonumber(row[1]);
         break;
      end
   end

   -- Insert call forwarding
   local stime = start_time[sysbench.rand.uniform(1, 3)]
   local etime = stime + sysbench.rand.uniform(1,8)
   local numberx = string.format("%015d", sysbench.rand.uniform(1, 2^32-1))
   query = string.format("INSERT INTO Call_Forwarding_%u " ..
                         "(s_id, sf_type, start_time, end_time, numberx) " ..
                         "VALUE (%d, %d, %d, %d, '%s')", table_id, s_id, sf_type, stime, etime, numberx)
   local rs = con:query(query)
end

function delete_call_forwarding()
   local table_id = get_table_id()
   local s_id = sysbench.rand.uniform(1, sysbench.opt.table_size);
   local sub_nbr = string.format("%015d", s_id)

   local query = string.format("SELECT s_id FROM Subscriber_%u " ..
                  "WHERE sub_nbr = '%s' ", table_id, sub_nbr)
   local rs = con:query(query)
   assert(rs.nrows == 1)
   local row = rs:fetch_row()
   assert(tonumber(row[1]) == s_id);

   local sf_type = sysbench.rand.uniform(1, 4)
   local stime = start_time[sysbench.rand.uniform(1, 3)]
   query = string.format("DELETE FROM Call_Forwarding_%u " ..
                     "WHERE s_id = %d AND sf_type = %d AND start_time = %d ", 
                        table_id, s_id, sf_type, stime)
   local rs = con:query(query)
end

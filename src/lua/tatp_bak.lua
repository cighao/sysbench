
local t = sysbench.sql.type
local stmt_defs = {
   get_subscriber_data = {
      "SELECT s_id, sub_nbr," ..
         "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7," ..
         "bit_8, bit_9, bit_10," ..
         "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7," ..
         "hex_8, hex_9, hex_10," ..
         "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5," ..
         "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10," ..
         "msc_location, vlr_location " ..
         "FROM Subscriber_%u " ..
         "WHERE s_id = ?",
          t.INT},
   get_new_destination = {
      "SELECT cf.numberx " ..
         "FROM Special_Facility_%u AS sf, Call_Forwarding_%u AS cf " ..
         "WHERE " ..
         "(sf.s_id = ? " ..
         "AND sf.sf_type = ? " ..
         "AND sf.is_active = 1) " ..
         "AND (cf.s_id = sf.s_id " ..
         "AND cf.sf_type = sf.sf_type) " ..
         "AND (cf.start_time <= ? " ..
         "AND ? < cf.end_time) ",
      t.INT, t.INT, t.INT, t.INT},
   get_access_data = {
      "SELECT data1, data2, data3, data4 " ..
         "FROM Access_Info_%u " ..
         "WHERE s_id = ? " ..
         "AND ai_type = ? ",
         t.INT, t.INT},
   update_subscriber_data = {
      "UPDATE Subscriber_%u " ..
      "SET bit_1 = ? " ..
      "WHERE s_id = ?; ",
      t.INT, t.INT},
   update_special_facility_data = {
      "UPDATE Special_Facility_%u " ..
      "SET data_a = ? " ..
      "WHERE s_id = ? " ..
      "AND sf_type = ?;",
      t.INT, t.INT, t.INT},
   update_location = {
      "UPDATE Subscriber_%u " ..
      "SET vlr_location = ? " ..
      "WHERE sub_nbr = ?;",
      t.INT, {t.CHAR, 15}},
}


function prepare_for_each_table(key)
   print("prepare for ", key)
   for t = 1, sysbench.opt.tables do
      if(key == "get_new_destination") then 
         stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t, t))
      else
         stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t))
      end

      local nparam = #stmt_defs[key] - 1

      if nparam > 0 then
         param[t][key] = {}
      end

      for p = 1, nparam do
         local btype = stmt_defs[key][p+1]
         local len

         if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
         end
         if btype == sysbench.sql.type.VARCHAR or
            btype == sysbench.sql.type.CHAR then
               param[t][key][p] = stmt[t][key]:bind_create(btype, len)
         else
            param[t][key][p] = stmt[t][key]:bind_create(btype)
         end
      end

      if nparam > 0 then
         stmt[t][key]:bind_param(unpack(param[t][key]))
      end
   end
end


function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end


function prepare_statements()
   if not sysbench.opt.skip_trx then
      prepare_begin()
      prepare_commit()
   end
   prepare_for_each_table("get_subscriber_data")
   prepare_for_each_table("get_new_destination")
   prepare_for_each_table("get_access_data")
   prepare_for_each_table("update_subscriber_data")
   prepare_for_each_table("update_special_facility_data")
   prepare_for_each_table("update_location")
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   -- Create global nested tables for prepared statements and their
   -- parameters. We need a statement and a parameter set for each combination
   -- of connection/table/query
   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end

   -- This function is a 'callback' defined by individual benchmark scripts
   prepare_statements()
end
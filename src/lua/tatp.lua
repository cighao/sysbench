require("tatp_common")

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

function thread_done()
  if con then
    con:disconnect()
	end
end

function cmd_prepare()
  local drv = sysbench.sql.driver()
	local con = drv:connect()
  local i
  -- we should set foreign_key_checks = 0 during prepare
  for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
          sysbench.opt.threads do
    create_table_and_load_data(i, drv, con)
  end
end

sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
}

function event()
  if not sysbench.opt.skip_trx then
    con:query("BEGIN")
  end

  local ret = sysbench.rand.uniform(0, 100)
  if(ret < 35) then
    get_subscriber_data()
  elseif( ret < 35 + 10) then
    get_new_destination()
  elseif( ret < 35 + 10 + 35) then
    get_access_data()
  elseif( ret < 35 + 10 + 35 + 2) then
    update_subscriber_data()
  elseif( ret < 35 + 10 + 35 + 2 +14) then
    update_location()
  elseif( ret < 35 + 10 + 35 + 2 +14 + 2) then
    insert_call_forwarding()
  else
    delete_call_forwarding()
  end

  if not sysbench.opt.skip_trx then
    con:query("COMMIT")
  end 
end

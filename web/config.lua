
--@Author: coldplay 
--@Date: 2017-04-11 10:22:06 
--@Last Modified by:   coldplay 
--@Last Modified time: 2017-04-11 10:22:06 


local mod_name = ...
local M = {}
local mysql = require "resty.mysql"

local curversion = "1.0"
local r = {}

local mysql_vm = {}
mysql_vm['host'] = "192.168.1.21"
mysql_vm['port'] = 3306
mysql_vm['database'] = "vm"
mysql_vm['user'] = "vm"
mysql_vm['password'] = "123456"
mysql_vm['max_packet_size'] = 1024 * 1024


local mysql_vm2 = {}
mysql_vm2['host'] = "192.168.1.21"
mysql_vm2['port'] = 3306
mysql_vm2['database'] = "vm2"
mysql_vm2['user'] = "vm"
mysql_vm2['password'] = "123456"
mysql_vm2['max_packet_size'] = 1024 * 1024

local mysql_hannel = {}
mysql_hannel['host'] = "192.168.1.21"
mysql_hannel['port'] = 3306
mysql_hannel['database'] = "hannel"
mysql_hannel['user'] = "vm"
mysql_hannel['password'] = "123456"
mysql_hannel['max_packet_size'] = 1024 * 1024

function M.mysql_vm_connect()
	local db, err = mysql:new()
	if not db then
	    return false
	end
	db:set_timeout(1000)

	local ok, err, errno, sqlstate = db:connect
	{
	    host = mysql_vm.host,
	    port = mysql_vm.port,
	    database = mysql_vm.database,
	    user =  mysql_vm.user,
	    password =  mysql_vm.password,
	    max_package_size =  mysql_vm.max_package_size
	 }
	 -- local times, err = db:get_reused_times()r
	 -- 	ngx.log(ngx.ERR, times)
	if not ok then
	    ngx.log(ngx.ERR,"failed to connect: "..(err or "nil")..": ".. (errno or 'nil'))
	    return false
	end
	db:query("SET NAMES utf8;")
	return db
end

function M.mysql_vm2_connect()
	local db, err = mysql:new()
	if not db then
	    return false
	end
	db:set_timeout(1000)

	local ok, err, errno, sqlstate = db:connect
	{
	    host = mysql_vm2.host,
	    port = mysql_vm2.port,
	    database = mysql_vm2.database,
	    user =  mysql_vm2.user,
	    password =  mysql_vm2.password,
	    max_package_size =  mysql_vm2.max_package_size
	 }
	 -- local times, err = db:get_reused_times()r
	 -- 	ngx.log(ngx.ERR, times)
	if not ok then
	    ngx.log(ngx.ERR,"failed to connect: "..(err or "nil")..": ".. (errno or 'nil'))
	    return false
	end
	db:query("SET NAMES utf8;")
	return db
end
function M.mysql_hannel_connect()
	local db, err = mysql:new()
	if not db then
	    return false
	end
	db:set_timeout(1000)

	local ok, err, errno, sqlstate = db:connect
	{
	    host = mysql_hannel.host,
	    port = mysql_hannel.port,
	    database = mysql_hannel.database,
	    user =  mysql_hannel.user,
	    password =  mysql_hannel.password,
	    max_package_size =  mysql_hannel.max_package_size
	 }
	 -- local times, err = db:get_reused_times()r
	 -- 	ngx.log(ngx.ERR, times)
	if not ok then
	    ngx.log(ngx.ERR,"failed to connect: "..(err or "nil")..": ".. (errno or 'nil'))
	    return false
	end
	db:query("SET NAMES utf8;")
	return db
end

M['mysql_vm'] = mysql_vm
M['version'] = curversion

M['mysql_hannel'] = mysql_hannel
M['version'] = curversion

return M

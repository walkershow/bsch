--
--@Author: coldplay 
--@Date: 20\%d7-07-29 10:21:17 
--@Last Modified by:   coldplay 
--@Last Modified time: 20\%d7-07-29 10:21:17 
--

local mysql = require "resty.mysql"
local config = require "config"
local db = config.mysql_vm_connect()
local cjson = require "cjson"
local args = ngx.req.get_uri_args()


local serverid = tonumber(args.serverid) or 0
if serverid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local a_times = tonumber(args.a_times) or 0
local b_times = tonumber(args.b_times) or 0
local c_times = tonumber(args.c_times) or 0
    -- sql = string.format("insert into vpn_a_times(serverid,vpna_times) values(%d,%d) ON DUPLICATE KEY UPDATE vpna_times=%d", serverid, a_times,a_times)
sql = string.format([[insert into ad_click_statistics(server_id,a_times,b_times,c_times,click_date,update_time) 
                    values(%d,a_times+%d,b_times+%d,c_times+%d,CURRENT_DATE,CURRENT_TIMESTAMP)
                    on duplicate key update a_times=a_times+%d,b_times=b_times+%d,c_times=c_times+%d]], serverid,a_times,b_times,c_times,a_times,b_times,c_times)
ngx.log(ngx.INFO, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    ngx.say("failed")
    return
end
ngx.say("succ")
db:set_keepalive(10000, 100)



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

-- id 任务id
local id = tonumber(args.tid) or -1
if id == -1 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local sid = tonumber(args.sid) or 0
if sid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end
local gid = tonumber(args.gid) or 0
if gid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end


local a_times = tonumber(args.a_times) or 0
local b_times = tonumber(args.b_times) or 0
local c_times = tonumber(args.c_times) or 0
    -- sql = string.format("insert into vpn_a_times(taskid,vpna_times) values(%d,%d) ON DUPLICATE KEY UPDATE vpna_times=%d", taskid, a_times,a_times)
    sql = string.format([[insert into ad_click_statistics(id,a_times,b_times,c_times,click_date,update_time) 
    values(%d,a_times+%d,b_times+%d,c_times+%d,CURRENT_DATE,CURRENT_TIMESTAMP)
    on duplicate key update a_times=a_times+%d,b_times=b_times+%d,c_times=c_times+%d]], 
    id,a_times,b_times,c_times,a_times,b_times,c_times)
-- ngx.log(ngx.INFO, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
ngx.log(ngx.ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
db:set_keepalive(10000, 100)
ngx.say("failed")
return
end
ngx.say("succ")
ngx.eof()

local up_col = nil
if a_times ~=0 then
    up_col = "a_time"
elseif b_times ~=0 then
    up_col = "b_time"
elseif c_times ~=0 then
    up_col = "c_time"
else
    up_col = nil
end 
if up_col == nil then
    return 
end

sql_ip = string.format('select ip,area from vpn_status where serverid=%d', sid)
local  res, err, errno, sqlstate = db:query(sql_ip, 10)
if not res then
    ngx.log(ngx.ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    ngx.say("failed")
    return
end

if next(res) == nil then
    db:set_keepalive(10000, 100)
    return
end
local ip= res[1].ip
local area= res[1].area
local ip_info =ip.." ["..area.."]"
ngx.log(ngx.ERR,ip_info)

 sql_oprcode = string.format("select oprcode from vm_oprcode where server_id=%d and group_id=%d and task_id=%d and status!=2 \
         order by create_time desc limit 1", sid,gid,id
        )
-- ngx.log(ngx.ERR, sql_oprcode)
local  res, err, errno, sqlstate = db:query(sql_oprcode, 10)
if not res then
    ngx.log(ngx.ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    ngx.say("failed")
    return
end
if next(res) == nil then
    db:set_keepalive(10000, 100)
    return
end
oprcode = res[1].oprcode
ngx.log(ngx.ERR, oprcode)
sql = string.format("insert into vm_task_log(oprcode,%s,ip,log_time) values(%d,CURRENT_TIMESTAMP,'%s',CURRENT_TIMESTAMP)",up_col, oprcode,ip_info)
ngx.log(ngx.ERR, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ngx.ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    ngx.say("failed")
    return
end



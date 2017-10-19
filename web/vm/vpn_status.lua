---
--@Author: coldplay 
--@Date: 2017-05-28 14:32:11 
--@Last Modified by:   coldplay 
--@Last Modified time: 2017-05-28 14:32:11 
---


local mysql = require "resty.mysql"
local config = require "config"
local db = config.mysql_vm_connect()
local cjson = require "cjson"
local args = ngx.req.get_uri_args()

local oprtype = args.oprtype or nil
if oprtype == nil then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local serverid = tonumber(args.serverid) or 0
if serverid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local status = tonumber(args.status) or 0
if status == 0 and oprtype == "up" then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local ip = args.ip or ""
local area = args.area or ""

if oprtype == "up" then
    -- sql = string.format("insert into vpn_status(serverid,vpnstatus) values(%d,%d) ON DUPLICATE KEY UPDATE vpnstatus=%d", serverid, status,status)
    sql = string.format("update vpn_status set vpnstatus=%d,ip='%s',area='%s' where serverid=%d", status, ip, area, serverid)
    ngx.log(ngx.INFO, sql)
    local  res, err, errno, sqlstate = db:query(sql, 10)
    if not res then
        ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
        db:set_keepalive(10000, 100)
        ngx.say("failed")
        return
    end
    ngx.say("succ")
else
    sql = string.format("select vpnstatus,UNIX_TIMESTAMP(update_time) as update_time from vpn_status where serverid=%d", serverid)
    ngx.log(ngx.INFO, sql)
    local  res, err, errno, sqlstate = db:query(sql, 10)
    if not res then
        ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
        db:set_keepalive(10000, 100)
        ngx.say("failed")
        return
    end
    ngx.say(cjson.encode(res))
end

db:set_keepalive(10000, 100)


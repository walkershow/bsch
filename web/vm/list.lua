--
--@Author: coldplay 
--@Date: 2017-05-09 15:55:21 
--@Last Modified by:   coldplay 
--@Last Modified time: 2017-05-09 15:55:21 
--

local mysql = require "resty.mysql"
local config = require "config"
local db = config.mysql_vm_connect()
local cjson = require "cjson"
local args = ngx.req.get_uri_args()

local sql = "select * from vm_group "
local gname= args.name
if gname ~= nil and gname ~= "" then
    sql = sql.." where groupname='"..gname.."'"
end
local page = tonumber(args.page) or 0
if page ~= 0 then
    local start = (page-1)*20
    local endt = page*20
    local limit = string.format(" limit %d,%d ",start, endt)
    sql = sql.. limit
end
ngx.log(ngx.INFO, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    return
end

function getTotal(groupname)
    local sql = "select count(*) total from vm_group "
    local gname= groupname
    if gname ~= nil then
        sql = sql.." where groupname='"..gname.."'"
    end
    local  res, err, errno, sqlstate = db:query(sql, 10)
    if not res then
        ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
        db:set_keepalive(10000, 100)
        -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
        return
    end
    return tonumber(res[1].total)
end

total = getTotal(gname)
ngx.say(cjson.encode({total=total,vms=res}))
db:set_keepalive(10000, 100)
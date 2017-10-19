

local mysql = require "resty.mysql"
local config = require "config"
local db = config.mysql_vm2_connect()
local cjson = require "cjson"
local args = ngx.req.get_uri_args()
local template = require "resty.template"
local vmid= args.vmid or 0 
if vmid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local serverid = tonumber(args.serverid) or 0
if serverid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST)
end
--vmid=1
--serverid=1
-- local status = tonumber(args.status) or 0
-- if status == 0 and oprtype == "up" then
--     ngx.exit(ngx.HTTP_BAD_REQUEST)
-- end

local sql = string.format("select a.cur_profile_id,b.name,b.shortcut from vm_cur_task a,profiles b where a.server_id=%d and a.vm_id=%d and a.status=1", serverid, vmid)
ngx.log(ngx.INFO, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ngx.ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    ngx.say("failed")
    return
end
if next(res)==nil then
    ngx.say(failed)
    db:set_keepalive(10000, 100)
return
end
local view = template.new "profile.html"
view.profile_id = res[1].profile_id
view.profile_name = res[1].name
view.profile_path= res[1].shortcut
view:render()
    
db:set_keepalive(10000, 100)




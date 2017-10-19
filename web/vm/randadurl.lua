--
--@Author: coldplay 
--@Date: 2017-04-11 11:06:57 
--@Last Modified by:   coldplay 
--@Last Modified time: 2017-04-11 11:06:57 
--
local mysql = require "resty.mysql"
local config = require "config"
local db = config.mysql_vm_connect()
local cjson = require "cjson"
local template = require "resty.template"
local args = ngx.req.get_uri_args()
local taskid = tonumber(args.taskid) or 0
if taskid == 0 then
   ngx.exit(ngx.HTTP_BAD_REQUEST) 
end
local sql = string.format("select link from link_ad_lib where task_id=%d order by rand() limit 1", taskid )
ngx.log(ngx.ERR, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    return
end
ngx.log(ngx.ERR, cjson.encode(res))
local link1 = res[1].link
ngx.log(ngx.ERR, link1)
local view = template.new "randurl.tpl"
view.url= link1
view:render()
db:set_keepalive(10000, 100)
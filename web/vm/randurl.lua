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
local fname = args.fname
if fname == nil then
    fname = "link"
end
local sql = string.format("select %s from link_lib where %s is not null and %s!='' order by rand() limit 1", fname, fname, fname)
ngx.log(ngx.ERR, sql)
local  res, err, errno, sqlstate = db:query(sql, 10)
if not res then
    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
    db:set_keepalive(10000, 100)
    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    return
end

local link = res[1][fname]
local view = template.new "randurl.tpl"
view.url= link
view:render()
db:set_keepalive(10000, 100)
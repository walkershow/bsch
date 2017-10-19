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
--获取页面的页码
local groupid= tonumber(args.groupid) or 0
if groupid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST) 
end
local serverid= tonumber(args.serverid) or 0
if serverid == 0 then
    ngx.exit(ngx.HTTP_BAD_REQUEST) 
end

function get_rand_keyword()
	local sql = "select keyword from keyword_lib order by rand() limit 1"
    ngx.log(ngx.INFO, sql)
	local  res, err, errno, sqlstate = db:query(sql, 10)
	if not res then
	    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
	    db:set_keepalive(10000, 100)
	    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	    return
	end
	-- local cjson = require "cjson"
	return cjson.decode(cjson.encode(res))
    --return res
end

function task_status(groupid)
    local sql = "select cur_task_num,task_quantity,final_action from vm_task_status where id="..groupid.." and serverid="..serverid
    ngx.log(ngx.INFO, sql)
	local  res, err, errno, sqlstate = db:query(sql, 10)
	if not res then
	    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
	    db:set_keepalive(10000, 100)
	    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	    return
	end
	-- local cjson = require "cjson"
	return cjson.decode(cjson.encode(res))
    --return res
end

function task_update(groupid,serverid, task_num)
    local sql = string.format("update vm_task_status set cur_task_num=%d where id=%d and serverid=%d",task_num, groupid, serverid) 
    ngx.log(ngx.INFO, sql)
	local  res, err, errno, sqlstate = db:query(sql, 10)
	if not res then
	    ngx.log(ERR,"the sql:"..sql.." executed failed; bad result: ".. err.. ": ".. errno.. ": ".. sqlstate.. ".")
	    db:set_keepalive(10000, 100)
	    -- ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
	    return
	end
	-- local cjson = require "cjson"
end
local actions = {"关机", "休眠"}
ts_res = task_status(groupid, serverid)
ngx.log(ngx.INFO,"res:%s",cjson.encode(ts_res[1]))
ts_row = ts_res[1]
local keyword = ""
if ts_row.cur_task_num >= ts_row.task_quantity then
	keyword = actions[ts_row.final_action+1]
   task_update(groupid, serverid, 0) 
else
    add_task_num = ts_row.cur_task_num + 1
    task_update(groupid,serverid, add_task_num)
    kw_res = get_rand_keyword()
    kw = kw_res[1].keyword
    
    keyword = kw 
    
end
local view = template.new "index.tpl"
view.keyword= keyword
view:render()
db:set_keepalive(10000, 100)
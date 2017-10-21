# -*- coding: utf-8 -*-
#
#@Author: coldplay
#@Date: 2017-09-05 10:06:21
#@Last Modified by:   coldplay
#@Last Modified time: 2017-09-05 10:06:21
#

import sys
import random
import bisect
import logging
import logging.config
sys.path.append("..")
import dbutil
from utils import GetOutip
class LogTaskError(Exception):
    pass

class LogTask(object):
    db = None
    logger = None
    outip = None 
    
    def __init__(self, dbs, log_handle):
        LogTask.db = dbs
        LogTask.logger = log_handle
        LogTask.outip = GetOutip()    

    def log_cur_taskid(self, server_id, vm_id, task_id):
        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id, start_time)
                values(%d,%d,%d, CURRENT_TIMESTAMP) on duplicate key update cur_task_id=%d, start_time=CURRENT_TIMESTAMP'''%( 
                    server_id,vm_id,task_id,task_id) 

        LogTask.logger.info(sql)
        ret = LogTask.db.execute_sql(sql)
        if ret<1:
            LogTask.logger.info("sql:%s ret:%d", sql, ret)
            raise LogTaskError, "gen oprcode error sql:%s ret:%d"%(sql, ret)

    def gen_oprcode_bytask(self, server_id, group_id, task_id):
        self.log_cur_taskid(server_id, group_id, task_id)
        sql = '''insert into vm_oprcode(server_id,group_id,task_id,status,create_time,update_time)
                values(%d,%d,%d,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)'''%(
                    server_id,group_id,task_id
                )

        LogTask.logger.info(sql)
        ret = LogTask.db.execute_sql(sql)
        if ret<1:
            LogTask.logger.info("sql:%s ret:%d", sql, ret)
            raise LogTaskError, "gen oprcode error sql:%s ret:%d"%(sql, ret)
            
    def get_oprcode_bytask(self, server_id, group_id,task_id):
        sql_oprcode = "select oprcode from vm_oprcode where server_id=%d and group_id=%d and task_id=%d \
         order by create_time desc limit 1"%(
            server_id,group_id,task_id
        )
        LogTask.logger.info(sql_oprcode)
        res = LogTask.db.select_sql(sql_oprcode)
        if res:
            return res[0][0]
        return None

    def get_cur_taskid(self, server_id, group_id):
        sql= "select cur_task_id from vm_cur_task where server_id=%d and group_id=%d "%(
            server_id,group_id
        )
        LogTask.logger.info(sql)
        res = LogTask.db.select_sql(sql)
        if res:
            return res[0][0]
        return -1
        # raise LogTaskError, "no task_id return error sql:%s "%(sql)
    
    
    def get_oprcode_latest(self, server_id, group_id):
        task_id = self.get_cur_taskid(server_id, group_id)
        sql_oprcode = "select oprcode from vm_oprcode where server_id=%d and group_id=%d and task_id=%d and status!=2 \
         order by create_time desc limit 1"%(
            server_id,group_id,task_id
        )
        LogTask.logger.info(sql_oprcode)
        res = LogTask.db.select_sql(sql_oprcode)
        if res:
            return res[0][0]
        return None
     
    def task_done(self, server_id, group_id):
        oprcode = self.get_oprcode_latest(server_id, group_id) 
        if oprcode is None:
            LogTask.logger.info("oprcode is none ,can't log task_done:%d,%d", server_id, group_id)
            return
        sql = '''update vm_oprcode set status=2,update_time=CURRENT_TIMESTAMP where oprcode=%d'''%(oprcode)
        LogTask.logger.info(sql)
        ret = LogTask.db.execute_sql(sql)
        if ret<0:
            LogTask.logger.info("sql:%s ret:%d", sql, ret)
            raise LogTaskError, "task done error sql:%s ret:%d"%(sql, ret)
        params = {'status':2,'end_time':'CURRENT_TIMESTAMP'}
        self.log_task_timepoint(oprcode, params )

    def task_done2(self, oprcode):
        if oprcode is None:
            LogTask.logger.info("oprcode is none ,can't log task_done:%d,%d", server_id, group_id)
            return
        sql = '''update vm_oprcode set status=2,update_time=CURRENT_TIMESTAMP where oprcode=%d'''%(oprcode)
        LogTask.logger.info(sql)
        ret = LogTask.db.execute_sql(sql)
        if ret<0:
            LogTask.logger.info("sql:%s ret:%d", sql, ret)
            raise LogTaskError, "task done error sql:%s ret:%d"%(sql, ret)
        params = {'status':2,'end_time':'CURRENT_TIMESTAMP'}
        self.log_task_timepoint(oprcode, params )

    def log_task_timepoint(self, oprcode, params):
        ip = LogTask.outip.getip()
        sql = '''insert into vm_task_log(oprcode,'''
        keys = params.keys()
        key1  = keys[0]
        key2  = keys[1]
        values = params.values()
        value1 = values[0]
        value2 = values[1]
        sql_log = sql+"%s,%s,ip,log_time) values(%s,%s,%s,'%s', CURRENT_TIMESTAMP)"%(key1, key2, oprcode, value1,value2, ip) 
        LogTask.logger.info(sql_log)
        ret = LogTask.db.execute_sql(sql_log)
        if ret<1:
            LogTask.logger.info("sql:%s ret:%d", sql_log, ret)
            raise LogTaskError, "log task error sql:%s ret:%d"%(sql_log, ret)
    
    def log(self, server_id, group_id, task_id, **kwargs):
        params = {}
        params.update(kwargs)
        oprcode = self.get_oprcode_bytask(server_id, group_id, task_id)
        LogTask.logger.info("oprcode:%s", oprcode)
        self.log_task_timepoint(oprcode, params)




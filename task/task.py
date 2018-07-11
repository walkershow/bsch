#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : task.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 11.07.2018 10:54:1531277692
# Last Modified Date: 11.07.2018 10:54:1531277692
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-12 16:29:06 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-12 16:29:06 
'''

import sys
import datetime
import os
import shutil
import time
import fnmatch
import threading
import logging
import logging.config
sys.path.append("..")
import dbutil
import utils
import action.action

logger = None

class TaskError(Exception):
    pass

class Task(object):
    '''任务
    '''
    def __init__(self, id, is_default,  db, rid=None):
        self.id = id
        print db
        self.db = db 
        self.is_default = is_default
        self.rid = rid
        self.__initTask()

    def __initTask(self):
        sql = "select id,task_name,script_file from vm_task where id=%d"%(self.id)
        res = self.db.select_sql(sql)
        # logger.info("init task data:%d",ret)
        if not res:
            raise TaskError,"%s sql get empty res"%(sql)
        self.task_name = res[0][1]
        self.script_file = res[0][2]
    
    def allot2(self):
        '''分配执行脚本替换掉默认的执行脚本
        cover_script_file:执行脚本路径文件名(任务脚本拷贝到这个路径含文件名)
        '''
        try:
            filename = utils.auto_encoding(self.script_file)
            #logger.info("allot script:%s", filename)
            print "allot script:", filename
            # shutil.copy(filename, cover_script_file)
        except (OSError,IOError), e:
            # logger.error('exception', exc_info = True)
            raise TaskError,"cannot copy %s ,error:%s"\
                        %(filename,e.message)
    
    def allot(self,  cover_script_file):
        try:
            mac = action.action.MakeActionScript(self.id, self.db)
            str = mac.make()
        except Exception,e:
            raise TaskError,"MakeActionScript error:%s"\
                        %(e.message)
        print "allot str", str
        print cover_script_file
        fp = open(cover_script_file, "w+")
        fp.write(str)
        fp.close()
            

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    t=Task(1,1,dbutil)
    t.allot("d:\\2.bat")




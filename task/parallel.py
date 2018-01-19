# -*- coding: utf-8 -*-

import sys
import datetime
import os
import shutil
import time
import fnmatch
import logging
import logging.config
sys.path.append("..")
import dbutil
import random


class ParallelControlException(Exception):
    pass

class ParallelControl(object):
    '''任务分配'''


    def __init__(self, server_id, db, logger):
        self.db = db 
        self.server_id = server_id
        self.logger = logger


    def is_ran_out_parallel_num(self, task_group_id):
        '''是否跑光了并行数
        '''
        pnum = self.get_parallel_num(task_group_id)
        if pnum is None:
            self.logger.info("pnum is None")
            return True
        allocated_num = self.get_allocated_num_on_ipchange(task_group_id)
        self.logger.info("pnum:%d,allocate_num:%d", pnum, allocated_num)
        if allocated_num>=pnum:
            return True
        return False

    def get_ran_out_parallel_task_set(self):
        sql = "select a.task_group_id from vm_parallel_control a,vm_parallel_control_info b where "\
        "a.task_group_id=b.task_group_id and b.allocated_num>=a.parallel_num and b.server_id=%d"%(self.server_id)
        res = self.db.select_sql(sql)
        id_set = set()
        for r in res:
            id_set.add(r[0])
        return id_set


    def is_parallel(self, task_group_id):
        sql = "select * from vm_parallel_control where task_group_id=%d"%(task_group_id)
        res = self.db.select_sql(sql)
        if res:
            return res[0][0]
        return None

    def get_parallel_num(self, task_group_id):
        sql = "select * from vm_parallel_control where task_group_id=%d"%(task_group_id)
        res = self.db.select_sql(sql)
        if res:
            return res[0][0]
        return None
    
    def get_allocated_num_on_ipchange(self, task_group_id):
        sql = "select allocated_num from vm_parallel_control_info where server_id=%d and task_group_id=%d"%(self.server_id, task_group_id)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        if res:
            return res[0][0]
        return 0

    def add_allocated_num(self, task_group_id):
        if not self.get_parallel_num(task_group_id):
            self.logger.info("task_group_id:%d not allow parallel", task_group_id)
            return
        sql = "insert into vm_parallel_control_info(server_id,task_group_id,allocated_num,update_time) values("\
        "%d,%d,1,CURRENT_TIMESTAMP) on duplicate key update allocated_num=allocated_num+1,update_time=CURRENT_TIMESTAMP  "%(
            self.server_id, task_group_id)
        # sql = "update vm_parallel_control_info set allocated_num=allocated_num+1,update_time=CURRENT_TIMESTAMP  where server_id=%d and task_group_id=%d"%(
        #     server_id, task_group_id)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise ParallelControlException,"%s excute error;ret:%d"%(sql, ret)

    def reset_allocated_num(self ):
        self.logger.info("server:%d reset allocated when ip change", self.server_id)
        sql = "update vm_parallel_control_info set allocated_num=0,update_time=CURRENT_TIMESTAMP where server_id=%d "%(
            self.server_id)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise ParallelControlException,"%s excute error;ret:%d"%(sql, ret)
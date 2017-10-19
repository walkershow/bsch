# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-13 11:39:55 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-13 11:39:55 
'''

import sys
import datetime
import os
import shutil
import time
import fnmatch
import logging
import logging.config
from task import Task
sys.path.append("..")
import dbutil
from random import choice
logger = None

class TaskGroupError(Exception):
    pass

class TaskGroup(object):
    '''任务组'''
    tasks = []
    def __init__(self, id,  db):
        self.id = id
        self.db = db 

    #groupid:0,task_id:0 表示默认任务
    def __initValidTasks(self):
        sql = "select b.id,b.task_id,a.start_time,a.end_time,a.allot_times,a.ran_times from vm_task_allot_impl a,vm_task_group b, vm_task c where b.id=%d \
                and b.task_id=a.task_id and b.id =a.id and a.task_id=c.id and c.status=1 \
                and time_to_sec(NOW()) between time_to_sec(a.start_time) and time_to_sec(a.end_time) \
                and a.ran_times<a.allot_times and b.id>0 order by rand()"%(self.id)

        res = self.db.select_sql(sql)
        task = {}
        self.tasks = []
        print res 
        for row in res:
            #print row
            task = {'task_id':row[1], 'start_time':row[2], 'end_time':row[3], 'times':row[4], 'ran_times':row[5],'is_default':False}
            #print "task:", task
            self.tasks.append(task)
    
    @staticmethod
    def getDefaultTask(db):
        sql = "select id,task_id,ran_times from vm_task_group where  id=0"
        res = db.select_sql(sql)
        if not res:
            raise TaskGroupError,"%s sql get empty res"%(sql)
        row = res[0]
        task = {'task_id':row[1], 'start_time':0, 'end_time':0, 'times':9999999, 'ran_times':row[2], 'is_default': True}
        return Task(task["task_id"], True, db)
        

    #deprecated 
    def __valid_time(self, start, end):
        sql = "select 1 from dual where time_to_sec(NOW()) between time_to_sec('%s') and time_to_sec('%s')"%(start, end)
        print sql
        res = self.db.select_sql(sql)
        if res:
            return True
        return False

    #deprecated 
    def __valid_times(self, times, ran_times):
        if ran_times < times:
            return True
        return False

    def add_ran_times(self, task_id):
        sql ="update vm_task_group set ran_times=ran_times+1 where id=%d and task_id=%d"%(self.id, task_id)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod        
    def add_default_ran_times(db):
        sql ="update vm_task_group set ran_times=ran_times+1 where id=0"
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    #如果刚好过那个时间点,次数可能会对不上
    def add_impl_ran_times(self, task_id):
        sql ="update vm_task_allot_impl set ran_times=ran_times+1 where id=%d and task_id=%d \
             and time_to_sec(NOW()) between time_to_sec(start_time) and time_to_sec(end_time)"%(self.id, task_id)

        ret = self.db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def impl_task_templ(db):
        sql_trunc =" truncate table vm_task_allot_impl_tmp"
        ret = db.execute_sql(sql_trunc)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
        sql = "select id,sum(times) as total,templ_id from vm_task_group where id>0 group by id order by id"
        sql_templ = "select percent,time_to_sec(start_time),time_to_sec(end_time),id,sub_id,detail_id from vm_task_allot_templ where id=%d order by sub_id"
        sql_alltask = "select task_id, times from vm_task_group  where id=%d "
        #sql_alltask = "select task_id, times from vm_task_group a,vm_task b where id=%d and a.task_id=b.id and b.status=1"

        res = db.select_sql(sql)
        for r in res:
            id = r[0]
            total = r[1]
            templ_id = r[2]
            sql = sql_alltask%(id)
            print "all_task:",sql
            res_alltask = db.select_sql(sql)
            task_dict = {}
            for t in res_alltask:
                task_dict[t[0]] = t[1]
            print "task_dict:",task_dict
            sql = sql_templ%(templ_id)
            print "templ:",sql
            res = db.select_sql(sql)
            for r in res:
                print "templ row:", r
                p = r[0]
                start_time = int(r[1])
                end_time = int(r[2])
                print "start_time,end_time",start_time,end_time
                templ_id = r[3]
                templ_sub_id = r[4]
                detail_id = r[5]
                cur_allot_num = int(round(total * p/100))
                print "cur_allot_num", cur_allot_num
                for i in range(cur_allot_num):
                    if task_dict:
                        task_id = choice(task_dict.keys())
                        print "task_id:",task_id
                        task_dict[task_id]= task_dict[task_id]- 1
                        print task_dict
                        if task_dict[task_id] <=0:
                            task_dict.pop(task_id)
                        sql = '''insert into vm_task_allot_impl_tmp(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id,detail_id, update_time)
                            values(%d,%d,sec_to_time(%d),sec_to_time(%d),allot_times+1,%d, %d,%d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 ''' 
                        sql_impl = sql%(id,task_id,start_time,end_time, templ_id, templ_sub_id,detail_id)
                        print sql_impl
                        ret = db.execute_sql(sql_impl)
                        if ret<0:
                            raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
    
    @staticmethod
    def impl_task_templ_detail(db):
        sql_trunc =" truncate table vm_task_allot_impl"
        ret = db.execute_sql(sql_trunc)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
        sql = '''select id,task_id,allot_times,templ_id,templ_sub_id,detail_id,time_to_sec(start_time),time_to_sec(end_time) from vm_task_allot_impl_tmp order by id,task_id'''
        res = db.select_sql(sql)
        for r in res:
            id,task_id,allot_times,templ_id,templ_sub_id,detail_id,start_time,end_time = r
            print r
            print allot_times
            sql = "select id,start_min,end_min from vm_task_allot_templ_detail where id=%d order by sub_id"%(detail_id)
            res = db.select_sql(sql)
            if not res:
                print sql, " empty"
                continue
            num = 1
            while True:
                if num>allot_times:
                    break
                for td in res:
                    start = td[1]
                    end = td[2]
                    sql = '''insert into vm_task_allot_impl(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id, update_time)
                        values(%d,%d,date_add(sec_to_time(%d), interval %d minute),date_add(sec_to_time(%d), interval %d minute),
                        allot_times+1,%d, %d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 ''' 
                    sql_impl = sql%(id,task_id,start_time, start, start_time, end, templ_id, templ_sub_id)
                    print sql_impl
                    ret = db.execute_sql(sql_impl)
                    if ret<0:
                        raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
                    num = num +1
                    if num>allot_times:
                        break

    @staticmethod
    def reset_rantimes_today(db):
        '''更新当天任务'''

        sql ="update vm_task_group set ran_times_lastday = ran_times ,ran_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) where templ_id>0" 
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def reset_rantimes_alltask(db):
        '''更新所有任务包括跨天'''

        sql ="update vm_task_group set ran_times_lastday = ran_times ,ran_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) " 
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def reset_rantimes_allot_impl(db):
        TaskGroup.impl_task_templ(db)
        TaskGroup.impl_task_templ_detail(db)
    
    def choose_vaild_task(self):
        self.__initValidTasks()
        for t in self.tasks:
            task_id = t["task_id"]
            print "task_id:", task_id
            return Task(t["task_id"], False, self.db)
        return TaskGroup.getDefaultTask(self.db)


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm2"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    t=TaskGroup(1,dbutil)
    t.choose_vaild_task()

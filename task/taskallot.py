# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-13 16:32:04 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-13 16:32:04 
'''

import sys
import datetime
import os
import shutil
import time
import fnmatch
import logging
import logging.config
from task import Task,TaskError
from taskgroup import TaskGroup
sys.path.append("..")
import dbutil
import random

logger = None

class TaskAllotError(Exception):
    pass

class TaskAllot(object):
    '''任务分配'''

    #cur_date = None

    def __init__(self,want_init, server_id, db):
        self.db = db 
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id
        self.groupids_dict = {}



    def log_task_id(self,id, task_id):
        sql ="update vm_allot_task set cur_task_id=%d where id=%d"%(task_id,id)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise TaskAllotError,"%s excute error;ret:%d"%(sql, ret)
    
    def reset_when_newday(self):
        '''新的一天重置所有运行次数'''
        today = datetime.date.today()
        print today,self.cur_date
        if today != self.cur_date:
            
            #统一到一个w = 1的进程进行更新
            if self.want_init == 1:
                print "start new day to reinit..."
                #logger.info("start new day to reinit...")
                TaskGroup.reset_rantimes_today(self.db)
                TaskGroup.reset_rantimes_allot_impl(self.db)
                self.cur_date = today
                print "cur_date",self.cur_date
                print "end new day to reinit..."
                #logger.info("end new day to reinit...")

    def get_running_groupids(self):
        '''获取运行状态的任务组
        '''
        group_ids = []
        sql = "select task_group_id from vm_cur_task where server_id=%d and status=1 and task_group_id !=0 "%(self.server_id)
        print sql
        res = dbutil.select_sql(sql)
        for r in res:
            id = r[0]
            group_ids.append(id)
        return group_ids

    def allot_by_priority(self, default_path):
        try:
            self.reset_when_newday()
            sql = '''SELECT
                            a.id
                        FROM
                            vm_task_group b,
                            vm_task_allot_impl a,
                            vm_allot_task_by_servergroup c,
                            vm_task d,
                            vm_server_group f
                        WHERE
                            b.id = a.id
                        AND b.task_id = a.task_id
                        AND d.id = b.task_id
                        AND d. STATUS = 1
                        AND f.id = c.server_group_id
                        and f.status =1
                        AND c.task_group_id = b.id
                        AND time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
                        AND time_to_sec(a.end_time)
                        AND a.ran_times < a.allot_times
                        AND b.id > 0
                        AND c.task_group_id = a.id
                        and b.priority>0
                        AND f.server_id = %d order by b.priority'''%(self.server_id)
                    

            res = self.db.select_sql(sql)
            ids = []
            for r in res:
                ids.append(r[0])
            rids = self.get_running_groupids()
                
            selected_ids = list(set(ids).difference(set(rids)))
            print selected_ids

            # logger.info("init task data:%d",ret)
            if not selected_ids:
                #不存在优先级高的任务组,执行随机分配
                # logger.info("no priority task, get rand taskgroup")
                print("no priority task, get rand taskgroup")
                return self.allot_by_rand(default_path)
            else:
                return self.handle_taskgroup(row[0], default_path).id, selected_ids[0]
        except TaskError, t:
            raise TaskAllotError,"excute error:%s"%( t.message)


    def allot_by_rand(self, default_path):
        try:
            sql = '''SELECT
                         a.id
                     FROM
                         vm_task_group b,
                         vm_task_allot_impl a,
                         vm_allot_task_by_servergroup c,
                         vm_task d,
                         vm_server_group f
                     WHERE
                         b.id = a.id
                     AND b.task_id = a.task_id
                     AND d.id = b.task_id
                     AND d. STATUS = 1
                     AND f.id = c.server_group_id
                     and f.status =1
                     AND c.task_group_id = b.id
                     AND time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
                     AND time_to_sec(a.end_time)
                     AND a.ran_times < a.allot_times
                     AND b.id > 0
                     AND c.task_group_id = a.id
                     and b.priority=0
                     AND f.server_id = %d'''%(self.server_id)
                    
            res = self.db.select_sql(sql)
            ids = []
            for r in res:
                ids.append(r[0])
            rids = self.get_running_groupids()
                
            selected_ids = list(set(ids).difference(set(rids)))

            task = None
            task_group_id = 0 
            if not selected_ids:
                # logger.info("get default taskgroup")
                task = TaskGroup.getDefaultTask(self.db)
                print task
                if task.gen_type == 0:
                    task.allot2(default_path)
                else:
                    task.allot(default_path)

                # TaskGroup.add_default_ran_times(self.db)
            else:
                task_group_id = random.choice(selected_ids)
                task = self.handle_taskgroup(task_group_id, default_path)
            return task.id, task_group_id
        except TaskError, t:
            raise TaskAllotError,"excute error:%s"%( t.message)

        

    def handle_taskgroup(self, task_group_id, default_path):
        print "task_group_id:", task_group_id
        # logger.info("task_group_id:%d", task_group_id)
        tg = TaskGroup(task_group_id, self.db)
        task = tg.choose_vaild_task()
        if task.gen_type == 0:
            task.allot2(default_path)
        else:
            task.allot(default_path)
        # if task.is_default:
        #     TaskGroup.add_default_ran_times(self.db)
        # else:
        #     tg.add_ran_times(task.id)
        #     tg.add_impl_ran_times(task.id)
        return task
    
    def add_ran_time(self, task_id,task_group_id):
        ''' 分配成功后有可用profile 时计数
        '''
        tg = TaskGroup(task_group_id, self.db)
        if id==0:
            TaskGroup.add_default_ran_times(self.db)
        else:
            tg.add_ran_times(task_id)
            tg.add_impl_ran_times(task_id)


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    #t=TaskAllot(0, 1, dbutil)
    task_group_id = None
    print len(sys.argv)
    if len(sys.argv)>1:
        task_group_id = int(sys.argv[1])
        print task_group_id
        TaskGroup.reset_rantimes_by_task_group_id(dbutil, task_group_id)
    # while True:
    #     t.allot_by_priority("d:\\10.bat")
    #     # t.allot_by_rand("d:\\10.bat")
    #     time.sleep(1)
        TaskGroup.reset_rantimes_allot_impl(dbutil, task_group_id)
    t=TaskAllot(1,1, dbutil)
    if task_group_id :
        exit(0)
    while True:
        try:
            t.reset_when_newday()
            time.sleep(10)
        except:
            time.sleep(5)
            continue

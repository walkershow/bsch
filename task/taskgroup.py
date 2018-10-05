#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : taskgroup.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 03.05.2018 19:21:1525346483
# Last Modified Date: 03.05.2018 19:21:1525346483
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
'''
@Author: coldplay
@Date: 2017-05-13 11:39:55
@Last Modified by:   coldplay
@Last Modified time: 2017-05-13 11:39:55
'''

from __future__ import division

import logging
import logging.config
import sys
from random import choice

sys.path.append("..")
import dbutil
from task import Task

logger = None


class TaskGroupError(Exception):
    pass


class TaskGroup(object):
    '''任务组'''
    tasks = []

    def __init__(self,  db):
        self.db = db
        self.task_group_dict={}
        self.id = None
        

    def can_be_run(self, task_group_id, task_id):
        sql = '''select 1 from vm_task_allot_impl a where 
                     time_to_sec(NOW()) between time_to_sec(a.start_time) and time_to_sec(a.end_time)
                     and a.ran_times<a.allot_times and a.id={0} and
                     a.task_id={1} ''' .format(task_group_id, task_id)
        res = self.db.select_sql(sql)
        if res and len(res)>0:
            return True
        return False


    # groupid:0 表示默认任务
    def __initValidTasks(self, task_group_id):
        if self.task_group_dict.has_key(task_group_id) and self.task_group_dict[task_group_id]:
            self.id = task_group_id
            return self.task_group_dict[task_group_id].pop()
        else:
            # sql = "select b.id,b.task_id,a.start_time,a.end_time,a.allot_times,a.ran_times from vm_task_allot_impl a,vm_task_group b, vm_task c where b.id=%d \
                    # and b.task_id=a.task_id and b.id =a.id and a.task_id=c.id and c.status=1 \
                    # and time_to_sec(NOW()) between time_to_sec(a.start_time) and time_to_sec(a.end_time) \
                    # and a.ran_times<a.allot_times and b.id>0 order by rand()" % (task_group_id)
            sql = '''SELECT
                b.id,
                b.task_id,
                a.start_time,
                a.end_time,
                a.allot_times,
                a.ran_times
            FROM
            vm_task_allot_impl a,
            vm_task_group b,
            vm_task c
            WHERE
                b.id = {0}
            AND b.task_id = a.task_id
            AND b.id = a.id
            AND a.task_id = c.id
            AND c. STATUS = 1
            AND time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
            AND time_to_sec(a.end_time)
            AND a.ran_times < a.allot_times
            AND b.ran_times < b.times
            AND b.id > 0
            ORDER BY rand()'''.format(task_group_id)
            print (sql)
            res = self.db.select_sql(sql)
            task = {}
            self.tasks = []
            for row in res:
                # task = {
                    # 'task_id': row[1],
                    # 'start_time': row[2],
                    # 'end_time': row[3],
                    # 'times': row[4],
                    # 'ran_times': row[5],
                    # 'is_default': False
                # }
                if not self.task_group_dict.has_key(task_group_id):
                    self.task_group_dict[task_group_id]=[row[1]]
                else:
                    self.task_group_dict[task_group_id].append(row[1])
            if self.task_group_dict.has_key(task_group_id) and self.task_group_dict[task_group_id]:
                self.id = task_group_id
                return self.task_group_dict[task_group_id].pop()
        return None

         
    def __initValidTasks2(self):
        sql = "select b.id,b.task_id from vm_task_group b, vm_task c where b.id=%d \
                and b.task_id=c.id and c.status=1 \
                and b.ran_times<b.times and b.id>0 order by rand()" % (
            self.id)

        res = self.db.select_sql(sql)
        task = {}
        self.tasks = []
        for row in res:
            task = {
                'task_id': row[1],
                'start_time': 0,
                'end_time': 0,
                'times': 0,
                'ran_times': 0,
                'is_default': False
            }
            self.tasks.append(task)
    
    @staticmethod
    def can_run_default(db, server_id, vm_id, tty, uty ):
        sql = '''select count(1) from zero_schedule_list where time_to_sec(NOW()) 
        between time_to_sec(start_time) and time_to_sec(end_time) 
        and ran_times>=run_times and ran_times>0 and server_id=%d and vm_id=%d and'''
        if uty == 0 and tty ==1:
            sql = sql + " user_type = 0 and terminal_type=1"
        elif uty == 7 and tty ==1:
            sql = sql + " user_type = 7 and terminal_type=1"
        elif uty == 0 and tty==2:
            sql = sql + " user_type = 0 and terminal_type=2"
        elif uty != 0 and tty==1:
            sql = sql + " user_type != 0 and terminal_type=1"
        elif uty != 0 and tty==2:
            sql = sql + " user_type != 0 and terminal_type=2"
        sql = sql % (server_id, vm_id)
        # logger.info(sql)
        res = db.select_sql(sql)
        if res:
            count = res[0][0]
            if count >= 1:
                return False
        return True

    @staticmethod
    def getDefaultTask(db, server_id, vm_id, not_baidu = 0):
        sql = "select id,user_type,terminal_type from zero_schedule_list where time_to_sec(NOW()) between time_to_sec(start_time) and time_to_sec(end_time) \
                and ran_times<run_times and server_id=%d and vm_id=%d"
        if not_baidu == 0:
            sql = sql + " and user_type=0"
            sql = sql % (server_id, vm_id)
        elif not_baidu == 7:
            sql = sql + " and user_type=7"
            sql = sql % (server_id, vm_id)
        else:
            sql = sql + " and user_type>0"
            sql = sql % (server_id, vm_id)
        print sql
        #logger.info(sql)
        res                     = db.select_sql(sql)
        dtask                   = []
        task_id_list            = []
        task_id_list_pc_baidu   = [10000]
        task_id_list_mobi_baidu = [10006]
        task_id_list_pc         = [None,10001, 10002, 10003, 10004, 10005,
        10012, 10012,10013,10014]
        task_id_list_mobi       = [None,10007, 10008, 10009, 10010, 10011, 
        None, None, None, None]
        if res:
            for r in res:
                id  = r[0]
                uty = r[1]
                tty = r[2]
                if not TaskGroup.can_run_default(db, server_id ,vm_id, tty, uty):
                    return None
                if tty == 1 and uty == 0:
                    task_id_list = task_id_list_pc_baidu
                elif tty == 1 and uty != 0:
                    task_id_list = task_id_list_pc

                elif tty == 2 and uty == 0:
                    task_id_list = task_id_list_mobi_baidu
                elif tty == 2 and uty != 0:
                    task_id_list = task_id_list_mobi
                else:
                    logger.error("unkonw tty:%d,uty:%d",tty, uty)
                task_id = task_id_list[uty]
                if task_id is None:continue
                task = {
                    'id': id,
                    'task_id': task_id,
                    'start_time': 0,
                    'end_time': 0,
                    'times': 9999999,
                    'ran_times': 0,
                    'is_default': True
                }
                dtask.append(task)
        else:
            return None
        if len(dtask)==0:
            return None
        task = choice(dtask)
        return Task(task["task_id"], True, db, task['id'])

    @staticmethod
    def getNineTask(db, server_id, vm_id):
        return Task(9999, True, db, None)


    def add_ran_times(self, task_id):
        sql = "update vm_task_group set ran_times=ran_times+1 where id=%d and task_id=%d" % (
            self.id, task_id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    def add_ran_times2(self, task_id, task_group_id):
        sql = "update vm_task_group set ran_times=ran_times+1 where id=%d and task_id=%d" % (
            task_group_id, task_id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    @staticmethod
    def add_default_ran_times(db):
        sql = "update vm_task_group set ran_times=ran_times+1 where id=0"
        ret = db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    def add_impl_ran_times(self, task_id):
        sql = '''update vm_task_allot_impl set ran_times=ran_times+1 where id=%d and task_id=%d
        and time_to_sec(NOW()) >= time_to_sec(start_time) and
        time_to_sec(now())<time_to_sec(end_time)''' % (self.id, task_id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    @staticmethod
    def impl_task_templ(db, task_group_id=None):
        sql = ""
        if not task_group_id:
            sql_trunc = " truncate table vm_task_allot_impl_tmp"
            ret = db.execute_sql(sql_trunc)
            if ret < 0:
                raise TaskGroupError, "%s excute error;ret:%d" % (sql_trunc,
                                                                  ret)
            sql = "select id,sum(times) as total,templ_id from vm_task_group where id>0 group by id order by id"
        else:
            sql_del = "delete from vm_task_allot_impl_tmp where id=%d" % (
                task_group_id)
            ret = db.execute_sql(sql_del)
            if ret < 0:
                raise TaskGroupError, "%s excute error;ret:%d" % (sql_del, ret)
            sql = "select id,sum(times) as total,templ_id from vm_task_group where id=%d group by id order by id" % (
                task_group_id)
        sql_templ = "select percent,time_to_sec(start_time),time_to_sec(end_time),id,sub_id,detail_id from vm_task_allot_templ where id=%d order by sub_id"
        sql_alltask = "select task_id, times from vm_task_group  where id=%d "
        #sql_alltask = "select task_id, times from vm_task_group a,vm_task b where id=%d and a.task_id=b.id and b.status=1"

        res = db.select_sql(sql)
        for r in res:
            id = r[0]
            total = r[1]
            templ_id = r[2]
            sql = sql_alltask % (id)
            if total == 0 or total is None:
                continue
            res_alltask = db.select_sql(sql)
            task_dict = {}
            task_templ_dict = {}
            for t in res_alltask:
                if t[1] > 0:
                    task_dict[t[0]] = t[1]
                    task_templ_dict[t[0]] = 0
            sql = sql_templ % (templ_id)
            res = db.select_sql(sql)
            for r in res:
                p = r[0]
                start_time = int(r[1])
                end_time = int(r[2])
                templ_id = r[3]
                templ_sub_id = r[4]
                detail_id = r[5]
                cur_allot_num = int(round(total * p / 100))

                if cur_allot_num == 0:
                    continue
                allot_total = cur_allot_num
                # for i in range(cur_allot_num):
                while True:
                    if allot_total <= 0:
                        break
                    if not task_dict:
                        break
                    for t, v in task_dict.items():
                        task_id = t
                        # task_id = choice(task_dict.keys())
                        task_dict[task_id] = task_dict[task_id] - 1
                        task_templ_dict[task_id] = task_templ_dict[task_id] + 1
                        allot_total = allot_total - 1
                        cnt = task_dict[task_id]
                        if cnt <= 0:
                            task_dict.pop(task_id)
                        if allot_total <= 0:
                            break
                for t, n in task_templ_dict.items():
                    allot_times = n
                    if allot_times == 0:
                        continue
                    task_id = t
                    sql = '''insert into vm_task_allot_impl_tmp(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id,detail_id, update_time)
                        values(%d,%d,sec_to_time(%d),sec_to_time(%d),%d,%d, %d,%d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 '''
                    sql_impl = sql % (id, task_id, start_time, end_time,
                                      allot_times, templ_id, templ_sub_id,
                                      detail_id)
                    task_templ_dict[task_id] = 0
                    ret = db.execute_sql(sql_impl)
                    if ret < 0:
                        raise TaskGroupError, "%s excute error;ret:%d" % (
                            sql_impl, ret)

    @staticmethod
    def impl_task_templ_detail(db, task_group_id=None):
        sql = ""
        if not task_group_id:
            sql_trunc = " truncate table vm_task_allot_impl"
            ret = db.execute_sql(sql_trunc)
            if ret < 0:
                raise TaskGroupError, "%s excute error;ret:%d" % (sql_impl,
                                                                  ret)
            sql = '''select id,task_id,allot_times,templ_id,templ_sub_id,detail_id,time_to_sec(start_time),time_to_sec(end_time) from vm_task_allot_impl_tmp order by id,task_id'''
        else:
            sql_del = "delete from vm_task_allot_impl where id=%d" % (
                task_group_id)
            ret = db.execute_sql(sql_del)
            if ret < 0:
                raise TaskGroupError, "%s excute error;ret:%d" % (sql_del, ret)
            sql = '''select id,task_id,allot_times,templ_id,templ_sub_id,detail_id,time_to_sec(start_time),time_to_sec(end_time)
            from vm_task_allot_impl_tmp where id=%d order by id,task_id''' % (
                task_group_id)
        res = db.select_sql(sql)
        for r in res:
            id, task_id, allot_times, templ_id, templ_sub_id, detail_id, start_time, end_time = r
            sql = "select id,start_min,end_min from vm_task_allot_templ_detail where id=%d order by sub_id" % (
                detail_id)
            res = db.select_sql(sql)
            if not res:
                continue
            pos = len(res) + 1
            for td in res:
                start = td[1]
                end = td[2]
                pos = pos - 1
                one_times = int(round(allot_times / pos))
                if one_times <= 0:
                    continue

                sql = '''insert into vm_task_allot_impl(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id, update_time)
                    values(%d,%d,date_add(sec_to_time(%d), interval %d minute),date_add(sec_to_time(%d), interval %d minute),
                    %d,%d, %d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 '''
                sql_impl = sql % (id, task_id, start_time, start, start_time,
                                  end, one_times, templ_id, templ_sub_id)
                ret = db.execute_sql(sql_impl)
                if ret < 0:
                    raise TaskGroupError, "%s excute error;ret:%d" % (sql_impl,
                                                                      ret)
                allot_times = allot_times - one_times

    @staticmethod
    def reset_rantimes_by_task_group_id(db, task_group_id):
        '''更新当天任务'''

        sql ="update vm_task_group set times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range)))" \
        "where templ_id>0 and id=%d"%(task_group_id)
        ret = db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    @staticmethod
    def reset_rantimes_today(db):
        '''更新当天任务'''

        sql = "update vm_task_group set ran_times_lastday = ran_times ,ran_times=0,allot_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) where templ_id>0"

        ret = db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    @staticmethod
    def reset_rantimes_alltask(db):
        '''更新所有任务包括跨天'''

        sql = "update vm_task_group set ran_times_lastday = ran_times ,ran_times=0,allot_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) "

        ret = db.execute_sql(sql)
        if ret < 0:
            raise TaskGroupError, "%s excute error;ret:%d" % (sql, ret)

    @staticmethod
    def reset_rantimes_allot_impl(db, task_group_id=None):
        TaskGroup.impl_task_templ(db, task_group_id)
        TaskGroup.impl_task_templ_detail(db, task_group_id)

    def choose_vaild_task(self, server_id, task_group_id):
        while True:
            tid = self.__initValidTasks(task_group_id)
            if tid:
                if self.can_be_run(task_group_id, tid):
                    return Task(tid, False, self.db)

            else:
                break
        return None

    def choose_vaild_task2(self, server_id, vm_id=None):
        self.__initValidTasks2()
        for t in self.tasks:
            task_id = t["task_id"]
            return Task(t["task_id"], False, self.db)
        return None

def test_choose_task():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    tg = TaskGroup(dbutil)
    for i in xrange(2):
        t = tg.choose_vaild_task(11,307)

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    test_choose_task()
    # t = TaskGroup(111, dbutil)
    # # TaskGroup.reset_rantimes_allot_impl(dbutil)
    # t.add_default_ran_times(dbutil)
    # t.add_impl_ran_times(111)
    # t.add_ran_times(111)
    # t.choose_vaild_task()

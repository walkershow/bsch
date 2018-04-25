#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : taskallot_rolling.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 07.04.2018 18:14:1523096068
# Last Modified Date: 24.04.2018 19:29:1524569373
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
'''
@Author: coldplay
@Date: 2017-05-13 16:32:04
@Last Modified by:   coldplay
@Last Modified time: 2017-05-13 16:32:04
'''

import sys
import datetime
import time
import logging
import logging.config
from taskgroup import TaskGroup
import dbutil
import utils
from task import TaskError
from taskallot import TaskAllot
from parallel import ParallelControl
from rolling_user import UserAllot
from user_ec import UserAllot_EC
sys.path.append("..")



class TaskAllotRolling(TaskAllot):
    '''任务分配'''

    def get_task_type(self, task_id):
        sql = '''select user_type from vm_task where id=%d ''' % (
            task_id)
        res = self.db.select_sql(sql)
        if not res:
            return None 
        return res[0][0]

    def get_candidate_gid(self, vm_id, type=0):
        sql = '''SELECT
                        distinct b.id
                    FROM
                        vm_task_group b,
                        vm_allot_task_by_servergroup c,
                        vm_task d,
                        vm_server_group f
                    WHERE
                        d.id = b.task_id
                    AND d.STATUS = 1
                    AND f.id = c.server_group_id
                    and f.status =1
                    AND c.task_group_id = b.id
                    AND b.ran_times < b.times
                    AND b.id > 0
                    AND f.server_id = %d order by b.id''' % (self.server_id)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        ids = set()
        for r in res:
            ids.add(r[0])
        rid_set = self.get_band_run_groupids()
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)
        self.selected_ids = list(set(ids) - rid_set)

    def handle_taskgroup(self, task_group_id, vm_id):
        tg = TaskGroup(task_group_id, self.db)
        task = tg.choose_vaild_task2(self.server_id, vm_id)
        if not task:
            return None
        self.logger.warn("==========get the valid task:%d==========", task.id)
        uty = self.get_task_type(task.id)
        print uty
        logger.info("task uty:%d", uty)
        if uty == 20:
            ret = self.user_ec.allot_user(vm_id, task_group_id, task.id)
        else:
            ret = self.user.allot_user(vm_id, task_group_id, task.id)
        print "the allot user ret", ret
        if not ret:
            self.logger.warn("vm_id:%d,task_id:%d,task_group_id:%d no user to run",
                        vm_id, task.id, task_group_id)
            return None

        return task


def allot_test(dbutil):
    '''任务分配测试
    '''
    task_group_id = None
    print len(sys.argv)
    if len(sys.argv) > 1:
        task_group_id = int(sys.argv[1])
        print task_group_id
        TaskGroup.reset_rantimes_by_task_group_id(dbutil, task_group_id)
        # while True:
        #     t.allot_by_priority("d:\\10.bat")
        #     # t.allot_by_rand("d:\\10.bat")
        #     time.sleep(1)
        TaskGroup.reset_rantimes_allot_impl(dbutil, task_group_id)
    t = TaskAllotRolling(1, 1, None, dbutil)
    if task_group_id:
        exit(0)
    while True:
        try:
            t.reset_when_newday()
            time.sleep(10)
        except Exception, e:
            print "except", e
            time.sleep(5)
            continue


def getTask(dbutil,logger):
    from user import UserAllot
    pc = ParallelControl(1, dbutil, logger)
    user = UserAllot(1, pc, dbutil, logger)
    user_ec = UserAllot_EC(1, pc, dbutil, logger)
    t = TaskAllotRolling(0, 1, pc, user,user_ec, dbutil,logger)

    #t.allot_by_default(2, 0)
    #t.allot_by_default(2, 6)
    # t.allot_by_default(2, 1)
    #t.allot_by_nine(1)
    # while True:
    ret = t.allot_by_priority(1)
    # print ret
    # time.sleep(5)
    # break


def get_default_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # console logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    #dbutil.db_host = "3.3.3.6"
    dbutil.db_name = "vm3"
    #dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    getTask(dbutil, logger)

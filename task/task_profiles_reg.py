#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : task_profiles.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 22.05.2018 10:40:1526956828
# Last Modified Date: 22.05.2018 15:08:1526972929
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-

import sys
import random
import logging
import logging.config
sys.path.append("..")
import dbutil
from logbytask.logtask import LogTask, LogTaskError
from task_profiles import TaskProfile

# from task.parallel import ParallelControl,ParallelControlException


class TaskProfile_Reg(TaskProfile):

    def get_uninited_profiles(self, vm_id, tty, uty, area):
        sql = " select a.profile_id,a.area from vm_profiles a,profiles b "\
            "where a.profile_id not in(select profile_id from vm_users "\
            "where server_id={0} and vm_id={1} and user_type={2} and " \
            "terminal_type={3} and user_type=11) and server_id={0} and vm_id={1} "\
            "and b.terminal_type = {3} and a.profile_id=b.id and a.area={4} " \
            .format( self.server_id, vm_id, uty, tty, area)
        print sql
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids

    def get_uninited_profiles2(self, vm_id, tty, uty, area):
        '''测试用，使用默认浏览器'''
        sql = '''select 1 from vm_users where server_id={0} and
        vm_id={1} and terminal_type={2} and area={3} and user_type=11 and
        profile_id=-1'''.format( self.server_id, vm_id,  tty, area)

        res = self.db.select_sql(sql)
        if res and len(res)>=1:
           return [] 
        else:
            return [-1]


    def get_task_usable_profiles(self, vm_id, terminal_type, user_type,
            task_group_id, area):
        if user_type == 10:
            user_type = 11
        uninited_profiles = self.get_uninited_profiles(vm_id, terminal_type,
                                                       user_type, area)
        print uninited_profiles
        if uninited_profiles:
            profile_id = random.choice(uninited_profiles)
            print "gogog", profile_id
            self.log_vm_user(vm_id, profile_id, terminal_type, user_type, area)
            print "gogog2", profile_id
            return profile_id
        return None

    def log_vm_user(self, vm_id, profile_id, terminal_type, user_type, area):
        sql = '''insert into vm_users(server_id,vm_id,profile_id,terminal_type,user_type,status,area) 
        values({0},{1},{2},{3},{4},0,{5})''' .format(self.server_id, vm_id, profile_id, 
                                     terminal_type, user_type, area)
        print sql
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise ZeroTaskError, "%s excute error;ret:%d" % (sql, ret)

    def set_cur_task_profile(self, vm_id, task_id, task_group_id, day, area):
        r = self.get_task_type(task_id)
        print r['standby_time'],r['inter_time']
        randtime = 1
        profile_id = self.get_task_usable_profiles(vm_id, r['terminal_type'],
                r['user_type'], task_group_id, area)
        print "==========="
        print profile_id
        print "==========="
        if not profile_id:
            return False
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id,
                                                   task_id)

        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,
        task_group_id,status,status2,start_time,oprcode,ran_minutes,user_type,
        terminal_type,standby_time, timeout,
        copy_cookie,click_mode,inter_time,area)
         value(%d,%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d,
                 %d,%d,%d,%d,%d,%d)''' %(
            self.server_id, vm_id, task_id, profile_id, task_group_id,
            -1, -1, oprcode,r['user_type'], r['terminal_type'],
            randtime,r['timeout'],
            r['copy_cookie'], r['click_mode'], r['inter_time'], int(area))
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
        return True


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
    global logger
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logger = get_default_logger()
    #pc = ParallelControl(g_serverid, dbutil)
    t = TaskProfile(1, dbutil, None, logger)
    # t.set_cur_task_profile(1, 410, 410,1)
    t.set_cur_task_profile(1, 10000, 0,1,1)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : task_profiles_ec.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 24.04.2018 17:10:1524561047
# Last Modified Date: 24.04.2018 17:30:1524562256
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-

# @CreateTime: Sep 13, 2017 3:47 PM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Jan 16, 2018 5:48 PM
# @Description: Modify Here, Please
import sys
import random
import logging
import logging.config
from task_profiles import TaskProfile
sys.path.append("..")
import dbutil
from logbytask.logtask import LogTask, LogTaskError



class TaskProfile_EC(TaskProfile):

    def get_inited_profiles(self, vm_id, tty, uty):
        sql = " select a.profile_id from vm_profiles a,profiles b "\
            "where a.profile_id not in(select profile_id from vm_users "\
            "where server_id=%d and vm_id=%d and user_type=%d and terminal_type=%d) and server_id=%d and vm_id=%d "\
            "and b.terminal_type = %d and a.profile_id=b.id " % (
                self.server_id, vm_id, uty, tty, self.server_id, vm_id, tty
            )
        print sql
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids
        

    def get_task_usable_profiles(self, vm_id, user_type, terminal_type, day):
        all_profiles = self.get_inited_profiles(vm_id, terminal_type,
                                                user_type )
        used_profiles = self.get_used_profiles(vm_id, user_type, terminal_type)
        # print all_profiles, used_profiles
        usable_profiles = list(
            set(all_profiles).difference(set(used_profiles)))
        # print usable_profiles
        profile_id = None
        if usable_profiles:
            profile_id = random.choice(usable_profiles)
        return profile_id

    def set_cur_task_profile(self, vm_id, task_id, task_group_id, day):
        (task_type, user_type, terminal_type,standby_time, timeout, copy_cookie,
        click_mode, inter_time) = self.get_task_type(task_id)
        print standby_time,inter_time
        randtime = 0
        self.logger.info(
            "task id:%d task_type:%d,user_type:%d, terminal_type:%d", task_id,
            task_type, user_type, terminal_type)
        # print "set_cur_task_profile:",task_type, terminal_typ
        profile_id = self.get_task_usable_profiles(vm_id, user_type,
                                                       terminal_type, day)
        # print profile_id
        if not profile_id:
            self.logger.warn("vm_id:%d task id:%d no profile to use!!!", vm_id,
                             task_id)
            return False
        self.logger.info("vm_id:%d task id:%d will run allot profile id:%d",
                         vm_id, task_id, profile_id)
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id,
                                                   task_id)
        self.pc.add_allocated_num(task_group_id)

        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,
        task_group_id,status,start_time,oprcode,ran_minutes,user_type,
        terminal_type,standby_time, timeout, copy_cookie,click_mode,inter_time)
         value(%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d, %d,%d,%d,%d,%d)''' %(
            self.server_id, vm_id, task_id, profile_id, task_group_id,
            -1, oprcode,user_type, terminal_type, randtime, timeout,
            copy_cookie, click_mode, inter_time)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
        self.logger.info(
            "allot profile succ info:server_id:%d,vm_id:%d,task_id:%d,task_type:%d,profile_id:%d",
            self.server_id, vm_id, task_id, task_type, profile_id)
        # print self.server_id,vm_id, task_id, task_type, profile_id
        if task_id != 0:
            self.log_task_profile_latest(vm_id, task_id, task_type, profile_id,
                                         oprcode, -1, user_type, terminal_type)
        self.log_task.log(
            self.server_id,
            vm_id,
            task_id,
            status=-1,
            start_time="CURRENT_TIMESTAMP")
        return True

    def log_task_profile_latest(self, vm_id, task_id, task_type, profile_id,
                                oprcode, status, user_type, terminal_type):
        re_enable_hours = self.get_reenable_day(task_type)
        print self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours
        sql = "insert into vm_task_profile_latest(server_id,vm_id,profile_id,task_type,task_id,start_time,re_enable_hours, oprcode, status, user_type,terminal_type)"\
        " values(%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d, %d, %d, %d, %d) on duplicate key update  task_type=%d,"\
        " start_time=CURRENT_TIMESTAMP, re_enable_hours=%d, oprcode=%d, status=%d"%(
           self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours, oprcode, status, user_type, terminal_type,
              task_type, re_enable_hours, oprcode, status)
        self.logger.debug("latest:%s", sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
        self.log_task_profile(vm_id, task_id, task_type, profile_id,
                              re_enable_hours, oprcode, user_type,
                              terminal_type)

    def log_task_profile(self, vm_id, task_id, task_type, profile_id,
                         re_enable_hours, oprcode, user_type, terminal_type):
        sql = "insert into vm_task_profile_log(server_id,vm_id,profile_id,task_type,task_id,log_time,re_enable_hours,oprcode, user_type,terminal_type)"\
        " values(%d,%d,%d,%d,%d,CURRENT_TIMESTAMP, %d, %d, %d, %d) "%(
            self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours, oprcode, user_type, terminal_type)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)


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
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logger = get_default_logger()
    #pc = ParallelControl(9, dbutil)
    t = TaskProfile_EC(9, dbutil, None, logger)
    t.set_cur_task_profile(1, 410, 410,1)

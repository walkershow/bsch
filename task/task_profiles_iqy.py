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


class TaskProfile_IQY(TaskProfile):

    def get_inited_profiles(self, vm_id, tty, uty, area):
        sql=    "select profile_id from vm_users "\
            "where server_id={0} and vm_id={1} and user_type={2} and " \
            "terminal_type={3} and status=1 and mobile_no is not null " \
            "and area={4}"\
            .format( self.server_id, vm_id, uty, tty, area)
        print sql
        self.logger.info("50277:"+sql)
        
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        print profile_ids
        return profile_ids

    def get_used_profiles(self, vm_id, user_type, terminal_type,
            task_group_id, area):
        '''当天该任务组已使用的用户'''
        '''观看类任务'''
        profiles = []
        sql = '''select cur_profile_id from vm_cur_task where
        server_id=%d and vm_id=%d and
        user_type in(11,13) and terminal_type=%d and area=%s and
        status>3 and UNIX_TIMESTAMP( NOW( ) ) - UNIX_TIMESTAMP( start_time ) < 21600''' % (self.server_id, vm_id,
                                                 terminal_type,
                                                 area)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        return profiles

    def get_usedsucc_profiles(self, vm_id, user_type, terminal_type,
            task_group_id,task_id, area):
        '''当天该任务已成功使用的用户'''
        profiles = []
        sql = '''select cur_profile_id from vm_cur_task where
        server_id=%d and vm_id=%d and
        user_type =11 and terminal_type=%d  and area=%s and
        succ_time is not null and cur_task_id=%d and start_time>current_date''' % (self.server_id, vm_id,
                                                 terminal_type,
                                                 area, task_id)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        return profiles

    def get_using_profiles(self, vm_id, user_type, terminal_type, task_group_id,
            area):
        '''正在使用的用户'''
        profiles = []
        sql = '''select cur_profile_id from vm_cur_task where
        server_id=%d and vm_id=%d and
        user_type in(11,12,13) and terminal_type=%d and area=%s and
        status in(-1,1,2)''' % (self.server_id, vm_id,
                                                terminal_type,
                                                 area)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        return profiles

    def get_task_usable_profiles(self, vm_id, user_type, terminal_type, 
            task_group_id, task_id,area):
        all_profiles = self.get_inited_profiles(vm_id, terminal_type,
                                                user_type, area)
        used_profiles = self.get_used_profiles(vm_id, user_type, terminal_type,
                task_group_id, area)
        print('used', used_profiles)
        usedsucc_profiles = self.get_usedsucc_profiles(vm_id, user_type, terminal_type,
                task_group_id,task_id, area)
        print('usedsucc', usedsucc_profiles)
        using_profiles = self.get_using_profiles(vm_id, user_type, terminal_type,
                task_group_id, area)
        print('useing', using_profiles)
        usable_profiles = list(
            set(all_profiles).difference(set(usedsucc_profiles)))
        usable_profiles = list(
            set(usable_profiles).difference(set(used_profiles)))
        usable_profiles = list(
            set(usable_profiles).difference(set(using_profiles)))
        profile_id = None
        print('useable', usable_profiles)
        if usable_profiles:
            # print('useable', usable_profiles)
            profile_id = random.choice(usable_profiles)
        return profile_id
        


    def get_dialup_ip(self, area):
        sql = '''select ip from vpn_status where area='{0}' and
            vpnstatus=1'''.format(area)
        res = self.db.select_sql(sql)
        if not res or len(res)<1:
            return None
        r = res[0]
        ip = r[0]
        return ip

    def set_cur_task_profile(self, vm_id, task_id, task_group_id, day, area):
        begin_ip = self.get_dialup_ip(area)
        print begin_ip
        if begin_ip is None:
            print "dialup_ip is None"
            return False
        r = self.get_task_type(task_id)
        print r['standby_time'],r['inter_time']
        randtime = 1
        profile_id = self.get_task_usable_profiles(vm_id,r['user_type'], r['terminal_type'],
                 task_group_id, task_id,area)
        if not profile_id:
            return False
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id,
                                                   task_id)

        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,
        task_group_id,status,status2,start_time,oprcode,ran_minutes,user_type,
        terminal_type,standby_time, timeout,
        copy_cookie,click_mode,inter_time,area,begin_ip)
         value(%d,%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d,
                 %d,%d,%d,%d,%d,%d,'%s')''' %(
            self.server_id, vm_id, task_id, profile_id, task_group_id,
            -1, -1,oprcode,r['user_type'], r['terminal_type'],
            randtime,r['timeout'],
            r['copy_cookie'], r['click_mode'], r['inter_time'], area, begin_ip)
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

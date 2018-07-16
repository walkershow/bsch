#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : zero_running_rule.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 13.06.2018 10:54:1528858440
# Last Modified Date: 13.06.2018 11:16:1528859797
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
'''
@Author: coldplay
@Date: 2017-05-12 16:29:06
@Last Modified by:   coldplay
@Last Modified Time: Jan 4, 2018 10:40 AM
'''

import sys
import random
import logging
import logging.config
sys.path.append("..")
import dbutil

logger = None
cardinal = 10000


class ZeroTaskError(Exception):
    pass


class ZeroTask(object):
    '''零跑任务
    '''

    def __init__(self, server_id, db,total_user_num=10000):
        self.db = db
        self.server_id = server_id
        self.total_user_num = total_user_num
        self.area_se = []
        self.splice_num()
        print self.area_se

    def get_task_type(self, task_id):
        sql = "select type from vm_task where id=%d " % (task_id)
        res = self.db.select_sql(sql)
        if not res:
            return None
        return res[0][0]

    def splice_num(self):
        idx = self.total_user_num/cardinal
        for i in range(0,idx):
            s = cardinal*i+1 
            e = cardinal*(i+1)+1
            self.area_se.append((s,e))

            
        
    def get_uninited_profiles(self, vm_id, tty, uty, area):
        #se = self.area_se[area-1]        
        #total_ids = range(se[0], se[1])
        #all country mix dial mode
        if tty == 1:
            total_ids = range(1,5001)
        elif tty == 2:
            total_ids = range(5001,10001)
        else:
            raise Exception, "unkown tty:%d" % (tty)

        sql = '''select profile_id from vm_users where server_id={0}
        and vm_id={1} and user_type={2} and terminal_type={3} and
        area={4}'''.format(self.server_id,
                vm_id,uty,tty, area)

        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return list(set(total_ids)-set(profile_ids))

    def get_inited_profiles(self, vm_id, tty, uty, area):
        sql = "select a.profile_id from vm_users a where a.server_id=%d and a.vm_id=%d "\
            "and user_type=%d and a.terminal_type = %d and a.area=%d "
        sql = sql % (self.server_id, vm_id, uty, tty, area)
        # logger.info(sql)
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids

    def get_used_profiles(self, vm_id, uty):
        profiles = []
        self.reuse_profiles(vm_id)
        sql = "select profile_id from vm_task_profile_latest where server_id=%d and vm_id=%d"\
            " and (user_type=%d or status in(-1,1,2))" % (self.server_id, vm_id, uty)
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        return profiles

    #need task_type,no!!!!
    def reuse_profiles(self, vm_id):
        sql = "delete from vm_task_profile_latest where server_id=%d and vm_id=%d and status in(-2,-1,1,2,4,6,7) "\
            "and TIMESTAMPDIFF(HOUR, start_time, now())>=re_enable_hours "
        sql = sql % (self.server_id, vm_id)
        # logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
        self.reuse_profiles2(vm_id)

    def reuse_profiles2(self, vm_id):
        sql = "delete from vm_task_profile_latest where server_id=%d and vm_id=%d and status in(3,5,8,9)"
        sql = sql % (self.server_id, vm_id)
        # logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)

    def get_usable_profiles(self, vm_id, uty, tty, area):
        '''未初始优先使用
        已初始的过滤掉被其他任务冻结的
        '''
        user_type = uty
        terminal_type = tty

        uninited_profiles = self.get_uninited_profiles(vm_id, terminal_type,
                                                       uty, area)
        # print uninited_profiles
        if uninited_profiles:
            print "...use uninited profile..."
            profile_id = random.choice(uninited_profiles)
            self.log_vm_user(vm_id, profile_id, terminal_type, user_type, area)
            return profile_id

        all_profiles = self.get_inited_profiles(vm_id, terminal_type, uty,area)
        used_profiles = self.get_used_profiles(vm_id, uty)
        # print all_profiles, used_profiles
        usable_profiles = list(
            set(all_profiles).difference(set(used_profiles)))
        # print usable_profiles
        profile_id = None
        if usable_profiles:
            profile_id = random.choice(usable_profiles)
        return profile_id

    def log_vm_user(self, vm_id, profile_id, terminal_type, user_type, area):
        sql = '''insert into vm_users(server_id,vm_id,profile_id,terminal_type,user_type,status,area) 
        values({0},{1},{2},{3},{4},0,{5})''' .format(self.server_id, vm_id, profile_id, 
                                     terminal_type, user_type, area)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise ZeroTaskError, "%s excute error;ret:%d" % (sql, ret)


if __name__ == '__main__':
    dbutil.db_host = "3.3.3.6"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    zt = ZeroTask(1, dbutil, 10000)
    #s,e = zt.area_se
    #print s,e
    profile_id = zt.get_usable_profiles(1, 0,1,5)
    print profile_id

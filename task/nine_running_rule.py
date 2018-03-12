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


class NineTaskError(Exception):
    pass


class NineTask(object):
    '''零跑任务
    '''

    def __init__(self, server_id, db):
        self.db = db
        self.server_id = server_id

    def get_task_type(self, task_id):
        sql = "select type from vm_task where id=%d " % (task_id)
        res = self.db.select_sql(sql)
        if not res:
            return None
        return res[0][0]

    def get_uninited_profiles(self, vm_id, tty, uty):
        sql = " select a.profile_id from vm_profiles a,profiles b "\
            "where server_id=%d and vm_id=%d "\
            "and b.terminal_type = %d and a.profile_id=b.id " % (
                self.server_id, vm_id, tty 
            )
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

    def get_usable_profiles(self, vm_id, uty, tty):
        '''
        已初始的过滤掉被其他任务冻结的
        '''
        user_type = uty
        terminal_type = tty

        uninited_profiles = self.get_uninited_profiles(vm_id, terminal_type,
                                                       uty)

        all_profiles = uninited_profiles
        used_profiles = self.get_used_profiles(vm_id, uty)
        #print all_profiles, used_profiles
        usable_profiles = list(
            set(all_profiles).difference(set(used_profiles)))
        print usable_profiles
        profile_id = None
        if usable_profiles:
            profile_id = random.choice(usable_profiles)
        return profile_id


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    nine = NineTask(21, dbutil)
    profile_id = nine.get_usable_profiles(4, 0,3)
    print profile_id

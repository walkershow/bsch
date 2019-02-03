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


class ZeroTaskError(Exception):
    pass


class ZeroTask(object):
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

    def get_uninited_profiles(self, vm_id, tty, uty, area):
        sql = " select a.profile_id,a.area from vm_profiles a,profiles b "\
            "where a.profile_id not in(select profile_id from vm_users "\
            "where server_id={0} and vm_id={1} and user_type={2} and " \
            "terminal_type={3}) and server_id={0} and vm_id={1} "\
            "and b.terminal_type = {3} and a.profile_id=b.id and a.area={4}"\
            .format( self.server_id, vm_id, uty, tty, area)
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids

    def get_inited_profiles(self, vm_id, tty, uty, area):
        sql = "select a.profile_id from vm_users a where a.server_id=%d and a.vm_id=%d "\
            "and user_type=%d and a.terminal_type = %d and a.area=%s "
        sql = sql % (self.server_id, vm_id, uty, tty, area)
        print (sql)
        #logger.info(sql)
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
        if uninited_profiles:
            profile_id = random.choice(uninited_profiles)
            self.log_vm_user(vm_id, profile_id, terminal_type, user_type, area)
            return profile_id

        all_profiles = self.get_inited_profiles(vm_id, terminal_type, uty,area)
        used_profiles = self.get_used_profiles(vm_id, uty)
        usable_profiles = list(
            set(all_profiles).difference(set(used_profiles)))
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

    def is_ceiling(self, terminal_type, user_type):
        sql = "select 1 from zero_schedule_list where time_to_sec(NOW()) between time_to_sec(start_time) and time_to_sec(end_time) \
                and ran_times<run_times and server_id=%d and terminal_type=%d and user_type=%d"

        sql = sql % (self.server_id, terminal_type, user_type)
        # logger.info(sql)
        res = self.db.select_sql(sql)
        if res:
            return False
        return True

    # def add_ran_times(self, id):
    #     sql ="update zero_schedule_list set ran_times=ran_times+1 where id=%d"%(id)
    #     ret = self.db.execute_sql(sql)
    #     if ret<0:
    #         raise ZeroTaskError,"%s excute error;ret:%d"%(sql, ret)


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    zt = ZeroTask(1, dbutil)
    profile_id = zt.get_usable_profiles(1, 0,1,1)

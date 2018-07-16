#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : user_rolling7.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 21.05.2018 15:28:1526887695
# Last Modified Date: 22.05.2018 17:15:1526980557
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-

# @CreateTime: Jan 16, 2018 11:28 AM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Jan 16, 2018 6:01 PM
# @Description: Modify Here, Please

import sys
import datetime
import logging
import logging.config
from parallel import ParallelControl
from task_profiles import TaskProfile
sys.path.append("..")
import dbutil
import utils
import random


class UserAllotError(Exception):
    pass


class UserAllot(object):
    '''用户分配'''

    def __init__(self, server_id, pc, db, logger, use_cache=True):
        self.db = db
        self.server_id = server_id
        self.logger = logger
        self.task_profile = TaskProfile(server_id, db, pc, logger)
        self.cur_date = datetime.date.today()
        # self.cur_date = None
        self.used_day_set = []
        self.log_used_day_set = []

    def is_new_day(self):
        # print "is new day=========================="
        today = datetime.date.today()
        # print today, self.cur_date
        if today == self.cur_date:
            return False
        self.cur_date = today
        return True

    def gone_days(self, user_type):
        days = 0
        sql = '''SELECT TIMESTAMPDIFF(DAY,min(create_time),now()) days from
        vm_users where user_type={0}'''.format(user_type)
        res = self.db.select_sql(sql)
        if res:
            days = res[0][0]
        return days


    def get_task_type(self, task_id):
        sql = '''select
        type,user_type,terminal_type,standby_time,timeout,copy_cookie,click_mode,
        inter_time from vm_task where id=%d ''' % ( task_id)
        res = self.db.select_sqlwithdict(sql)
        if not res:
            return None
        return res[0]


    def task_rolling_times(self, task_id):
        times = 1
        sql = '''select rolling_times from vm_task where id={0}'''.format(task_id)
        res = self.db.select_sql(sql)
        if res:
            times = int(res[0][0])
        return times


    def get_rolling_time(self, task_id):
        sql = '''select rolling_time from vm_task_rolling7 where task_id={0}
                and done=1'''.format(task_id)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        print res
        print len(res)
        if res and len(res)>0:
            time = res[0][0]
            rolling_times = self.task_rolling_times(task_id)
            print "task_rolling_times:", rolling_times
            print "time:", time
            if rolling_times>time:
                return time+1
            else:
                self.logger.warn("task_id:%d day is out", task_id)
                return -1
                # self.reset_rolling_time_done(task_id)
        return 1

    def set_rolling_time_done(self, task_id, time_seq):
        sql = '''update vm_task_rolling7 set done=1 where task_id={0} and
        rolling_time = {1}'''.format(task_id, time_seq)
        ret = self.db.execute_sql(sql)
        if ret<=0:
            self.logger.error("the sql:%s excute faild ret:%d",sql, ret)

    def get_used_out_server_ids(self, task_id, time_seq):
        sql = '''select used_out_server_ids from vm_task_rolling7 where task_id={0} and
        rolling_time = {1}'''.format(task_id, time_seq)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        if res and len(res)>0:
            server_str = res[0][0]
            if not server_str:
                return []
            server_ids = server_str.split(',')
            server_ids_int = map(int, server_ids)
            return server_ids_int
        return []

            
    def set_used_out_server_id(self, task_id, time_seq, server_str):
        sql = '''update vm_task_rolling7 set used_out_server_ids='{2}' where task_id={0} and
        rolling_time = {1}'''.format(task_id, time_seq, server_str)
        ret = self.db.execute_sql(sql)
        if ret<=0:
            self.logger.error("the sql:%s excute faild ret:%d",sql, ret)

    def reset_rolling_time_done(self, task_id):
        self.logger.info("reset rolling time done")
        sql = '''update vm_task_rolling7 set done=0,used_out_server_ids='',rolling_used_days="" where task_id={0}
        '''.format(task_id)
        ret = self.db.execute_sql(sql)
        if ret<=0:
            self.logger.error("the sql:%s excute faild ret:%d",sql, ret)
    

    def initial_day(self, user_type):
        total_days = self.gone_days(user_type)
        return random.randint(1, total_days)


    def total_days(self, user_type):
        total_days = self.gone_days(user_type)
        l = range(1, total_days)
        return set(l)
    

    def used_days(self, task_id, time_seq):
        days_int = []
        sql = '''select rolling_used_days from vm_task_rolling7 where
        task_id={0} and rolling_time ={1}'''.format(task_id, time_seq)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        if res and len(res)>0:
            days_str = res[0][0]
            days = days_str.split(',')
            print "used days:", days
            if days and days[0]!='' :
                days_int = map(int, days)
                return days_int,int(days[-1])
        return days_int,-1
            

    def exclude_days(self, task_id, time_seq, user_type, last_used_day):
        # self.used_day_set, last_used_day = self.used_days(task_id, time_seq)
        # self.log_used_day_set = self.used_day_set
        # print "used_day_set:", self.used_day_set
        if not self.used_day_set:
            return set()
        min_day = last_used_day - 3
        max_day = last_used_day + 3
        gone_day = self.gone_days(user_type)
        if min_day < 1:
            min_day = 1
        if max_day > gone_day:
            max_day = gone_day
        ex_day_set = set(range(min_day, max_day))
        #print "exclude days:", ex_day_set
        return ex_day_set|set(self.used_day_set)

    def get_random_useable_day(self, task_id, time_seq, user_type,
            last_used_day, server_ids):
        ex_day_set = self.exclude_days(task_id, time_seq, user_type, last_used_day ) 
        #print "before:", ex_day_set
        if ex_day_set is None or len(ex_day_set)<=0:
            day = self.initial_day(user_type)
            self.used_day_set.append(day)
            #print "after:", ex_day_set
            return day
            
        total_day_set = self.total_days(user_type)
        #print "total_day_set:",total_day_set
        #print "ex_day_set:",ex_day_set
        #print self.used_day_set
        useable_days = total_day_set - ex_day_set
        if useable_days:
            #day = random.choice(useable_days)
            day = random.sample(useable_days, 1)[0]
            self.used_day_set.append(day)
            return day
        else:
            left = total_day_set - set(self.log_used_day_set)
            if not left:
                self.set_rolling_time_done(task_id, time_seq)
            server_ids.append(self.server_id)
            server_str = self.server_str_from_set(server_ids)
            self.set_used_out_server_id(task_id, time_seq, server_str)
        return None

    def day_str_from_set(self, days_set):
        days_str = ",".join(str(s) for s in days_set)
        return days_str

    def server_str_from_set(self, days_set):
        s_str = ",".join(str(s) for s in days_set)
        return s_str

    def log_task_usedday(self, task_id, time_seq,task_group_id, day_set):
        days_str = self.day_str_from_set(day_set)
        sql = '''insert into
        vm_task_rolling7(task_id,rolling_time,task_group_id,rolling_used_days)
        values({0},{1},{2}, '{3}') on duplicate key update
        rolling_used_days="{3}" '''.format(task_id, time_seq, task_group_id, days_str)
        self.logger.info("log_task_usedday:%s",sql)
        ret = self.db.execute_sql(sql)
        if ret<=0:
            self.logger.error("the sql:%s excute faild ret:%d",sql, ret)


    def useable_profiles(self, day, task_id, uty, tty):
        sql = '''select count(1) from vm_users where
        TIMESTAMPDIFF(DAY,create_time,now()) = %d and user_type=%d and
        terminal_type = %d''' % (day, uty, tty)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        count = res[0][0]
        if count > 0:
            return True
        return False


    def allot_user(self, vm_id, task_group_id, task_id, area):
        if task_group_id == 0 or task_group_id==9999:
            return self.task_profile.set_cur_task_profile(
                vm_id, task_id, task_group_id, None, area)
        if self.is_new_day():
            print "new day"
            self.reset_rolling_time_done(task_id)

        r = self.get_task_type(task_id)
        if r is None:
            return False
        uty = r['user_type']
        tty = r['terminal_type']
        time_seq = self.get_rolling_time(task_id)
        if time_seq == -1:
            self.logger.warn('''task_group_id:%d 没有可以分配的天数了''',
                    task_group_id)
            return False
        print "rolling time",time_seq

        server_ids = self.get_used_out_server_ids(task_id, time_seq)
        print server_ids
        if server_ids :
            if self.server_id in server_ids:
                logger.info("此服务器:%d已登记使用完", self.server_id)
                return False
        self.used_day_set, last_used_day = self.used_days(task_id, time_seq)
        self.log_used_day_set = self.used_day_set[:]
        print "used_day_set:", self.used_day_set
        
        while True:
            day = self.get_random_useable_day(task_id, time_seq,
                    uty,last_used_day, server_ids)
            print "get random day:", day
            if day:
                if not self.useable_profiles(day, task_id, uty, tty):
                    self.logger.warn(
                        utils.auto_encoding('''该任务组:%d
                            天没有执行过零跑的用户'''), task_group_id)
                    self.used_day_set.append(day)
                    print "append:", self.used_day_set
                    print "append log_used_day_set:",self.log_used_day_set
                    continue
            else:
                return False

            if not self.task_profile.set_cur_task_profile(
                    vm_id, task_id, task_group_id,  day, area):
                self.logger.warn(
                    utils.auto_encoding(
                        "task_group_id:%d 距离现在第%d天无可分配使用的用户"),
                    task_group_id, day)
                self.used_day_set.append(day)
                print "append:", self.used_day_set
                print "append log_used_day_set:",self.log_used_day_set
                continue
            else:
                self.logger.info(
                    utils.auto_encoding(
                        "task_group_id:%d,day:%d 成功分配到执行用户"),
                    task_group_id, day)
                self.log_used_day_set.append(day)
                print "append used_day:", self.used_day_set
                print "log_used_day_set:",self.log_used_day_set

                # self.log_used_day_set = list(set(self.log_used_day_set))
                self.log_task_usedday(task_id, time_seq, task_group_id,
                        self.log_used_day_set)
                return True
        return False


def get_default_logger():
    # import colorer
    logger = logging.getLogger()
    #logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)

    # console logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter("[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def test():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    global logger
    logger = get_default_logger()
    pc = ParallelControl(41, dbutil, logger)
    user_allot = UserAllot(41, pc, dbutil, logger)
    user_allot.allot_user(1, 50060, 50060, 1)
    #for i in range(0, 8):
    #    user_allot.allot_user(1, 10086, 10086)


if __name__ == '__main__':
    test()
    # t3 = threading.Thread(target=test, name="pause_thread")
    # t3.start()
    # t4 = threading.Thread(target=test, name="pause_thread")
    # t4.start()

    # t5 = threading.Thread(target=test, name="pause_thread")
    # t5.start()

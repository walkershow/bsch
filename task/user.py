# -*- coding: utf-8 -*-

# @CreateTime: Jan 16, 2018 11:28 AM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Jan 16, 2018 6:01 PM
# @Description: Modify Here, Please

import sys
import datetime
import os
import time
import logging
import logging.config
sys.path.append("..")
import dbutil
import random
from task_profiles import TaskProfile
from parallel import ParallelControl,ParallelControlException
from zero_running_rule import ZeroTask,ZeroTaskError


class UserAllotError(Exception):
    pass

class UserAllot(object):
    '''用户分配'''


    def __init__(self, server_id, pc, db, logger):
        self.db = db 
        self.server_id = server_id
        self.logger = logger
        self.task_profile = TaskProfile(server_id, db, pc, logger)

    def gone_days(self):
        days = 0
        sql = "SELECT TIMESTAMPDIFF(DAY,min(create_time),now()) days from vm_users;"
        res = self.db.select_sql(sql)
        if res:
            days = res[0][0]
        return days

    def runtimes_one_day(self):
        times = 0
        sql = "select value from vm_sys_dict where `key` = 'runtimes_one_day'"
        res = self.db.select_sql(sql)
        if res:
            times = res[0][0]
        return times


    def runnable_statistic(self, task_group_id, day, times_one_day):
        amount = 0
        runnable = False
        sql_count ="select count(1) from vm_task_runtimes_config where task_group_id=%d "\
        "and day=%d"%(task_group_id, day)
        self.logger.debug(sql_count)
        res = self.db.select_sql(sql_count)
        if res:
            count = res[0][0]
            if count == 0:
                return True, day, amount
        print task_group_id, day, times_one_day
        sql = "select day,users_used_amount from vm_task_runtimes_config where task_group_id=%d "\
        "and users_used_amount<%s and remained=1 order by day limit 1"%(task_group_id, times_one_day)
        self.logger.debug(sql)
        res = self.db.select_sql(sql)
        if res:
            day = res[0][0]
            amount = res[0][1]
            runnable = True
        return runnable, day, amount


    def has_oper_priv(self, task_group_id, day, times_one_day, s_info):
        # sql = '''INSERT INTO vm_task_runtimes_config (
        #         task_group_id,
        #         DAY,
        #         users_used_amount,
        #         remained
        #         )
        #         VALUES
        #         (% d ,% d ,@amount := 1 ,1) ON DUPLICATE KEY UPDATE 
        #         users_used_amount = @amount := users_used_amount + 1;
        #         '''%(task_group_id, day)
        sql = "call return_amount(%d,%d, '%s')"%(task_group_id, day, s_info)
        self.logger.debug(sql)
        res = self.db.select_sql(sql)
        print "res:",res
        if not res:
            return False
        times = res[0][0]
        self.logger.error("runnable statistic[day:%d,times:%d,times_one_day:%s]", day, times, times_one_day)
        if times > int(times_one_day):
            self.logger.warn("task_group_id:%d,day:%d is ran out!!!", task_group_id, day)
            return False
        self.set_s_info(task_group_id, day, s_info)
        return True


    def add_allot_succ_times(self, task_group_id, day):
        sql = "update vm_task_runtimes_config set allot_succ_times=allot_succ_times+1 where task_group_id=%d and day=%d"%(task_group_id, day)
        ret = self.db.execute_sql(sql)
        self.logger.info(sql)
        if ret<0:
            raise UserAllotError,"%s excute error;ret:%d"%(sql, ret)

    def set_remained(self, task_group_id, day):
        sql = "update vm_task_runtimes_config set remained=0 where task_group_id=%d and day=%d"%(task_group_id, day)
        ret = self.db.execute_sql(sql)
        self.logger.info(sql)
        if ret<0:
            raise UserAllotError,"%s excute error;ret:%d"%(sql, ret)

    def set_s_info(self, task_group_id, day ,s_info):
        sql = "update vm_task_runtimes_config set allocated_server=CONCAT_WS(',','%s',allocated_server) where task_group_id=%d and day=%d"
        sql_tmp =sql%(s_info, task_group_id, day)
        self.logger.error(sql_tmp)
        ret = self.db.execute_sql(sql_tmp)
        if ret<0:
            raise UserAllotError,"%s excute error;ret:%d"%(sql_tmp, ret)


    def allot_user(self, vm_id, task_group_id, task_id ):
        if task_group_id == 0:
            return self.task_profile.set_cur_task_profile(vm_id, task_id, task_group_id, None)
        s_info = str(self.server_id) + ":" + str(vm_id)
        g_days = self.gone_days()
        print "gone_days:", g_days
        times_one_day = self.runtimes_one_day()
        day, amount = 0,0
        for d in range(1, g_days+1):
            runnable, day, amount = self.runnable_statistic(task_group_id, d, times_one_day)
            if runnable:
                self.logger.info("距离现在第%d天有可分配使用的用户名额".decode("utf-8").encode("gbk"), day)
                if self.has_oper_priv(task_group_id, day, times_one_day, s_info):
                    if not self.task_profile.set_cur_task_profile(vm_id, task_id, task_group_id, day):
                        self.set_remained(task_group_id, day)
                        continue
                    else:
                        self.add_allot_succ_times(task_group_id, day)
                        return True
                    break
            self.logger.warn("该任务组:%d,第%d天的没有可执行的用户名额".decode("utf-8").encode("gbk"), task_group_id, day)
            return False



import colorer
def get_default_logger():
	logger = logging.getLogger()
	#logger.setLevel(logging.DEBUG)
	logger.setLevel(logging.INFO)

	# console logger
	# ch = logging.StreamHandler()
	# ch.setLevel(logging.ERROR)
	# formatter = logging.Formatter("[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s")
	# ch.setFormatter(formatter)
	# logger.addHandler(ch)
	return logger

def test():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    global logger
    logger = get_default_logger()
    pc = ParallelControl(11, dbutil,logger)
    user_allot = UserAllot(11, pc, dbutil, logger)
    user_allot.allot_user(1, 1, 1)

if __name__ == '__main__':
    import threading
    t2 = threading.Thread(target=test, name="pause_thread")
    t2.start()
    t3 = threading.Thread(target=test, name="pause_thread")
    t3.start()
    t4 = threading.Thread(target=test, name="pause_thread")
    t4.start()

    t5 = threading.Thread(target=test, name="pause_thread")
    t5.start()

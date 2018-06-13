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
from task_profiles import TaskProfile
from parallel import ParallelControl
sys.path.append("..")
import dbutil
import utils


class UserAllotError(Exception):
    pass


class UserAllot(object):
    '''用户分配'''

    def __init__(self, server_id, pc, db, logger ):
        self.db = db
        self.server_id = server_id
        self.logger = logger
        self.task_profile = TaskProfile(server_id, db, pc, logger)
        self.cur_date = datetime.date.today()

    def gone_days(self):
        days = 0
        sql = "SELECT TIMESTAMPDIFF(DAY,min(create_time),now()) days from vm_users;"
        res = self.db.select_sql(sql)
        if res:
            days = res[0][0]
        return days


    def is_task_inited(self, task_group_id):
        sql_count = '''select 1 from vm_user_rolling_pos where
        task_group_id=%d''' % (task_group_id)
        self.logger.debug(sql_count)
        res = self.db.select_sql(sql_count)
        if res and len(res) > 0:
            return True
        return False

    def log_rolling_pos(self, task_group_id, cur_used_day):
        if not self.is_task_inited(task_group_id):
            sql = '''insert into vm_user_rolling_pos(task_group_id, cur_used_day)
            values(%d, %d) ''' % (task_group_id, cur_used_day)
        else:
            sql = '''update vm_user_rolling_pos set cur_used_day=%d where
            task_group_id=%d''' % (cur_used_day, task_group_id)
        ret = self.db.execute_sql(sql)
        if ret >= 0:
            return True
        return False

    def get_cur_used_day(self, task_group_id):
        sql = '''select cur_used_day from vm_user_rolling_pos where
        task_group_id=%d''' % (task_group_id)
        self.logger.debug(sql)
        res = self.db.select_sql(sql)
        if res and len(res) > 0:
            return res[0][0]
        return 1
    
    def reset_rolling_pos(self, task_group_id):
        return self.log_rolling_pos(task_group_id ,1)

    def allot_user(self, vm_id, task_group_id, task_id, area):
        if task_group_id == 0 :
            return self.task_profile.set_cur_task_profile(
                vm_id, task_id, task_group_id, None, area)
        g_days = self.gone_days()
        used_day = self.get_cur_used_day(task_group_id)
        if used_day>= g_days:
            self.reset_rolling_pos(task_group_id)
            day = 1
        else:
            day = used_day+1
        for i in xrange(day, g_days+1):
            if not self.task_profile.set_cur_task_profile(
                    vm_id, task_id, task_group_id, i, area):
                self.logger.warn(
                    utils.auto_encoding("task_group_id:%d 距离现在第%d天无可分配使用的用户"),
                    task_group_id, i)

                continue
            else:
                self.logger.info(
                    utils.auto_encoding("task_group_id:%d,day:%d 成功分配到执行用户"),
                    task_group_id, i)
                self.log_rolling_pos(task_group_id,i)
                return True
        self.log_rolling_pos(task_group_id,g_days)
        return False



def get_default_logger():
    # import colorer
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
    pc = ParallelControl(15, dbutil, logger)
    user_allot = UserAllot(15, pc, dbutil, logger)
    user_allot.allot_user(1, 50000, 50000 ,2)
    #for i in range(0, 8):
    #    user_allot.allot_user(1, 10086, 10086)


if __name__ == '__main__':
    import threading
    t2 = threading.Thread(target=test, name="test")
    t2.start()
    # t3 = threading.Thread(target=test, name="pause_thread")
    # t3.start()
    # t4 = threading.Thread(target=test, name="pause_thread")
    # t4.start()

    # t5 = threading.Thread(target=test, name="pause_thread")
    # t5.start()

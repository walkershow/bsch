#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : user_ec.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 24.04.2018 16:49:1524559760
# Last Modified Date: 24.04.2018 19:25:1524569142
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
from task_profiles_ec import TaskProfile_EC
from user import UserAllot
from parallel import ParallelControl
sys.path.append("..")
import dbutil
import utils



class UserAllot_EC(UserAllot):
    '''用户分配'''

    def __init__(self, server_id, pc, db, logger):
        UserAllot.__init__(self,server_id, pc, db,logger)
        self.task_profile = TaskProfile_EC(server_id, db, pc, logger)


    def allot_user(self, vm_id, task_group_id, task_id):
        day = -1
        if not self.task_profile.set_cur_task_profile(
                vm_id, task_id, task_group_id, day):
            self.logger.warn(
                utils.auto_encoding(
                    "task_group_id:%d 无可分配使用的用户"),
                task_group_id, day)
            return False

        else:
            self.logger.info(
                utils.auto_encoding(
                    "task_group_id:%d,day:%d 成功分配到执行用户"),
                task_group_id, day)
            #self.add_allot_succ_times(task_group_id, day)
            return True


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
    user_allot = UserAllot_EC(15, pc, dbutil, logger)
    user_allot.allot_user(1, 50000, 50000)
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

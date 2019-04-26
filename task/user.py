# -*- coding: utf-8 -*-
# File              : user_ec.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 24.04.2018 16:49:1524559760
# Last Modified Date: 24.04.2018 19:25:1524569142
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-

import sys
import datetime
import logging
import logging.config
from task_profiles_rand import TaskProfile_Rand
from task_profile_rand_noarea import TaskProfile_Rand_NoArea

sys.path.append("..")
import dbutil
import utils


class UserAllot(object):
    """用户分配"""

    def __init__(self, server_id, pc, db, logger):
        self.db = db
        self.server_id = server_id
        self.logger = logger
        self.task_profile = TaskProfile_Rand(server_id, db, pc, logger)
        self.task_profile_noarea = TaskProfile_Rand_NoArea(server_id, db, pc, logger)

    def allot_user(self, vm_id, task_group_id, task_id, area):
        self.logger.info("allot_user rand")
        cookie_type = utils.random_pick([0, 1], [0.1, 0.9])
        if area is None:
            print "no area profile"
            if not self.task_profile_noarea.set_cur_task_profile(
                vm_id, task_id, task_group_id, cookie_type, area
            ):
                self.logger.warn("task_group_id:%d 无可分配使用的用户", task_group_id)
                return False
        else:
            if not self.task_profile.set_cur_task_profile(
                vm_id, task_id, task_group_id, cookie_type, area
            ):
                self.logger.warn("task_group_id:%d 无可分配使用的用户", task_group_id)
                return False

        self.logger.info("task_group_id:%d area:%s 成功分配到执行用户", task_group_id, area)
        return True


def get_default_logger():
    # import colorer
    logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)
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
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    global logger
    logger = get_default_logger()
    # pc = ParallelControl(15, dbutil, logger)
    user_allot = UserAllot(15, None, dbutil, logger)
    user_allot.allot_user(1, 50000, 50000, 99)
    # for i in range(0, 8):
    #    user_allot.allot_user(1, 10086, 10086)


if __name__ == "__main__":
    import threading

    t2 = threading.Thread(target=test, name="test")
    t2.start()
    # t3 = threading.Thread(target=test, name="pause_thread")
    # t3.start()
    # t4 = threading.Thread(target=test, name="pause_thread")
    # t4.start()

    # t5 = threading.Thread(target=test, name="pause_thread")
    # t5.start()

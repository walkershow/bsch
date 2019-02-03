
#-*- coding: utf-8 -*-
"""
@Author: coldplay
@Date: 2017-03-20 10:56:47
@Last Modified by: coldplay
@Last Modified time: 2018-01-31 21:29:21
"""

import vms
import vm_utils
import utils
import time
import threading
from dbutil import DBUtil
import logging
import logging.config
import sys


class CManVM(object):
    def __init__(self, server_id, logger, db_ip, port, db_name, db_user, db_pass):
        self.logger = logger
        self.db = DBUtil(logger, db_ip, port, db_name, db_user, db_pass,
                         'utf8')
        vms.dbutil =self.db
        self.exit_flag = False
        self.server_id = server_id

    def get_reset_vms(self):
        vm_list = []
        sql = "select vm_id from vm_reset_status where server_id={0}".format(self.server_id)
        res = self.db.select_sql(sql)
        if res is None or len(res) < 1:
            return None
        for r in res:
            vm_list.append(r[0])
        return vm_list


    #网络异常,关机重置网络
    def reset_network(self):
        while True:
            try:
                reset_vms = self.get_reset_vms()
                if reset_vms:
                    for vm_id in reset_vms:
                        vmname = "w"+vm_id
                        self.logger.info(
                            "============find network corrupt zombie vm:%s==========",
                            vmname)

                        vm_utils.poweroffVM(vmname)
                        time.sleep(8)
                        vm_utils.set_network_type(vmname, 'null')
                        time.sleep(10)
                        vm_utils.set_network_type(vmname, 'nat')
                        time.sleep(5)
                        ret = vm_utils.startVM(vmname)
                        if ret == 0:
                            self.logger.info("startvm %s succ", vmname)
                            self.log_reset_info(vm_id)
                        else:
                            #失败也更新时间,防止
                            self.logger.info("startvm %s failed", vmname)
                        self.logger.info(
                            "============reset network corrupt zombie vm ok:%s==========",
                            vmname)
            except:
                self.logger.error('[reset_network] exception ', exc_info=True)
            time.sleep(60)

    def log_reset_info(self, vm_id):
        sql = "insert into vm_reset_info(server_id,vm_id,reset_times,update_time,running_date) values(%d,%d,1,current_timestamp,current_date)"\
            " on duplicate key update reset_times=reset_times+1, update_time=current_timestamp,running_date=current_date"
        sql_tmp = sql % (self.server_id, vm_id)
        self.logger.info(sql_tmp)
        ret = self.db.execute_sql(sql_tmp)
        if ret < 0:
            raise Exception, "log_reset_info exception sql:%s,ret:%d" % (
                sql_tmp, ret)



def get_default_self_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # console self.logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    logge = get_default_self_logger()
    vm_utils.g_vManager_path = "D:\CMac\VBox-x64"
    vm_utils.g_vbox_path = "D:\CMac"
    vm_utils.logger = logger
    if len(sys.argv)<2:
        print "args not enough"
        sys.exit(0)
    server_id = sys.argv[1]
    cmv = CManVM(logger, server_id, '192.168.1.21', 3306, 'vm4', 'vm', '123456')
    cmv.reset_network()

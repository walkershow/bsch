
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
        sql = "select vm_id from vm_reset_status where server_id={0} and status=0".format(self.server_id)
        res = self.db.select_sql(sql)
        if res is None or len(res) < 1:
            return None
        for r in res:
            vm_list.append(r[0])
        return vm_list

    def update_reset_status(self, vm_id,status):
        sql = '''update vm_reset_status set status={0} where server_id={1} and vm_id={2}'''.format(status,self.server_id,vm_id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "update_reset_status exception sql:%s,ret:%d" % (
                sql, ret)




    #网络异常,关机重置网络
    def reset_network(self):
        while True:
            try:
                self.reset_zombie_vm()
                reset_vms = self.get_reset_vms()
                print reset_vms
                if reset_vms:
                    time.sleep(60)
                    for vm_id in reset_vms:
                        vmname = "w"+str(vm_id)
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
                            self.update_reset_status(vm_id, 1)
                            self.log_reset_info(vm_id)
                        else:
                            #失败也更新时间,防止
                            self.logger.info("startvm %s failed", vmname)
                        self.logger.info(
                            "============reset network corrupt zombie vm ok:%s==========",
                            vmname)
            except:
                self.logger.error('[reset_network] exception ', exc_info=True)
            time.sleep(3)

    def log_reset_info(self, vm_id):
        sql = "insert into vm_reset_info(server_id,vm_id,reset_times,update_time,running_date) values({0},{1},1,current_timestamp,current_date)"\
            " on duplicate key update reset_times=reset_times+1, update_time=current_timestamp,running_date=current_date"
        sql_tmp = sql.format(self.server_id, vm_id)
        self.logger.info(sql_tmp)
        ret = self.db.execute_sql(sql_tmp)
        if ret < 0:
            raise Exception, "log_reset_info exception sql:%s,ret:%d" % (
                sql_tmp, ret)
                
    def get_restart_vm_interval(self):
        sql = "select `value` from vm_sys_dict where `key`='restart_vm_interval'"
        res = self.db.select_sql(sql)
        if not res:
            return 15
        return res[0][0]

    def get_max_update_time(self):
        sql = "select  a.id,a.vm_id,max(UNIX_TIMESTAMP(a.update_time)) max_ut from vm_cur_task a  where  a.server_id=%d "\
            "and a.status in(-1,1,2) group by  a.vm_id" % (self.server_id)
        print sql
        res = self.db.select_sql(sql)
        vms_time = {}
        if not res:
            return vms_time
        for r in res:
            vms_time[r[1]] = {"id": r[0], "ut": r[2]}
        return vms_time

    def vpn_update_time(self):
        sql = "select UNIX_TIMESTAMP(now()) from dual " 
        res = self.db.select_sql(sql)
        if res:
            update_time = res[0][0]
            return update_time
        return None

    #程序卡死,没更新时间
    def get_zombie_vms(self):
        vms = {}
        interval = int(self.get_restart_vm_interval())
        sql = "select vm_id,vm_name from vm_list where server_id=%d and enabled=0 and status=1 "\
            "and UNIX_TIMESTAMP(current_timestamp)-UNIX_TIMESTAMP(update_time) >%d " % (
                self.server_id, interval)
        # self.logger.info(sql)
        res = self.db.select_sql(sql)
        if not res:
            return
        for r in res:
            vm_id = r[0]
            vmname = r[1]
            vms[vm_id] = vmname
            self.logger.info("get zombie vm:%s", vmname)
        return vms

    # 长时间未曾更新任务时间
    def get_zombie_vms2(self):
        vm_ids = {}
        interval = int(self.get_restart_vm_interval())
        vms_time = self.get_max_update_time()
        if not vms_time:
            self.logger.info(
                "==========there's no task running, and will no lastest running time!!!"
            )
            return vm_ids
        print vms_time
        redial_time = self.vpn_update_time()
        if not redial_time:
            return vm_ids
        for vid, item in vms_time.items():
            id = item['id']
            time = item['ut']
            if time is None:
                continue
            if time >= redial_time:
                continue
            else:
                print "vm_id:", vid, "========stime:", time, "rtime:", redial_time
                if redial_time - time > interval:
                    vm_ids[vid] = id
                    self.logger.info("vm_id:%d,id:%d redial_time-stime>%d",
                                     vid, id, interval)
        return vm_ids

        #发现程序卡死,重启
    def reset_zombie_vm(self):
        try:
            zombie_vms = self.get_zombie_vms()
            if zombie_vms:
                for vm_id, vmname in zombie_vms.items():
                    self.logger.info(
                        "============find network corrupt zombie vm:%s==========",
                        vmname)

                    vm_utils.poweroffVM(vmname)
                    time.sleep(8)

                    ret = vm_utils.startVM(vmname)
                    if ret == 0:
                        self.logger.info("startvm %s succ", vmname)
                        self.update_vm_updatetime(1, vm_id)
                    else:
                        #失败也更新时间,防止
                        self.update_vm_updatetime(0, vm_id)
                        self.logger.info("startvm %s failed", vmname)
                    self.logger.info(
                        "============reset network corrupt zombie vm ok:%s==========",
                        vmname)
        except:
            self.logger.error('[reset_network] exception ', exc_info=True)
            # time.sleep(60)

    def vm_updatetime(self, id):
        sql = "update vm_cur_task set update_time=current_timestamp where id=%d" % (
            id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "update_vm_cur_task_time exception sql:%s,ret:%d" % (
                sql, ret)

    def update_vm_updatetime(self, status, vm_id):
        sql = "update vm_list set status=%d,update_time=current_timestamp where server_id=%d and vm_id=%d" % (
            status, self.server_id, vm_id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "update_vm_time exception sql:%s,ret:%d" % (sql,ret)
             

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
    logger = get_default_self_logger()
    vm_utils.g_vManager_path = "D:\CMac\VBox-x64"
    vm_utils.g_vbox_path = "D:\CMac"
    vm_utils.logger = logger
    if len(sys.argv)<2:
        print "args not enough"
        sys.exit(0)
    server_id = int(sys.argv[1])
    cmv = CManVM( server_id,logger, '192.168.1.21', 3306, 'vm4', 'vm', '123456')
    cmv.reset_network()

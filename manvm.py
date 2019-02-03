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


class CManVM(object):
    def __init__(self, server_id, logger, db_ip, port, db_name, db_user, db_pass):
        self.logger = logger
        self.db = DBUtil(logger, db_ip, port, db_name, db_user, db_pass,
                         'utf8')
        vms.dbutil =self.db
        self.exit_flag = False
        self.server_id = server_id

    def vpn_status(self):
        sql = "select vpnstatus,update_time from vpn_status where serverid=%d " % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res:
            status = res[0][0]
            update_time = res[0][1]
            return status, update_time
        return None, None

    def notify_vpn_2(self):
        sql = "insert into vpn_change2(serverid,want_change2) value(%d,%d) on duplicate key update want_change2=%d,update_time=CURRENT_TIMESTAMP" % (
            self.server_id, 2, 2)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "change2 to 2 failed"

    def notify_vpn_1(self):
        sql = "insert into vpn_change2(serverid,want_change2) value(%d,%d) on duplicate key update want_change2=%d,update_time=CURRENT_TIMESTAMP" % (
            self.server_id, 1, 1)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "change1 to 1 failed"

    def is_vpn_2(self):
        sql = "select 1 from vpn_change2 where want_change2=2 and serverid=%d" % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res is None or len(res) < 1:
            return False
        return True

    def is_vpn_1(self):
        sql = "select 1 from vpn_change2 where want_change2=1 and serverid=%d" % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res is None or len(res) < 1:
            return False
        return True

    def get_reset_network_interval(self):
        sql = "select `value` from vm_sys_dict where `key`='reset_network_interval'"
        res = self.db.select_sql(sql)
        if not res:
            return 15
        return res[0][0]

    def get_restart_vm_interval(self):
        sql = "select `value` from vm_sys_dict where `key`='restart_vm_interval'"
        res = self.db.select_sql(sql)
        if not res:
            return 15
        return res[0][0]

    #程序卡死,没更新时间
    def get_zombie_vms(self):
        vms = {}
        if not self.is_vpn_dialup_3min():
            return vms
        interval = int(self.get_reset_network_interval())
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

    def get_max_update_time(self):
        sql = "select  a.id,a.vm_id,max(UNIX_TIMESTAMP(a.update_time)) max_ut from vm_cur_task a,vm_list b  where  a.vm_id=b.id and b.enabled =0 and a.server_id=%d "\
            "and status in(-1,1,2) group by  a.vm_id" % (self.server_id)
        res = self.db.select_sql(sql)
        vms_time = {}
        if not res:
            return vms_time
        for r in res:
            vms_time[r[1]] = {"id": r[0], "ut": r[2]}
        return vms_time

    def vpn_update_time(self):
        sql = "select UNIX_TIMESTAMP(update_time) from vpn_status where serverid=%d and vpnstatus=1 " % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res:
            update_time = res[0][0]
            return update_time
        return None

    def is_vpn_dialup_3min(self):
        '''已拨号成功3分钟'''
        sql = "select 1 from vpn_status where serverid=%d and vpnstatus=1 and UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(update_time)>180" % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res:
            print "dial up 3 mins!!!"
            return True
        else:
            return False

    # 长时间未曾更新任务时间
    def get_zombie_vms2(self):
        vm_ids = {}
        if not self.is_vpn_dialup_3min():
            return vm_ids
        interval = int(self.get_restart_vm_interval())
        vms_time = self.get_max_update_time()
        if not vms_time:
            self.logger.info(
                "==========there's no task running, and will no lastest running time!!!"
            )
            return vm_ids
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
                    self.logger.info("vm_id:%d,id:%d redial_time-stime>300",
                                     vid, id)
        return vm_ids

    #发现程序卡死,重启
    def reset_zombie_vm(self):
        vm_set = self.get_zombie_vms2()
        if vm_set:
            vms.resume_allvm(self.server_id)
        for vid, id in vm_set.items():
            self.logger.info("============find zombie vm:%s==========", vmname)
            vmname = "w" + str(vid)
            vm_utils.resetVM(vmname)
            self.vm_updatetime(id)
            self.logger.info("============reset zombie vm ok:%s==========",
                             vmname)

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
            raise Exception, "update_vm_time exception sql:%s,ret:%d" % (sql,
                                                                         ret)

    def init_start_vm(self):
        vms.shutdown_allvm(self.server_id)
        time.sleep(10)
        sql = "select vm_name from vm_list where server_id=%d and enabled=0 order by id" % (
            self.server_id)
        res = self.db.select_sql(sql)
        if res is None or len(res) < 1:
            return False
        for r in res:
            vm_utils.startVM(r['vm_name'])
            time.sleep(5)
        return True


    #网络异常,关机重置网络
    def reset_network(self):
        # while True:
        try:
            zombie_vms = self.get_zombie_vms()
            if zombie_vms:
                for vm_id, vmname in zombie_vms.items():
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
                        self.update_vm_updatetime(1, vm_id)
                        self.log_reset_info(vm_id)
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

    def log_reset_info(self, vm_id):
        sql = "insert into vm_reset_info(server_id,vm_id,reset_times,update_time,running_date) values(%d,%d,1,current_timestamp,current_date)"\
            " on duplicate key update reset_times=reset_times+1, update_time=current_timestamp,running_date=current_date"
        sql_tmp = sql % (self.server_id, vm_id)
        self.logger.info(sql_tmp)
        ret = self.db.execute_sql(sql_tmp)
        if ret < 0:
            raise Exception, "log_reset_info exception sql:%s,ret:%d" % (
                sql_tmp, ret)

    def stop(self):
        self.exit_flag = True

    def process(self):
        #args是关键字参数，需要加上名字，写成args=(self,)
        th1 = threading.Thread(target=self.monitoring)
        th1.start()

    def monitoring(self):
        last_status = 1
        self.init_start_vm()
        while True:
            if self.exit_flag:
                break
            try:
                # reset_zombie_vm(self)
                self.reset_network()
                status, update_time = self.vpn_status()

                # self.logger.info("status:%d,last_status:%d", status, last_status)
                while True:
                    #暂停
                    if status == 2 and last_status == 1:
                        vms.pause_allvm(self.server_id)
                        last_status = status
                        self.notify_vpn_2()
                    #恢复
                    elif last_status == 2 and status == 1:
                        vms.resume_allvm(self.server_id)
                        last_status = status
                        self.notify_vpn_1()
                        #g_pc.reset_allocated_num(self)
                    elif last_status == 2 and status == 2:
                        if not self.is_vpn_2():
                            self.logger.info(
                                "status and last_status is 2 but it'nt notify vpn chagne 2"
                            )
                            last_status = 1
                            continue
                    elif last_status == 1 and status == 1:
                        if not self.is_vpn_1():
                            self.logger.info(
                                "status and last_status is 1 but it'nt notify vpn chagne 1"
                            )
                            last_status = 2
                            continue
                    break
                # last_status = status
                time.sleep(1)
            except:
                self.logger.error(
                    '[pasue_reusme_vm] exception on main_loop', exc_info=True)
                time.sleep(3)
        self.logger.info("exit pause_resume_vm")


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
    if len(sys.argv)<2:
        print "args not enough"
        sys.exit(0)
    server_id = sys.argv[1]
    cmv = CManVM(logger, server_id, '192.168.1.21', '3306', 'vm3', 'vm', '123456')
    cmv.init_start_vm()

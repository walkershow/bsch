#-*- coding: utf-8 -*-
"""
@Author: coldplay
@Date: 2017-03-20 10:56:47
@Last Modified by: coldplay
@Last Modified time: 2018-01-31 21:29:21
"""

import utils
import time
import threading
from dbutil import DBUtil
import logging
import logging.config
import sys
import os


class CManVM(object):
    def __init__(self, server_id, logger, db_ip, port, db_name, db_user, db_pass, vmrun_path ,vmpath):
        self.logger = logger
        self.db = DBUtil(logger, db_ip, port, db_name, db_user, db_pass,
                         'utf8')
        self.exit_flag = False
        self.server_id = server_id
        self.vm_path = vmpath
        self.vmrun_path =vmrun_path


    #程序卡死,没更新时间
    def get_zombie_vms(self):
        vms = []
        sql = "select vmid from vpn_status "\
            "where UNIX_TIMESTAMP(now())-UNIX_TIMESTAMP(update_time) >5400 and vm_server_id={0}".format(self.server_id) 
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        if not res:
            return None
        for r in res:
            vm_id = r[0]
            vms.append(vm_id)
        print vms
        return vms
    
    def reset_vm(self, vm_id):
        vmname = "w"+str(vm_id)
        # print self.vmrun_path
        # os.chdir(self.vmrun_path)
        cmd = '''vmrun reset d:/"{0}"/{1}/{2} hard'''.format( self.vm_path, vmname, vmname+".vmx")
        print cmd
        ret = os.system(cmd)
        if ret == 0:
            self.update_vm_updatetime( vm_id)


        #发现程序卡死,重启
    def reset_zombie_vm(self):
        try:
            zombie_vms = self.get_zombie_vms()
            if zombie_vms:
                for vm_id in zombie_vms:
                    self.logger.info(
                        "============find network corrupt zombie vm:%d==========",
                        vm_id)
                    self.reset_vm(vm_id)
                    self.logger.info(
                        "============reset network corrupt zombie vm ok:%d==========",
                        vm_id)
        except:
            self.logger.error('[reset_network] exception ', exc_info=True)
            # time.sleep(60)


    def update_vm_updatetime(self,vm_id):
        sql = "update vpn_status set update_time=current_timestamp where vm_server_id=%d and vmid=%d" % (
            self.server_id, vm_id)
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
    if len(sys.argv)<2:
        print "args not enough"
        sys.exit(0)
    server_id = int(sys.argv[1])
    cmv = CManVM( server_id,logger, '192.168.1.21', 3306, 'vm4', 'vm', '123456',"C:/Program Files (x86)/VMware/VMware Workstation","Virtual Machines")
    cmv.reset_zombie_vm()

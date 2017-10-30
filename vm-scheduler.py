# -*- coding: utf-8 -*-
"""
@Author: coldplay 
@Date: 2017-03-20 10:56:47 
@Last Modified by: coldplay
@Last Modified time: 2017-03-20 11:21:21
"""
import sys
import ConfigParser
import datetime
import optparse
import os
import shutil
import time
import random
import fnmatch
import threading
import signal
import logging
import logging.config
import dbutil
import atexit
import commands
import subprocess
#import job
import threading
from task.taskallot import TaskAllot
from task.taskallot import TaskAllotError
import task.taskallot
from multiprocessing import Process, Queue
import vms,vm_utils
from task_profiles import TaskProfile
from logbytask.logtask import LogTask,LogTaskError
global g_vManager_path
global g_current_dir
global g_reset
global g_cur_running_count
global g_origin_limit
g_serverid = 0
g_rto = 0
g_rto_tmp = 0
g_dsp = ""
g_taskallot = None
g_logtask = None
g_task_profile = None
g_cur_date = datetime.date.today()
g_last_shutdown_time = None
g_start_idx = 0
vm_names = []
vm_ids = []
g_reset_waittime = 120
g_pb = 4

#标识是否是进入暂停状态 0:否 1:是


def getCurrentRunningCount():
    #sql = "select count(1) from vm_group where status = 0 and groupid<=%d"%(g_running_limit)
    sql = "select count(1) from vm_group where (status = 0 or status=9) and serverid=%d"
    res = dbutil.select_sql(sql % (g_serverid))
    count = res[0][0]
    logger.info("current running count:%d", count)
    return count

def get_cur_hour():
    now = datetime.datetime.now()
    return now.hour


def is_new_day():
    global g_cur_date
    today = datetime.date.today()
    if g_cur_date == today:
        return False
    return True

def log_allot_status(server_id, task_id):
    sql = "insert into vm_server_allot_status (server_id, task_id, ran_times,create_time) values(%d,%d,1,CURRENT_DATE) ON DUPLICATE KEY UPDATE \
            ran_times = ran_times+1" % (
        server_id, task_id)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception, "%s excute error;ret:%d" % (sql, ret)

def vpn_status():
    #sql = "select vpnstatus,update_time from vpn_status where serverid=%d and ip is not null and ip!='' "%(g_serverid)
    sql = "select vpnstatus,update_time from vpn_status where serverid=%d "%(g_serverid)
    res = dbutil.select_sql(sql)
    if res:
        status = res[0][0]
        update_time = res[0][1]
        return status, update_time
    return None,None

def vpn_update_time():
    #sql = "select update_time from vpn_status where serverid=%d and vpnstatus=1 and (ip is not null and ip!='') "%(g_serverid)
    sql = "select update_time from vpn_status where serverid=%d and vpnstatus=1 "%(g_serverid)
    res = dbutil.select_sql(sql)
    print "res", res
    if res:
        update_time = res[0][0]
        print update_time
        return  update_time
    return None

def vm_last_succ_time(vm_id):
    sql = "select max(succ_time) from vm_cur_task where server_id=%d and vm_id=%d and status=2"%(g_serverid,vm_id)
    res = dbutil.select_sql(sql)
    if res:
        return res[0][0]
    return '1970-1-1 00:00:00'

def right_to_allot(vm_id):
    succ_time = vm_last_succ_time(vm_id)
    redial_time = vpn_update_time()
    print "right to allot",succ_time, redial_time
    logger.info("last_succ_time:%s, redial_time:%s", succ_time, redial_time)
    rtime ,stime = None,None
    if redial_time:
        rtime = time.strptime(str(redial_time),"%Y-%m-%d %H:%M:%S")
        if not succ_time:
            return True
        else:
            stime = time.strptime(str(succ_time),"%Y-%m-%d %H:%M:%S")

        if stime< rtime:
            return True
        else:
            logger.info("succ_time>=redial_time")
    return False

def notify_vpn_2():
    sql = "insert into vpn_change2(serverid,want_change2) value(%d,%d) on duplicate key update want_change2=%d,update_time=CURRENT_TIMESTAMP"%(g_serverid,2,2)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        raise Exception,"change2 to 2 failed"

def is_vpn_2():
    sql = "select 1 from vpn_change2 where want_change2=2 and serverid=%d"%(g_serverid)
    res = dbutil.select_sql(sql)
    if res is None or len(res)<1:
        return False
    return True
        
def pause_resume_vm():
    last_status = 1
    while True:
        try:
            status, update_time = vpn_status()
            # logger.info("status:%d,last_status:%d", status, last_status)
            #暂停
            if status == 2 and last_status == 1:
                vms.pause_allvm(g_serverid)
                notify_vpn_2()
            #恢复
            elif last_status ==2 and status ==1:
                vms.resume_allvm(g_serverid) 
            elif last_status == 2 and status == 2:
                if not is_vpn_2():
                    logger.info("status and last_status is 2 but it'nt notify vpn chagne 2")
                    last_status = 1
                    continue
            last_status = status
            time.sleep(1)
        except:
            logger.error(
                '[pasue_reusme_vm] exception on main_loop', exc_info=True)
    logger.info("[%s] exit pause_resume_vm", tname)
        



def get_shutdown_time():
    """获取关机时间点 
    """
    sql = "select `value` from vm_sys_dict where `key`='shutdown_time'"
    # logger.info(sql)
    res = dbutil.select_sql(sql)
    # print "is_exp_vm:", res
    if res is None or len(res) < 1:
        return None
    time_list = res[0][0].split(',')
    return time_list


def shutdown_vms_oneday():
    global g_last_shutdown_time
    tlist = get_shutdown_time()
    if not tlist:
        logger.info("shutdown time list is empty")
        return
    print tlist
    cur_hour = get_cur_hour()
    print "cur_hour", cur_hour
    if str(cur_hour) in tlist and cur_hour != g_last_shutdown_time:
        g_last_shutdown_time = cur_hour
        logger.info("time:%d", cur_hour)
        logger.info("=======start to shutdown all vm one day============")
        vms.resume_allvm(g_serverid)
        vms.shutdown_allvm(g_serverid)
        while True:
            time.sleep(30)
            running = False
            for vm in vm_utils.list_allrunningvms():
                running = True
                break
            if running:
                time.sleep(10)
                vms.resume_allvm(g_serverid)
                vms.shutdown_allvm(g_serverid)
                continue
            else:
                break
        logger.info("shutdown complete, sleep %s", g_reset_waittime)
        time.sleep(g_reset_waittime)
        logger.info("=======ent to shutdown all vm one day============")
        logger.info("========start reset all vm =============")
        reset()
        logger.info("=========end reset all vm ================")


def shutdown_by_flag():
    sql = "select 1 from vm_server_poweroff where server_id=%d and poweroff=1" % (
        g_serverid)
    res = dbutil.select_sql(sql)
    if res is None or len(res) < 1:
        return False
    logger.info("=======start to shutdown all vm ============")
    vms.resume_allvm(g_serverid)
    vms.shutdown_allvm(g_serverid)
    while True:
        time.sleep(30)
        running = False
        for vm in vm_utils.list_allrunningvms():
            running = True
            break
        if running:
            time.sleep(10)
            vms.resume_allvm(g_serverid)
            vms.shutdown_allvm(g_serverid)
            continue
        else:
            break

    sql = "update vm_server_poweroff set poweroff=0 where server_id=%d" % (
        g_serverid)
    logger.info("sql:%s", sql)
    ret = dbutil.execute_sql(sql)
    if ret <= 0:
        logger.info("sql:%s======>> exceute ret:%d", sql, ret)
    logger.info("=======shutdown all vm finished============")
    return True

def normal_task_canbe_run():
    sql = "select 1 from vm_cur_task where server_id=%d and vm_id=%d and cur_task_id=0 and status in (-1,1,2)"
    res = dbutil.select_sql(sql)
    # print "is_exp_vm:", res
    if not res:
        return True
    return False 


def main_loop():
    # reset()

    #获取运行状态,请求运行的vm 
    sql = "select a.vm_id,b.vm_name from vm_cur_task a,vm_list b where a.vm_id=b.vm_id and"\
    " a.server_id=%d and a.vm_id=%d and a.status in(1,-1) "
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    vm_names,vm_ids = vms.get_vms(g_serverid)
    # vm_ids = [1]
    # vm_names = ['w1']
    while True:
        try:
            # if shutdown_by_flag():
            #     logger.info("exit the main loop!!!")
            #     os._exit(0)
            #     break
            # shutdown_vms_oneday()
            for i in range(0, len(vm_ids)):
                sqltmp = sql %(g_serverid, vm_ids[i])
                print sqltmp
                res = dbutil.select_sql(sqltmp)
                if not res :
                    sqltmp = sql_count%(g_serverid, vm_ids[i])
                    res = dbutil.select_sql(sqltmp)
                    count = 0
                    if res:
                        count = res[0][0]
                    logger.info("running task vm:%d,count:%d", vm_ids[i], count)
                    if count<g_pb:
                        g_dsp_tmp = g_dsp
                        g_dsp_tmp = g_dsp_tmp % (vm_names[i])
                        task_id,task_group_id = g_taskallot.allot_by_priority(g_dsp_tmp.encode("gbk"))
                        if right_to_allot(vm_ids[i]) or task_id==0:
                            # if task_id == 0 :
                            #     if not normal_task_canbe_run():
                            #         logger.info("there's 0 task is pending or running")
                            #         time.sleep(5)
                            #         continue
                            ret = g_task_profile.set_cur_task_profile(vm_ids[i], task_id, task_group_id)
                            if not ret:
                                logger.info("vm_id:%d,task_id:%d,task_group_id:%d no profile to run", vm_ids[i], task_id, task_group_id)
                            else:
                                g_taskallot.add_ran_time(task_id, task_group_id)
                # else:
                #     if len(res)>1: 
                #         logger.error("vm:%d running task count %d>1 ,the data wrong ,please fix",vm_ids[i], len(res))



            time.sleep(6)

        except:
            logger.error('exception on main_loop', exc_info=True)
            time.sleep(3)
            continue


def reset():
    global g_start_idx,vm_names, vm_ids 
    vms.shutdown_allvm(g_serverid)
    if g_start_idx == 0:
        vm_names, vm_ids= vms.start_vms(g_serverid, 1, 8)
        g_start_idx == 1
    else:
        vm_names, vm_ids= vms.start_vms(g_serverid, 9, 16)
        g_start_idx = 0

def init():
    parser = optparse.OptionParser()
    parser.add_option(
        "-v",
        "--vManagePath",
        dest="vpath",
        default='D:\CMac\VBox-x64',
        help="vboxmanage path, default is d:\cmac\vbox-x64")
 
    parser.add_option(
        "-i",
        "--ip",
        dest="db_ip",
        default="192.168.1.21",
        help="mysql database server IP addrss, default is 192.168.1.21")
    parser.add_option(
        "-n",
        "--name",
        dest="db_name",
        default="vm2",
        help="database name, default is vm")
    parser.add_option(
        "-u",
        "--usrname",
        dest="username",
        default="vm",
        help="database login username, default is vm")
    parser.add_option(
        "-p",
        "--password",
        dest="password",
        default="123456",
        help="database login password, default is 123456")
    parser.add_option(
        "-l",
        "--logconf",
        dest="logconf",
        default="./vm-scheduler.log.conf",
        help="log config file, default is ./scheduler.log.conf")
    parser.add_option(
        "-r",
        "--reset",
        dest="reset",
        default="0",
        help="reset all vm to initail status  default is 0")
    parser.add_option(
        "-b",
        "--paraell browser",
        dest="pb",
        default="4",
        help="paraell running browser  default is 0")
    parser.add_option(
        "-m",
        "--serverid",
        dest="serverid",
        default="0",
        help="the server id,default is  0")
    parser.add_option(
        "-a",
        "--defaul_script_path",
        dest="dsp",
        default="d:\\jb\\%s\\jb\\20170411.jb",
        help="default sciprt to replace,default is d:\jb\vmname\jb\20170411.jb"
    )
    parser.add_option(
        "-w",
        "--want_init",
        dest="winit",
        default="0",
        help="init taskgroup,default is false")
    parser.add_option(
        "-k",
        "--reset_waittime",
        dest="rwt",
        default="120",
        help="reset waiting time,default is 120s")
    (options, args) = parser.parse_args()
    global g_current_dir, g_reset, g_cur_running_count
    global g_serverid, g_rto, g_dsp,g_reset_waittime ,g_pb
    g_cur_running_count = 0
    g_dsp = options.dsp
    g_serverid = int(options.serverid)
    if g_serverid == 0:
        print "serverid is 0,exit"
        sys.exit(1)
    g_reset_waittime = int(options.rwt)
    g_pb= int(options.pb)
    #print options.winit
    g_want_init_task = int(options.winit)
    #print "g_want_init_task", g_want_init_task
    # g_vManager_path='C:\Program Files\Oracle\VirtualBox'
    g_reset = int(options.reset)
    g_current_dir = os.getcwd()
    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info(options)

    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    #启动时的时间跟设置时间不一致,关机开关开启
    cur_hour = get_cur_hour()
    tlist = get_shutdown_time()
    global g_last_shutdown_time
    if str(cur_hour) in tlist:
        g_last_shutdown_time = cur_hour
        print "last_shutdown_time", g_last_shutdown_time
    global g_taskallot,g_logtask,g_task_profile
    task.taskallot.logger = logger
    g_taskallot = TaskAllot(g_want_init_task,g_serverid, dbutil)
    g_logtask = LogTask(dbutil, logger)
    g_task_profile = TaskProfile(g_serverid, dbutil, logger)

    vms.logger = logger
    vms.dbutil = dbutil

    vm_utils.logger = logger
    vm_utils.dbutil = dbutil
    vm_utils.g_vManager_path = options.vpath
    vm_utils.g_current_dir = os.getcwd()
    return True


def main():
    try:
        init()
        if g_reset == 1:
            logger.info("reseting !!!")
            reset()
        tname = "queue_thread"
        t2 = threading.Thread(target=pause_resume_vm, name="pause_thread")
        t2.start()
        main_loop()
    except (KeyboardInterrupt, SystemExit):
        logger.error("exit system,start to shut down all vm...")
        exit(0)
        # vms.shutdown_allvm(g_serverid)
        # logger.info("shutdown all vm done")


if __name__ == "__main__":
    main()

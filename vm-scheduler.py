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
import time
import threading
import signal
import logging
import logging.config
# import colorer  # for colored logger
import dbutil
from task.taskallot import TaskAllot
import task.taskallot
import vms
import vm_utils
import utils
import task.parallel
from task.parallel import ParallelControl
from task.user import UserAllot
from logbytask.logtask import LogTask
from manvm import CManVM
global g_vManager_path
global g_current_dir
global g_reset
global g_origin_limit
from dbutil import DBUtil  # use when multi db connection needed
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
g_vpn_db = None
vm_names = []
vm_ids = []
g_reset_waittime = 120
g_pb = 4
g_pc = None
exit_flag = False
g_user = None
g_manvm = None


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
            ran_times = ran_times+1" % (server_id, task_id)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception, "%s excute error;ret:%d" % (sql, ret)


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


def reset_vms_oneday():
    global g_last_shutdown_time
    tlist = get_shutdown_time()
    if not tlist:
        logger.info("reset time list is empty")
        return
    print tlist
    cur_hour = get_cur_hour()
    print "cur_hour", cur_hour
    if str(cur_hour) in tlist and cur_hour != g_last_shutdown_time:
        g_last_shutdown_time = cur_hour
        logger.info("time:%d", cur_hour)
        logger.info("=======start to reset all vm one day============")
        vms.resume_allvm(g_serverid)
        vms.reset_allvm(g_serverid)
        logger.info("reset complete, sleep %s", g_reset_waittime)
        time.sleep(g_reset_waittime)
        logger.info("=======ent to reset all vm one day============")


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


def can_take_task():
    sql = "select count(1) from vm_task_runtimes_config where "\
        " date(used_out_time) != current_date"
    logger.debug(sql)
    res = dbutil.select_sql(sql)
    if res:
        count = res[0][0]
        if count:
            return True
    return False




def main_loop():
    # reset()

    # 获取运行状态,请求运行的vm
    sql = "select a.vm_id from vm_cur_task a where a.server_id=%d and a.vm_id=%d and a.status in(1,-1,-2) "
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    vm_names, vm_ids = vms.get_vms(g_serverid)
    # vm_ids = [1,2]
    # vm_names = ['w1', 'w2']
    while True:
        try:
            # if shutdown_by_flag():
            #     logger.info("exit the main loop!!!")
            #     os._exit(0)
            #     break
            # reset_vms_oneday()
            for i in range(0, len(vm_ids)):
                sqltmp = sql % (g_serverid, vm_ids[i])
                # print sqltmp
                logger.debug(sqltmp)
                res = dbutil.select_sql(sqltmp)
                if not res:
                    sqltmp = sql_count % (g_serverid, vm_ids[i])
                    res = dbutil.select_sql(sqltmp)
                    count = 0
                    get_default = False
                    if res:
                        count = res[0][0]
                    # logger.warn("running task vm:%d,count:%d", vm_ids[i], count)
                    if count < g_pb:
                        if not can_take_task():
                            logger.warn(
                                utils.auto_encoding("vm:%d\
                                        没有可运行任务名额,只能跑零跑任务"), vm_ids[i])
                            get_default = True
                        logger.error(
                            utils.auto_encoding("==========进入任务分配=========="))
                        # dbutil.select_sql('''select * from vm_priv where id=1 for
                        # update''')
                        ret = g_taskallot.allot_by_priority( vm_ids[i], get_default)
                        if not ret:
                        # if task_id is None:
                            logger.warn(
                                utils.auto_encoding("虚拟机:%d 没有non zero任务可运行"),
                                vm_ids[i])
                            logger.error(
                                utils.auto_encoding(
                                    "==========进入任务分配结束=========="))
                            time.sleep(5)
                            continue
                    else:
                        logger.warn(
                            utils.auto_encoding("虚拟机:%d 当前运行任务数:%d>=4"),
                            vm_ids[i], count)
                else:
                    logger.info(
                        utils.auto_encoding("当前虚拟机:%d,已分配任务或有正在执行的任务"),
                        vm_ids[i])
            logger.error(utils.auto_encoding("==========进入任务分配结束=========="))
            time.sleep(2)

        except:
            logger.error('exception on main_loop', exc_info=True)
            dbutil.rollback()
            time.sleep(3)
        logger.error(utils.auto_encoding("==========进入任务分配结束=========="))


def reset():
    global g_start_idx, vm_names, vm_ids
    vms.shutdown_allvm(g_serverid)
    vm_names, vm_ids = vms.start_vms(g_serverid, 1, 8)


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
        default="vm3",
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
    global g_current_dir, g_reset
    global g_serverid, g_rto, g_dsp, g_reset_waittime, g_pb
    g_dsp = options.dsp
    g_serverid = int(options.serverid)
    if g_serverid == 0:
        print "serverid is 0,exit"
        sys.exit(1)
    g_reset_waittime = int(options.rwt)
    g_pb = int(options.pb)
    g_want_init_task = int(options.winit)
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

    # global g_vpn_db
    # g_vpn_db = DBUtil(logger,options.db_ip,3306, "vpntest", options.username, options.password,'utf8')

    #启动时的时间跟设置时间不一致,关机开关开启
    cur_hour = get_cur_hour()
    tlist = get_shutdown_time()
    global g_last_shutdown_time
    if str(cur_hour) in tlist:
        g_last_shutdown_time = cur_hour
        print "last_shutdown_time", g_last_shutdown_time
    global g_taskallot, g_logtask, g_task_profile, g_pc, g_user
    task.taskallot.logger = logger
    task.parallel.logger = logger
    g_pc = ParallelControl(g_serverid, dbutil, logger)
    g_taskallot = TaskAllot(g_want_init_task, g_serverid, g_pc, dbutil)
    g_logtask = LogTask(dbutil, logger)
    # g_task_profile = TaskProfile(g_serverid, dbutil, g_pc, logger)
    g_user = UserAllot(g_serverid, g_pc, dbutil, logger)
    global g_manvm
    g_manvm = CManVM(g_serverid, logger, options.db_ip, 3306, "vm3",
                     options.username, options.password)

    vms.logger = logger
    vms.dbutil = dbutil

    vm_utils.logger = logger
    vm_utils.dbutil = dbutil
    vm_utils.g_vManager_path = options.vpath
    vm_utils.g_current_dir = os.getcwd()
    return True


def test():
    # dbutil.autocommit(False)
    dbutil.select_sql('''select * from vm_priv where id=1 for
    update''', False)
    dbutil.execute_sql("update vm_priv set priv=0 where id=1", False)


    # dbutil.commit()
def main():
    try:
        init()
        if g_reset == 1:
            logger.info("reseting !!!")
            reset()
        # while True:
        # test()
        # print "hihihi"
        # time.sleep(5)
        # t2 = threading.Thread(target=pause_resume_vm, name="pause_thread")
        # t2.start()
        # g_manvm.process()
        main_loop()
    except (KeyboardInterrupt, SystemExit):
        print("exit system,start to shut down all vm...")
        g_manvm.stop()
        time.sleep(10)
        exit(0)
        # vms.shutdown_allvm(g_serverid)
        # logger.info("shutdown all vm done")


if __name__ == "__main__":
    main()
    # init()
    # g_vpn_db.create_connection()
    # print is_ip_valid('61.145.245.183')
    # print is_ip_valid('115.225.153.71')

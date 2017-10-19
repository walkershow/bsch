# -*- coding: utf-8 -*-
'''
 @Author: coldplay 
 @Date: 2017-04-12 14:29:23 
 @Last Modified by:   coldplay 
 @Last Modified time: 2017-04-12 14:29:23 
'''
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
import subprocess
import ctypes
from logbytask.logtask import LogTask,LogTaskError
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

vm_id = 0
server_id = 0
autocmd = ""
g_rto = 1
last_rec_time = None
isdone = None
g_logtask = None
timeout = None

class LASTINPUTINFO(ctypes.Structure):
    """docstring for LASTINPUTINFO"""
    _fields_ = [
        ("cbSize",ctypes.c_long),
        ("dwTime",ctypes.c_ulong)
    ]

def get_last_input():
    struct_lastinputinfo = LASTINPUTINFO()
    struct_lastinputinfo.cbSize = ctypes.sizeof(LASTINPUTINFO)

    #获得用户最后输入的相关信息
    user32.GetLastInputInfo(ctypes.byref(struct_lastinputinfo))

    #获得机器运行的时间
    run_time = kernel32.GetTickCount()

    elapsed = run_time - struct_lastinputinfo.dwTime

    # print "[*] It's been %d milliseconds since the last input event."%elapsed

    return elapsed

def autoargs():
    global vm_id, server_id
    cur_cwd = os.getcwd()
    dirs = cur_cwd.split('\\')
    vmname = dirs[-2]
    vm_id= vmname[1:]
    server_id= dirs[-3]
    logger.info("get groupid,serverid from cwd:%s,%s",groupid, serverid)


def init():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--ip", dest="db_ip", default="192.168.1.21",
            help="mysql database server IP addrss, default is 192.168.1.235" )
    parser.add_option("-n", "--name", dest="db_name", default="vm2",
            help="database name, default is gamedb" )
    parser.add_option("-u", "--usrname", dest="username", default="vm",
        help="database login username, default is chinau" )
    parser.add_option("-p", "--password" , dest="password", default="123456",
            help="database login password, default is 123" )
    parser.add_option("-l", "--logconf", dest="logconf", default="./wssc.log.conf",
        help="log config file, default is ./wssc.log.conf" )
    parser.add_option("-v", "--vid", dest="vmid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-c", "--cmd", dest="cmd", default="",
        help="running cmd , default is " )
    parser.add_option("-e", "--restarttimeout", dest="rto", default="1",
        help="restart vm when timeout, default is 1 min" )
    parser.add_option("-d", "--isdone", dest="isdone", default="0",
        help="task is done , default is 0" )
    (options, args) = parser.parse_args()
    global vm_id
    vm_id = int(options.vmid)
    global server_id
    server_id = int(options.serverid)
    global autocmd,g_rto,isdone
    autocmd = options.cmd
    g_rto = int(options.rto)
    isdone = int(options.isdone)
    if autocmd=="":
        print "autocmd is empty"
        sys.exit(1)
    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info( options )
    if vm_id == 0 or server_id == 0:
        autoargs()
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    global g_logtask
    g_logtask = LogTask(dbutil, logger)
    make_groupid_file()
    return True


def runcmd():
    p = subprocess.Popen(autocmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def new_task_come():
    sql = "select a.cur_task_id,b.timeout,a.oprcode from vm_cur_task a,vm_task b where a.cur_task_id=b.id and a.status=-1 and a.server_id=%d and a.vm_id=%d"%(int(server_id), int(vm_id))
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None,None,None
    return res[0][0],res[0][1],res[0][2]

def is_task_running():
    sql = "select cur_task_id from vm_cur_task where server_id=%d and vm_id=%d and status=1"%(int(server_id), int(vm_id))
    res = dbutil.select_sql(sql)
    if not res:
        return None
    return res[0][0]

def get_task_id():
    sql = "select cur_task_id from vm_cur_task where server_id=%d and vm_id=%d "%(int(server_id), int(vm_id))
    res = dbutil.select_sql(sql)
    if not res:
        return None
    return res[0][0]
def set_task_status(status):
    ''' 1:running
        2:done
        3:noinput
        4:timeout
    '''
    sql = "update vm_cur_task set status=%d where vm_id=%d and server_id = %d"%(status,int(vm_id), int(server_id))
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        raise Exception,"sql:%s exec failed,ret:%d"%(sql, ret)


def set_task_running():
    sql = "update vm_cur_task set status=1 where vm_id=%d and server_id = %d"%(int(vm_id), int(server_id))
    logger.info(sql)
    ret = dbutil.execute_sql(sql)


def signal_task_noinput():
    sql = "update vm_cur_task set status=3 where vm_id=%d and server_id = %d"%(int(vm_id), int(server_id))
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    # runcmd()

def update_startup_time():
    #更改为虚拟机启动时间,不在此更新(脚本运行时间)
    # sql = "update vm_group set startup_time=CURRENT_TIMESTAMP,running_script=1 where  groupid=%d and serverid = %d"%(int(groupid), int(serverid))
    sql = "update vm_cur_task set start_time=CURRENT_TIMESTAMP where vm_id=%d and server_id = %d"%(int(vm_id), int(server_id))
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

def reset_running_minutes():
    sql = "update vm_cur_task set ran_minutes =0 where server_id=%s and vm_id=%s"%(server_id, vm_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)


def add_one_minutes():
    sql = "update vm_cur_task set ran_minutes=ran_minutes+1 where server_id=%s and vm_id=%s"%(server_id, vm_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

def get_ran_minutes():
    sql = "select ran_minutes from vm_cur_task where server_id=%d and vm_id=%d"%(server_id, vm_id)
    res = dbutil.select_sql(sql)
    if not res:
        return None
    return res[0][0]


def update_running_minutes():
    global last_rec_time
    curr_time = time.time()
    td = (curr_time - last_rec_time)/60
    if td >= 1:
        add_one_minutes()
        last_rec_time = curr_time
        return get_ran_minutes()


def make_groupid_file():
    f = open(r"d:\z.txt",'w')
    f.write("http://192.168.1.21/vm/ad_stat?sid=%s&gid=%s"%(server_id,vm_id))
    f.close()

def run_new_task():
    global task_id,last_rec_time,timeout
    while True:
        task_id, timeout,oprcode = new_task_come()
        print task_id,timeout,oprcode
        if task_id is not None and task_id>=0:
            logger.info("===============get new task,task_id:%d, timeout:%d,oprcode:%d=============",
                        task_id, timeout, oprcode)
            reset_running_minutes()
            last_rec_time = time.time()
            runcmd()
            set_task_status(1)
            g_logtask.log(server_id, vm_id, task_id, status=1, start_time="CURRENT_TIMESTAMP")
            update_startup_time()
            break
        time.sleep(5)

def task_done():
    task_id = get_task_id()
    logger.info("==========the task:%d is done==========",task_id)
    set_task_status(2)
    g_logtask.log(server_id, vm_id, task_id, status="2", end_time="CURRENT_TIMESTAMP")

def main():
    global last_rec_time,task_id
    init()
    try:
        if isdone:
            task_done()
        else:
            #程序启动时,上次任务强制设置为完成
            task_done()
            while True:
                run_new_task()
                while is_task_running():
                    elapsed = get_last_input()
                    print 'no input elasped',elapsed
                    if elapsed>g_rto*60000:
                        set_task_status(3)
                        g_logtask.log(server_id, vm_id, task_id,status=3, end_time="CURRENT_TIMESTAMP")
                        print "sign task noinput"
                        logger.info("long time no input, elasped:%d", elapsed)
                    ran_minutes = update_running_minutes()
                    if ran_minutes>= timeout and timeout!=0:
                        set_task_status(4)
                        g_logtask.log(server_id, vm_id, task_id,status=4, end_time="CURRENT_TIMESTAMP")
                        logger.info("task is timeout,ran_minutes:%d,timeout:%d", ran_minutes, timeout)
                    time.sleep(5)

            time.sleep(5)
    except:
        logger.error('exception on main_loop', exc_info = True)

if __name__ == "__main__":
    main()
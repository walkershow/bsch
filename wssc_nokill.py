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
import getffhwnd.gbh
from getffhwnd.gbh import find_ff_hwnd, close_ff, get_pid,get_p_by_pid,close_ff_win,getwin
import urllib
import requests
from logbytask.logtask import LogTask,LogTaskError
import traceback
import singleton
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

vm_id = 0
server_id = 0
autocmd = ""
g_rto = 1
last_rec_time = time.time()
isdone = None
g_logtask = None
timeout = None
script_path = None

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
    vm_id= int(vmname[1:])
    server_id= int(dirs[-3])
    logger.info("get vmid,serverid from cwd:%s,%s",vm_id, server_id)


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
    parser.add_option("-e", "--restarttimeout", dest="rto", default="1",
        help="restart vm when timeout, default is 1 min" )
    parser.add_option("-d", "--isdone", dest="isdone", default="0",
        help="task is done , default is 0" )
    parser.add_option("-o", "--script_path", dest="script_path", default="",
        help="script path, default script path is..." )
    (options, args) = parser.parse_args()
    global vm_id
    vm_id = int(options.vmid)
    global server_id
    server_id = int(options.serverid)
    global autocmd,g_rto,isdone
    g_rto = int(options.rto)
    isdone = int(options.isdone)
    global script_path 
    script_path = options.script_path
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
    return True

def runcmd(task_id, id):
    script_name = str(task_id) + ".py"
    script = os.path.join(script_path, script_name)
    if not os.path.exists(script):
        logger.error("script:%s not exists", script)
        return False
    #commands = ["python", script, "-i", str(id)]
    commands = ["python", script]
    print commands
    process = subprocess.Popen(commands)
    return True

# def runcmd():
#     p = subprocess.Popen(autocmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def new_task_come():
    sql = "select a.id,a.cur_task_id,b.timeout,a.oprcode,a.ff_hwnd,a.cur_profile_id from vm_cur_task a,vm_task b where a.cur_task_id=b.id and a.status=-1 and a.server_id=%d and a.vm_id=%d"%(int(server_id), int(vm_id))
    # logger.debug(sql)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None,None,None,None,None,None
    return res[0][0],res[0][1],res[0][2],res[0][3],res[0][4],res[0][5]

def is_task_running():
    sql = "select id,cur_task_id,ff_hwnd,ff_pids,cur_profile_id from vm_cur_task "\
    "where server_id=%d and vm_id=%d and status=1 and start_time>current_date order by start_time desc limit 1"%(int(server_id), int(vm_id))
    # logger.debug(sql)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res:
        return None,None,None,None,None
    return res[0][0],res[0][1],res[0][2],res[0][3],res[0][4]

def get_running_task_id():
    sql = "select id,cur_task_id,ff_hwnd,ff_pids,cur_profile_id from vm_cur_task "\
    "where server_id=%d and vm_id=%d and status=1 and start_time>current_date order by start_time desc limit 1"%(int(server_id), int(vm_id))
    res = dbutil.select_sql(sql)
    if not res:
        return None,None,None,None,None
    return res[0][0],res[0][1], res[0][2],res[0][3], res[0][4]

def set_task_status(id,  status):
    ''' 0:nothing to do ,reday 
        1:running
        2:done
        3:noinput
        4:timeout
    '''
    sql = None
    #状态完成时将运行时间重置为0,进入待机状态
    if status == 2 :
        sql = "update vm_cur_task set status=%d,succ_time=CURRENT_TIMESTAMP,update_time=CURRENT_TIMESTAMP,ran_minutes=0 "\
        " where id=%d"%(
            status,id)
    #任务超时时也记录成功时间
    elif status == 6:
        sql = "update vm_cur_task set status=%d,succ_time=CURRENT_TIMESTAMP,update_time=CURRENT_TIMESTAMP"\
        " where id=%d"%(
            status,id)
    else:
        sql = "update vm_cur_task set status=%d,update_time=CURRENT_TIMESTAMP "\
        " where id=%d"%(
            status,id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        raise Exception,"sql:%s exec failed,ret:%d"%(sql, ret)


def set_task_hwnd(id, hwnd, pids):
    if not hwnd:
        return
    pids_str = ','.join(str(p) for p in pids)
    sql = "update vm_cur_task set ff_hwnd=%d,ff_pids='%s',update_time=CURRENT_TIMESTAMP where "\
    "id=%d"%(
        hwnd, pids_str, id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)


def signal_task_noinput(id):
    sql = "update vm_cur_task set status=3,update_time=CURRENT_TIMESTAMP where id=%d"%(
        id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)
    # runcmd()

def update_startup_time(id):
    #更改为虚拟机启动时间,不在此更新(脚本运行时间)
    # sql = "update vm_group set startup_time=CURRENT_TIMESTAMP,running_script=1 where  groupid=%d and serverid = %d"%(int(groupid), int(serverid))
    sql = "update vm_cur_task set start_time=CURRENT_TIMESTAMP where id=%d"%(
        id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

def reset_running_minutes():
    sql = "update vm_cur_task set ran_minutes =0 where id=%d"%(id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

#运行状态任务时间+1
def add_one_minutes():
    sql = "update vm_cur_task set ran_minutes=ran_minutes+1,update_time=CURRENT_TIMESTAMP where server_id=%d and vm_id=%d and status in (1,2)"%(
        server_id, vm_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

def get_task_timeout(task_id):
    sql = "select timeout,standby_time from vm_task where id=%d"%(task_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        return res[0][0],res[0][1]
    return None,None

def get_ran_minutes():
    # sql = "select id,cur_task_id,ran_minutes,ff_hwnd,ff_pids,cur_profile_id,status,should_refresh from vm_cur_task  where server_id=%d and vm_id=%d and status in (1,2)"%(server_id, vm_id)
    sql = "select a.id,a.cur_task_id,a.ran_minutes,a.ff_hwnd,a.ff_pids,a.cur_profile_id,a.status,b.should_refresh from vm_cur_task a,vm_task b  where a.cur_task_id=b.id and a.server_id=%d and a.vm_id=%d and a.status in (1,2)"%(server_id, vm_id)
    res = dbutil.select_sql(sql)
    task_min = {}
    if res:
        for r in res:
            task_min[r[0]] = {'task_id':r[1],'mins':r[2],'hwnd': r[3], 
                            'pid_str':r[4],'profile_id':r[5],'status':r[6],'should_refresh':r[7]}
    print "task_min:", task_min
    return task_min

    
def update_running_minutes():
    global last_rec_time
    curr_time = time.time()
    td = (curr_time - last_rec_time)/60
    if td >= 1:
        add_one_minutes()
        last_rec_time = curr_time
        return get_ran_minutes()


def update_latest_profile_status( task_id, profile_id,status):
    sql = "update vm_task_profile_latest set status=%d where server_id=%d and vm_id=%d and task_id=%d and profile_id=%d"%(
        status, server_id, vm_id,task_id, profile_id
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)
        

def run_new_task():
    global last_rec_time
    # while True:
    id, task_id, timeout,oprcode,last_hwnd,profile_id = new_task_come()
    print id,task_id,timeout,oprcode
    if id is not None:
        logger.info("===============get new task,id:%d,task_id:%d,profile_id:%d timeout:%d,oprcode:%d=============",
                    id, task_id, profile_id, timeout, oprcode)
        #关闭ff
        ret = runcmd(task_id, id)
        if ret:
            status = 1
        else:
            status = 3
        # hwnd = 0
        set_task_status(id, status)
        if status == 1:
            update_latest_profile_status(task_id,profile_id, status)
            update_startup_time(id)
        # set_task_hwnd(id, hwnd, ff_pids)
        g_logtask.log(server_id, vm_id, task_id, status=status, start_time="CURRENT_TIMESTAMP")
        # break
    # time.sleep(5)

def task_done():
    id, task_id, hwnd, oprcode,profile_id = get_running_task_id()
    if id is None:
        return
    logger.info("==========the id:%d, task:%d is done==========",id, task_id)
    set_task_status(id,2)
    update_latest_profile_status(task_id,profile_id, 2)
    # g_logtask.task_done2(oprcode)
    dbutil.close_connection()
    # g_logtask.log(server_id, vm_id, task_id, status="2", end_time="CURRENT_TIMESTAMP")


#待机时间是否已到
def holdon_done():
    task_minutes = update_running_minutes()
    if not task_minutes:
        return
    for t,v in task_minutes.items():
        print "item:",v
        task_id = v['task_id']
        profile_id = v['profile_id']
        m = v['mins'] 
        h = v['hwnd']
        p_str = v['pid_str']
        status = v['status']
        should_refresh = v['should_refresh']
        timeout,standby_time = get_task_timeout(task_id)
        print "timeout:", timeout,"standby:",standby_time,"b_sr:",should_refresh
        logger.info("timeout:%d,standby:%d", timeout, standby_time)
        logger.info("task_id:%d, status:%d,ran_min:%d,b_sr:%d", task_id, status,m, should_refresh)
        # if m>= timeout:
        if status == 1:
            logger.info("checking timeout task:%d,m:%d,standby:%d",task_id, m, standby_time)
            if m>= timeout: 
                status =6
                set_task_status(t,status)
                update_latest_profile_status(task_id,profile_id, status)
                g_logtask.log(server_id, vm_id, task_id, status=status, end_time="CURRENT_TIMESTAMP")
                logger.info("id:%d task:%d is timeout,ran_minutes:%d,timeout:%d", t, task_id, m, timeout)
                # time.sleep(3)
        elif status == 2:
            logger.info("checking standby task:%d,m:%d,standby:%d",task_id, m, standby_time)
            if m>= standby_time :
                #task finish
                status =4
                set_task_status(t,status)
                update_latest_profile_status(task_id,profile_id, status)
                g_logtask.log(server_id, vm_id, task_id, status=status, end_time="CURRENT_TIMESTAMP")
                logger.info("id:%d task:%d is standby,ran_minutes:%d,timeout:%d", t, task_id, m, timeout)
                # time.sleep(3)
        else:
            continue

def main():
    myapp = singleton.singleinstance("wssc.py")
    myapp.run()
    global last_rec_time
    init()
    try:
        if isdone:
            task_done()
        else:
            #程序启动时,上次任务强制设置为完成
            task_done()
            while True:
                try:
                    run_new_task()
                    while True:
                        id, task_id, h,p_str,profile_id=is_task_running()
                        if id is not None:
                            holdon_done()
                            time.sleep(3)
                        else:
                            logger.info("no running task,turn to get new task")
                            break
                    holdon_done()
                    time.sleep(5)
                except Exception ,e:
                    print 'first while traceback.print_exc():'; traceback.print_exc()
                    logger.error('exception on main_loop', exc_info = True)
                    time.sleep(5)
                    continue

    except Exception ,e:
        print 'traceback.print_exc():'; traceback.print_exc()
        # logger.error('exception on main_loop', exc_info = True)

if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except Exception ,e:
            print 'traceback.print_exc():'; traceback.print_exc()
            logger.error('exception on main_loop', exc_info = True)
            time.sleep(5)
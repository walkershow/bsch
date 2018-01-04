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
import signal
import logging
import logging.config
import dbutil
import subprocess
import ctypes
import urllib
import requests
import traceback
import singleton
import psutil
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

vm_id = 0
server_id = 0
script_path = None
# task_script_names = ['bdrank.py', 'sgrank.py', '360rank.py']
task_script_names = ['bdads.py', 'sgads.py', '360ads.py']

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
    parser.add_option("-o", "--script_path", dest="script_path", default="",
        help="script path, default script path is..." )
    (options, args) = parser.parse_args()
    global vm_id
    vm_id = int(options.vmid)
    global server_id
    server_id = int(options.serverid)
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
    return True

def get_max_update_time():
    # sql = "select max(UNIX_TIMESTAMP(a.update_time))  from vm_cur_task a where a.server_id=%d and vm_id=%d group by server_id, vm_id"%(server_id,vm_id)
    sql = "select id,cur_task_id,UNIX_TIMESTAMP(a.update_time) from vm_cur_task a where a.server_id=%d and status in (1,2)"%(server_id)
    # sql = "select  a.id,a.vm_id,max(UNIX_TIMESTAMP(a.update_time)) max_ut from vm_cur_task a where a.server_id=%d "%(server_id)
    res = dbutil.select_sql(sql)
    vms_time = {}
    if not res:
        return vms_time
    for r in res:
        vms_time[r[1]] = {"id":r[0],"ut":r[2]}
    return vms_time

def vpn_update_time():
    sql = "select UNIX_TIMESTAMP(update_time) from vpn_status where serverid=%d and vpnstatus=1 "%(server_id)
    res = dbutil.select_sql(sql)
    if res:
        update_time = res[0][0]
        return  update_time
    return None

def is_vpn_dialup_3min():
    '''已拨号成功3分钟'''
    sql = "select 1 from vpn_status where serverid=%d and vpnstatus=1 and UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(update_time)>180"%(server_id)
    res = dbutil.select_sql(sql)
    if res:
        print "dial up 3 mins!!!"
        return True
    else:
        return False

def kill_proc_by_pid(proc):
    try:
        logger.info("start to kill pid:%d", p)
        if proc:
            if proc.pid == 0:
                logger.info("the ppid is 0,ignore it")
                return
            proc.kill() 
            logger.info("killed pid:%d", proc.pid)
        else:
            logger.info("the process:%d was not exists ", proc.pid)
    except:
        logger.error("kill proc:%d except",proc.pid)

#长时间未曾更新任务时间        
def kill_zombie_proc(interval=140):
    vm_ids = {}
    if not is_vpn_dialup_3min():
        return
    vms_time = get_max_update_time()
    print vms_time
    redial_time = vpn_update_time()
    print redial_time
    if not redial_time:
        return vm_ids
    for tid, item in vms_time.items():
        id = item['id'] 
        time = item['ut']
        if not time:
            continue
        if time >= redial_time:
            continue
        else:
            if redial_time -time >interval:
                logger.info("===========task proc:%d is not acting======", tid)
                logger.info("task_id:%d,id:%d redial_time-stime>140", tid,id)
                set_task_status(8,id)
                script_name = get_scirpt_name(tid)
                cmd_findstr = script_name + " -t %d"%(id)
                proc = find_proc_by_cmdline(cmd_findstr)
                kill_proc_by_pid(proc)
                clean_all_chrome()
                notify_vpn_redial()
                logger.info("===========task proc:%d clean done======", tid)
                
    return vm_ids


def runcmd(task_id, id, task_type):
    if task_type >0:
        script_name = task_script_names[task_type-1]
    else:
        script_name = str(task_id) + ".py"
    script = os.path.join(script_path, script_name)
    # script = get_task_scriptfile(task_id)
    print "script:", script
    if not os.path.exists(script):
        logger.error("script:%s not exists", script)
        return False
    os.chdir(script_path)
    commands = ["python", script,"-t", str(id)]
    process = subprocess.Popen(commands, creationflags=subprocess.CREATE_NEW_CONSOLE)
    return True

def new_task_come():
    sql = "select a.id,a.cur_task_id,a.oprcode,a.cur_profile_id,b.task_type,b.timeout,b.standby_time from vm_cur_task a,vm_task b where a.cur_task_id=b.id and a.status=-1 and a.server_id=%d and a.vm_id=%d"%(int(server_id), int(vm_id))
    # logger.debug(sql)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None,None,None,None,None,None,None
    return res[0][0],res[0][1],res[0][2],res[0][3],res[0][4],res[0][5],res[0][6]

def set_task_status(status,id):
    sql = 'update vm_cur_task set status=%d,update_time=current_timestamp where id=%d'%(status, id)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        raise Exception,"%s excute error;ret:%d"%(sql, ret)
        

def update_latest_profile_status( task_id, profile_id,status):
    sql = "update vm_task_profile_latest set status=%d where server_id=%d and vm_id=%d and task_id=%d and profile_id=%d"%(
        status, server_id, vm_id,task_id, profile_id
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)


def get_task_scriptfile(task_id):
    sql = "select script_file from vm_task where id=%d"%(task_id)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None
    script = res[0][0]
    script = script.decode("utf-8").encode("gbk")
    return script


def get_task_type(task_id):
    sql = "select task_type from vm_task where id=%d"%(task_id)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None
    task_type = res[0][0]
    return task_type

def clean_all_chrome():
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            # print proc.info['cmdline']
            if proc.info["cmdline"] is not None and proc.info["cmdline"].find("chrome.exe") != -1:
                proc.kill()

def find_proc_by_cmdline(cmdline):
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            # print proc.info['cmdline']
            if proc.info["cmdline"] is not None and proc.info["cmdline"].find(cmdline) != -1:
                print cmdline, "is exist"
                return proc
    return None

def notify_vpn_redial():
    sql = 'update vm_isdial_baidu set isdial=1,update_time=current_timestamp where serverid=%d'%(server_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

    
def get_scirpt_name(task_id):
    task_type = get_task_type(task_id)
    script_name = str(task_id)+".py"
    if task_type >0:
        script_name = task_script_names[task_type-1]
    return script_name

def del_timeout_task():
    sql = "select id,cur_task_id from vm_cur_task where status in (1,2) and server_id=%d and vm_id=%d"%(server_id, vm_id)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    bflag =False
    if res:
        print res
        for r in res:
            id = r[0]
            task_id = r[1]
            script_name = get_scirpt_name(task_id)
            cmd_findstr = script_name + " -t %d"%(id)
            proc = find_proc_by_cmdline(cmd_findstr)
            if not proc:
                print cmd_findstr, "is not exist"
                print "task is not running"
                set_task_status(7,id)
                bflag = True

            # for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            #     if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            #         proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            #         # print proc.info['cmdline']
            #         if proc.info["cmdline"] is not None and proc.info["cmdline"].find(cmd_findstr) != -1:
            #             print cmd_findstr, "is exist"
            #             print "id", id ,"===task_id",task_id, " is running"
            #             return
            # set_task_status(7,id)

def main():
    myapp = singleton.singleinstance("wssc.py")
    myapp.run()
    init()
    try:
        while True:
            try:
                while True:
                    id, task_id, oprcode, profile_id, task_type,timeout,standby = new_task_come()
                    if id is not None:
                        print "get task", task_id
                        ret = runcmd(task_id, id, task_type)
                        if ret:
                            set_task_status(1,id)
                        else:
                            update_latest_profile_status(task_id, profile_id, 3)                            
                            set_task_status(3,id)
                    del_timeout_task()
                    kill_zombie_proc()
                    time.sleep(3)
                        
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
        except Exception ,e:
            print 'traceback.print_exc():'; traceback.print_exc()
            logger.error('exception on main_loop', exc_info = True)
            time.sleep(5)
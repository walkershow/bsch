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
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

vm_id = 0
server_id = 0
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


def runcmd(task_id, id):
    script_name = str(task_id) + ".py"
    script = os.path.join(script_path, script_name)
    # script = get_task_scriptfile(task_id)
    print "script:", script
    if not os.path.exists(script):
        logger.error("script:%s not exists", script)
        return False
    os.chdir(script_path)
    commands = ["python", script,"-t", str(id)]
    process = subprocess.Popen(commands)
    return True

def new_task_come():
    sql = "select a.id,a.cur_task_id,a.oprcode,a.cur_profile_id from vm_cur_task a,vm_task b where a.cur_task_id=b.id and a.status=-1 and a.server_id=%d and a.vm_id=%d"%(int(server_id), int(vm_id))
    # logger.debug(sql)
    logger.info(sql)
    res =dbutil.select_sql(sql)
    if not res:
        return None,None,None,None
    return res[0][0],res[0][1],res[0][2],res[0][3]

def set_task_status(status,id):
    sql = 'update vm_cur_task set status=%d where id=%d'%(status, id)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        raise Exception,"%s excute error;ret:%d"%(sql, ret)
        

def del_unuse_latest_profile_status( task_id, profile_id):
    sql = "delete vm_task_profile_latest where server_id=%d and vm_id=%d and task_id=%d and profile_id=%d"%(
         server_id, vm_id,task_id, profile_id
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)

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
def main():
    myapp = singleton.singleinstance("wssc.py")
    myapp.run()
    init()
    try:
        while True:
            try:
                while True:
                    id, task_id, oprcode, profile_id = new_task_come()
                    if id is not None:
                        print "get task", task_id
                        ret = runcmd(task_id, id)
                        if ret:
                            set_task_status(1,id)
                        else:
                            update_latest_profile_status(task_id, profile_id, 3)                            
                            set_task_status(3,id)


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
# -*- coding:utf-8 -*-
#@Author: coldplay 
#@Date: 2017-08-15 14:43:18 
#@Last Modified by:   coldplay 
#@Last Modified time: 2017-08-15 14:43:18 
#
import sys
import ConfigParser
import optparse
import time
import os
import threading
import signal
import logging
import logging.config
import subprocess
import psutil
import subprocess
import string
sys.path.append("..")
import dbutil

groupid = 0
serverid = 0

def init():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--ip", dest="db_ip", default="192.168.1.21",
            help="mysql database server IP addrss, default is 192.168.1.235" ) 
    parser.add_option("-n", "--name", dest="db_name", default="vm3",
            help="database name, default is gamedb" ) 
    parser.add_option("-u", "--usrname", dest="username", default="vm",
        help="database login username, default is chinau" )
    parser.add_option("-p", "--password" , dest="password", default="123456",
            help="database login password, default is 123" )
    parser.add_option("-l", "--logconf", dest="logconf", default="./vm-onekeytask.log.conf",
        help="log config file, default is ./vm_onekeytask.log.conf" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    (options, args) = parser.parse_args()
    global serverid
    serverid = options.serverid

    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info( options )
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    return True

def restart_process():
    process_cmd_list = get_restart_process_path_list()
    for process_cmd in process_cmd_list:
        process = process_cmd.split(" ")[0]
        (filepath,tempfilename) = os.path.split(process)
        try:
            SW_MINIMIZE = 6
            info = subprocess.STARTUPINFO()
            info.dwFlags = subprocess.STARTF_USESHOWWINDOW
            info.wShowWindow = SW_MINIMIZE

            p = subprocess.Popen(process_cmd, cwd = filepath, startupinfo=info, creationflags = subprocess.CREATE_NEW_CONSOLE)
        except:
            logger.error("fail to create process")

def find_kill_process(process_name_list):
    '''
    1.先查找进程id, 名字, 命令行
    2.组成命令行
    3.查看匹配，并杀死
    '''
    process_list = []
    kill_process_id = []
    special_process = "vm_update.py" # self.process_name

    # 命令行
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
        else:
            proc.info["cmdline"] = None
        process_list.append(proc)


    #查看后缀，py, bat 需要观察命令行
    for process_name in process_name_list:
        (filepath,tempfilename) = os.path.split(process_name)
        (filename,extension) = os.path.splitext(tempfilename)
        # special situation
        if extension == ".py" :
            for proc in process_list:
                if proc.info["cmdline"] is not None and proc.info["cmdline"].find(process_name) != -1:
                    kill_process_id.append(proc.info["pid"])
        # kill .bat self and its children
        elif extension == ".bat":
            for proc in process_list:
                if proc.info["cmdline"] is not None and proc.info["cmdline"].find(process_name) != -1:
                    kill_process_id.append(proc.info["pid"])
                    for child in psutil.Process(proc.info["pid"]).children():
                        kill_process_id.append(child.pid)
        else:
            for proc in process_list:
                if proc.info["name"].find(process_name) != -1:
                    kill_process_id.append(proc.info["pid"])

    return kill_process_id

def format_cmd(cmd_pattern, ip, server_id):

    placeholders = {
        'ip': ip,
        'sid': server_id,
    }

    template = string.Template(cmd_pattern)
    cmd = template.substitute(placeholders)  # locals()
    return cmd 


def get_restart_process_path_list():
    ret_list = []
    sql = "select process_path from restart_process_computer"
    res = dbutil.select_sqlwithdict(sql)
    for v in res:
        real_path = format_cmd(v["process_path"], "", serverid)
        ret_list.append(real_path)
    return ret_list

def restart_self():
    python = sys.executable
    os.execl(python, python, * sys.argv)
    sys.exit(0)

def restart_process():
    process_cmd_list = get_restart_process_path_list()
    for process_cmd in process_cmd_list:
        process = process_cmd.split(" ")[0]
        (filepath,tempfilename) = os.path.split(process)
        try:
            SW_MINIMIZE = 6
            info = subprocess.STARTUPINFO()
            info.dwFlags = subprocess.STARTF_USESHOWWINDOW
            info.wShowWindow = SW_MINIMIZE
            logger.error(process_cmd)
            logger.error(filepath)
            p = subprocess.Popen(process_cmd, cwd = filepath, startupinfo=info, creationflags = subprocess.CREATE_NEW_CONSOLE)
        except:
            logger.error("fail to create process")


def get_kill_process_name_list():
    ret_list = []
    sql = "select process_name from kill_process_computer"
    res = dbutil.select_sqlwithdict(sql)
    for v in res:
        ret_list.append(v["process_name"])
    return ret_list

def kill_process():
    process_list = get_kill_process_name_list()
    process_id_list = find_kill_process(process_list)
    for pid in process_id_list:
        p = psutil.Process(pid)
        if p is None:
            continue
        p.terminate()

def main():
    init()
    sql = "select a.cmd,b.id from vm_cmd a,vm_onekey_task b where a.id=b.cmd_id and b.server_id=%s and b.status=0 order by b.sort_order"
    sql_up = "update vm_onekey_task set status=%d where id=%d"
    while True:
        try:
            sql_sel =  sql%(serverid)
            res= dbutil.select_sql(sql_sel)
            logger.info("select sql:%s", sql_sel)
            if len(res) != 0:
               kill_process() 
            for r in res:
                cmd = r[0].encode("gbk")
                id = r[1]
                ret = os.system(cmd)
                logger.info("cmd:%s run ret:%d ", r[0], ret)
                if ret==0:
                    sql_upstatus = sql_up%(1, id)
                else:
                    sql_upstatus = sql_up%(-1, id)
                ret = dbutil.execute_sql(sql_upstatus)
                logger.info("update sql:%s, ret:%d", sql_upstatus, ret) 
                time.sleep(5)
            if len(res) != 0:
                restart_process()
                restart_self()
        except:
            logger.error(
                'exception on reset ', exc_info=True)
            time.sleep(5)
            continue

        time.sleep(10)
        print("still running!!!")


if __name__ == "__main__":
    main()

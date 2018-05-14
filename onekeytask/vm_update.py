# -*- encoding: utf-8 -*-
#@Author: coldplay 
#@Date: 2017-08-15 14:43:18 
#@Last Modified by:   coldplay 
#@Last Modified time: 2017-08-15 14:43:18 
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
sys.path.append("..")
import dbutil # I used thread in this program, so maybe fuck up here, just let it go
import string
from win32com.client import GetObject
import psutil
import singleton

vm_id= 0
server_id = 0
g_work_path = sys.path[0]

  


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
    parser.add_option("-n", "--name", dest="db_name", default="vm3",
            help="database name, default is gamedb" ) 
    parser.add_option("-u", "--usrname", dest="username", default="vm",
        help="database login username, default is chinau" )
    parser.add_option("-p", "--password" , dest="password", default="123456",
            help="database login password, default is 123" )
    parser.add_option("-l", "--logconf", dest="logconf", default="./vm-update.log.conf",
        help="log config file, default is ./vm_onekeytask.log.conf" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-v", "--vmid", dest="vmid", default="0",
        help="log config file, default is 0" )
    (options, args) = parser.parse_args()
    global server_id,vm_id



    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info( options )
    server_id = options.serverid
    vm_id = options.vmid
    if server_id == '0' or vm_id == '0':
        autoargs()
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    return True

def format_cmd(cmd_pattern, ip, server_id, vm_id):

    placeholders = {
        'ip': ip,
        'sid': server_id,
        'vid': vm_id,
    }

    template = string.Template(cmd_pattern)
    cmd = template.substitute(placeholders)  # locals()
    return cmd 

def get_server_ip():
    sql = "select ip from vm_server_list where id=%s"%(server_id)
    res = dbutil.select_sql(sql)
    if res:
        return res[0][0]
    return None

def find_kill_process(process_name_list):
    '''
    1.先查找进程id, 名字, 命令行
    2.组成命令行
    3.查看匹配，并杀死
    '''
    process_list = all_process_list()
    kill_process_id = []
    special_process = "vm_update.py" # self.process_name

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


def restart_processes():
    process_cmd_list = get_restart_process_path_list()
    for process_cmd in process_cmd_list:
        restart_process(process_cmd)

def restart_process(process_cmd):
    process = process_cmd.split(" ")[0]
    (filepath,tempfilename) = os.path.split(process)
    try:
        SW_MINIMIZE = 6
        info = subprocess.STARTUPINFO()
        info.dwFlags = subprocess.STARTF_USESHOWWINDOW
        info.wShowWindow = SW_MINIMIZE
        logger.error(process_cmd)
        p = subprocess.Popen(process_cmd, cwd = filepath, startupinfo=info, creationflags = subprocess.CREATE_NEW_CONSOLE)
    except:
        logger.error("fail to create process")

def kill_process():
    process_list = get_kill_process_name_list()
    process_id_list = find_kill_process(process_list)
    for pid in process_id_list:
        p = psutil.Process(pid)
        if p is None:
            continue
        p.terminate()

def get_kill_process_name_list():
    ret_list = []
    sql = "select process_name from kill_process"
    res = dbutil.select_sqlwithdict(sql)
    for v in res:
        ret_list.append(v["process_name"])
    return ret_list

def get_restart_process_path_list():
    ret_list = []
    sql = "select process_path from restart_process"
    res = dbutil.select_sqlwithdict(sql)
    for v in res:
        real_path = format_cmd(v["process_path"], "", server_id , vm_id)
        ret_list.append(real_path)
    return ret_list

def restart_self():
    python = sys.executable
    os.execl(python, python, * sys.argv)
    sys.exit(0)

def get_update_time(db):
    sql = "select value from vm_sys_dict where `key` = 'report_vm_status_interval'"
    res = db.select_sql(sql, 'DictCursor')
    if not res or len(res) == 0:
        logger.error("select key time failed")
        return 300
    else:
        return res[0]["value"]

def update_status_and_time(db):
    sql = "update vm_list set `status` = 1, update_time = CURRENT_TIMESTAMP where server_id = %s and vm_id = %s" %(server_id, vm_id)
    res = db.execute_sql(sql)
    if res == 0:
        logger.error("update zero row")
    return res

def update_thread_func():
    while True:
        try:
            db = dbutil.DBUtil(logger, '192.168.1.21', 3306, 'vm3', 'vm', '123456', 'utf8')
            while True:
                update_time_internal = int(get_update_time(db))
                res = update_status_and_time(db)
                if res == -2:
                    continue
                time.sleep(update_time_internal)
                logger.error("on fuck up update")
        except Exception as e:
            logger.error(e)

def all_process_list():
    process_list = []
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
        else:
            proc.info["cmdline"] = None
        process_list.append(proc)
    return process_list

def check_wssc_exist():
    process_list = all_process_list()
    found = False
    for proc in process_list:
        if proc.info["cmdline"] is not None and proc.info["cmdline"].find("wssc") != -1:
            found = True
    if found == False:
        real_path = format_cmd(r"Z:\${sid}\w${vid}\script\wssc.bat", "", server_id , vm_id)
        restart_process(real_path)

def main():
    myapp = singleton.singleinstance("vm_update.py")
    myapp.run()

    print("start")
    init()

    #Create a thread here to update status and time
    t = threading.Thread(target = update_thread_func)
    t.start()

    sql = "select a.cmd,b.id, b.cmd_id from vm_cmd a,vm_update_task b where a.id=b.cmd_id and b.server_id=%s and vm_id=%s and b.status=0 order by b.sort_order"
    sql_up = "update vm_update_task set status=%d where id=%d"

    while True:
        try:
            sql_sel =  sql%(server_id, vm_id)
            res= dbutil.select_sqlwithdict(sql_sel)
            logger.info("select sql:%s", sql_sel)
            if len(res) != 0:
                kill_process()

            for r in res:
                count = 0
                # check update
                while True:
                    computer_sql = "select computer_cmd_id from cmd_mapping where vm_cmd_id = %d" %(r["cmd_id"])
                    computer_res = dbutil.select_sqlwithdict(computer_sql)

                    if len(computer_res) == 0:
                        break
                    computer_cmd_id = computer_res[0]["computer_cmd_id"]
                    has_update_sql = "select status from vm_onekey_task where server_id = %s and cmd_id = %s" %(server_id, computer_cmd_id)
                    has_update_res = dbutil.select_sqlwithdict(has_update_sql)
                    if has_update_res[0]["status"] == 1:
                        if count >= 5:
                            kill_process()
                        break
                    else:
                        print("wait for transport")
                        count = count + 1
                        if count == 5:
                            restart_processes()
                        time.sleep(5)
                server_ip = get_server_ip()
                if server_id is None:
                    logger.info("can not find the server ip:%s", server_id)
                    break
                #cmd = r[0].encode("gbk")
                cmd = r["cmd"].decode("utf8").encode("gbk")
                cmd = format_cmd(cmd, server_ip, server_id , vm_id)
                id = r["id"]
                if cmd == "vm_update.py":
                    restart_self(sql_up %(1, id))
                ret = os.system(cmd)
                    
                # ret = 0
                logger.info("cmd:%s run ret:%d ", cmd, ret)
                if ret==0:
                    sql_upstatus = sql_up%(1, id)
                else:
                    sql_upstatus = sql_up%(-1, id)
                ret = dbutil.execute_sql(sql_upstatus)
                logger.info("update sql:%s, ret:%d", sql_upstatus, ret)
                time.sleep(5)

            if len(res) != 0:
                #reboot()
                restart_processes()
                restart_self()
            check_wssc_exist()
        except Exception:
            logger.error(
                'exception on reset ', exc_info=True)
            time.sleep(5)
            continue

        time.sleep(10)
        print("still running!!!")


if __name__ == "__main__":
    main()

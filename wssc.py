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
    getffhwnd.gbh.logger = logger
    return True


def runcmd():
    p = subprocess.Popen(autocmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

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

def get_ff_hwnds_pids_on_vm():
    sql = "select ff_hwnd,ff_pids from vm_cur_task where server_id=%d and vm_id=%d and status not in(3,4,5,6) "%(server_id, vm_id)
    res = dbutil.select_sql(sql)
    hwnds = []
    pids = []
    if res:
        for r in res:
            hwnds.append(r[0])
            pid_str = r[1]
            if pid_str:
                ps = pid_str.split(',')
                for p in ps:
                    s = p.strip()
                    if not s:
                        continue
                    pids.append(int(s))
    return hwnds,pids
    
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
    f.write("http://192.168.1.21/vm/ad_stat2?sid=%s&gid=%s"%(server_id,vm_id))
    f.close()

def update_latest_profile_status( task_id, profile_id,status):
    sql = "update vm_task_profile_latest set status=%d where server_id=%d and vm_id=%d and task_id=%d and profile_id=%d"%(
        status, server_id, vm_id,task_id, profile_id
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret<0:
        logger.error("sql:%s, ret:%d", sql, ret)
        
#start new task, kill known ff process
def kill_unkown_ff():
        hwnds,pids = get_ff_hwnds_pids_on_vm()
        logger.info("find known ff hwnd in 5 secs...")
        hwnd = find_ff_hwnd(hwnds,5)
        status = 0
        if hwnd:
            logger.info("********************")
            logger.info("find unknown ff hwnd in 30 secs over, found the hwnd:%d", hwnd)
            logger.info("********************")
        ff_pids = get_pid("firefox.exe",pids)
        if ff_pids:
            pids_str = ','.join(str(p) for p in pids)
            ff_pids_str = ','.join(str(p) for p in ff_pids)
            logger.info("********************")
            logger.info("find known ff pids:%s", pids_str)
            logger.info("find unkown ff pids:%s", ff_pids_str)
            logger.info("********************")
        for pid in ff_pids:
            close_kill_ff2(hwnd, pid) 


def run_new_task():
    global last_rec_time
    # while True:
    id, task_id, timeout,oprcode,last_hwnd,profile_id = new_task_come()
    print id,task_id,timeout,oprcode
    if id is not None:
        logger.info("===============get new task,id:%d,task_id:%d,profile_id:%d timeout:%d,oprcode:%d=============",
                    id, task_id, profile_id, timeout, oprcode)
        #关闭ff
        kill_unkown_ff() 
        last_rec_time = time.time()
        open_ff(id, task_id, profile_id)
        #查找ff
        hwnds,pids = get_ff_hwnds_pids_on_vm()
        logger.info("find ff hwnd in 30 secs...")
        hwnd = find_ff_hwnd(hwnds)
        status = 0
        if not hwnd:
            logger.info("find ff hwnd in 30 secs over, not found the hwnd")
            status = 5
        else:
            logger.info("find ff hwnd in 30 secs over, found the hwnd:%d", hwnd)
            status = 1 
        pids_str = ','.join(str(p) for p in pids)
        logger.info('exists pids:%s', pids_str)
        ff_pids = get_pid("firefox.exe",pids)
        if not ff_pids: 
            logger.info("find ff pid in 30 secs over, not found the pid")
            status = 5
        else:
            for pid in ff_pids:
                logger.info("find ff pid in 30 secs over, found the pid:%d", pid)
                pids_str = ','.join(str(p) for p in pids)
                logger.info('exists pids222:%s', pids_str) 
            if hwnd:
                status = 1 
            else:
                status =5
        if status == 5:
            logger.info("======status is 5, still to close ff and kill process=====")
            for pid in ff_pids:
                close_kill_ff2(hwnd, pid) 
        elif status == 1:
            logger.info("find ff win,then start zhixing.exe")
            runcmd()
        set_task_status(id, status)
        update_latest_profile_status(task_id,profile_id, status)
        set_task_hwnd(id, hwnd, ff_pids)
        g_logtask.log(server_id, vm_id, task_id, status=status, start_time="CURRENT_TIMESTAMP")
        update_startup_time(id)
        # break
    # time.sleep(5)

def task_done():
    id, task_id, hwnd, oprcode,profile_id = get_running_task_id()
    if id is None:
        return
    logger.info("==========the id:%d, task:%d is done==========",id, task_id)
    kill_zhixing()
    set_task_status(id,2)
    update_latest_profile_status(task_id,profile_id, 2)
    g_logtask.task_done2(oprcode)
    dbutil.close_connection()
    # g_logtask.log(server_id, vm_id, task_id, status="2", end_time="CURRENT_TIMESTAMP")

def open_ff(id ,task_id, profile_id):
    i = 0 
    while True:
        try:
            i = i +1
            if i >5 :
                logger.info("open_ff times is out:%d",i)
                break
            url="http://192.168.1.21/vm/getprofile3?serverid=%d&vmid=%d"%(server_id, vm_id) 
            # print url
            return_data = requests.get(url)
            # print return_data
            profile = return_data.text
            logger.info("open ff link:%s", profile)
            # print profile
            if profile.strip() == 'nil':
                print 'nil...'
                logger.error("ff link is nil, set status to 5")
                status = 5
                set_task_status(id, status)
                update_latest_profile_status(task_id,profile_id, status)
                g_logtask.log(server_id, vm_id, task_id, status=status, start_time="CURRENT_TIMESTAMP")
                update_startup_time(id)
                break

            cmd = "start "+ profile
            logger.info("%s", cmd)
            # print cmd
            # os.system("d:\\y.exe")
            os.system(cmd)
        except:
            logger.error("http request error")
            continue
        break

def close_kill_ff2(h,p):
    hwnds,pids = get_ff_hwnds_pids_on_vm()
    if h:
        close_ff(int(h), hwnds)
        time.sleep(5)
    try:
        proc = get_p_by_pid(p)
        logger.info("start to kill pid:%d", p)

    # print proc
        if proc:
            if proc.pid == 0:
                logger.info("the ppid is 0,ignore it")
                return
            proc.kill() 
            logger.info("killed pid:%d", proc.pid)
        else:
            logger.info("the process:%d is killed when sending close msg to ff", p)
    except:
        logger.error("close and kill ff --- process id:%d no longer exist",p)

def close_kill_ff(h,pstr,b_sr):
    hwnds,pids = get_ff_hwnds_pids_on_vm()
    if h:
        close_ff(int(h), hwnds, b_sr)
        time.sleep(5)
    if pstr:
        ps = pstr.split(',')
        for s in ps:
            t = s.strip()
            if not t:
                continue
            p = int(t)
            try:
                proc = get_p_by_pid(p)
                logger.info("start to kill pid:%d", p)
            # print proc
                if proc:
                    if proc.pid == 0:
                        logger.info("the ppid is 0,ignore it")
                        continue
                    proc.kill() 
                    logger.info("killed pid:%d", proc.pid)
                else:
                    logger.info("the process:%d is killed when sending close msg to ff", p)
            except: 
                logger.error("close and kill ff --- process id:%d no longer exist",p)

def kill_zhixing():
    cmd = '''taskkill /f /im "zhixing.exe" /T'''
    # print cmd
    ret = os.system(cmd)
    logger.info("%s,ret:%d", cmd, ret)

def kill_all_ff():
    proc_name = ['firefox.exe','crashreporter.exe','plugin-container.exe','WerFault.exe']
    for n in proc_name:
        cmd = '''taskkill /f /im "%s" /T'''%(n)
        # print cmd
        ret = os.system(cmd)
        logger.info("%s,ret:%d", cmd, ret)

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
                kill_zhixing()
                close_kill_ff(h, p_str, should_refresh)
                #task timeout
                status =6
                set_task_status(t,status)
                update_latest_profile_status(task_id,profile_id, status)
                g_logtask.log(server_id, vm_id, task_id, status=status, end_time="CURRENT_TIMESTAMP")
                logger.info("id:%d task:%d is timeout,ran_minutes:%d,timeout:%d", t, task_id, m, timeout)
                # time.sleep(3)
        elif status == 2:
            logger.info("checking standby task:%d,m:%d,standby:%d",task_id, m, standby_time)
            if m>= standby_time :
                # kill_zhixing()
                #在任务task_done是杀掉zhixing,不能在待机时
                
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
                            # elapsed = get_last_input()
                            # # print 'no input elasped',elapsed
                            # if elapsed>g_rto*120000:
                            #     close_kill_ff(h, p_str)
                            #     set_task_status(id, 3)
                            #     update_latest_profile_status(task_id,profile_id, 3)
                            #     g_logtask.log(server_id, vm_id, task_id,status=3, end_time="CURRENT_TIMESTAMP")
                            #     logger.info("long time no input, elasped:%d", elapsed)
                            wins = getwin(u"<错误>")
                            if wins:
                                set_task_status(id, 7)
                                kill_zhixing()
                                close_kill_ff(h, p_str, 0)
                                update_latest_profile_status(task_id,profile_id, 7)
                                g_logtask.log(server_id, vm_id, task_id,status=7, end_time="CURRENT_TIMESTAMP")
                                logger.info("zhixing is error, close and get new task")
                            
                            wins2 = getwin(u"脚本执行")
                            if wins2:
                                set_task_status(id, -2)
                                kill_zhixing()
                                update_latest_profile_status(task_id,profile_id, -2)
                                g_logtask.log(server_id, vm_id, task_id,status=-2, end_time="CURRENT_TIMESTAMP")
                                logger.info("zhixing execute  error, notify vm-schedule to reset net work")

                            wins3 = getwin(u"崩溃报告器")
                            wins4 = getwin(u"Plugin Container for Firefox")
                            wins5 = getwin(u"关闭")
                            if wins3 or wins4 or wins5:
                                logger.info("==========start clean the win environment========")
                                kill_zhixing()
                                kill_all_ff()
                                set_task_status(id, 9)
                                update_latest_profile_status(task_id,profile_id, 9)
                                g_logtask.log(server_id, vm_id, task_id,status=9, end_time="CURRENT_TIMESTAMP")
                                logger.info("==========clean the win environment============")


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
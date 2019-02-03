﻿# -*- coding: utf-8 -*-
'''
 @Author: coldplay
 @Date: 2017-04-12 14:29:23
 @Last Modified by:   coldplay
 @Last Modified time: 2017-04-12 14:29:23
'''
import sys
import optparse
import os
import time
import datetime
import logging
import logging.config
import dbutil
import subprocess
import ctypes
import traceback
import singleton
import psutil
import shutil
import platform
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

cur_date = None
cur_hour = None
vm_id = 0
server_id = 0
script_path = None
# task_script_names = ['bdrank.py', 'sgrank.py', '360rank.py']
task_script_names = ['bdads.py', 'sgads.py', '360ads.py','zgads.py','bingads.py','yahooads.py','bdads.py', '58ads.py']
tempdir = 'x:\\'
wssc_path = None
version = "1.01"

def closeprocess(pname):
    try:
        command = "taskkill /F /IM {0}.exe".format(pname)
        # print command
        os.popen(command)
        return True
    except Exception, e:
        return False

def get_free_space_mb(folder):
    """ Return folder/drive free space (in bytes)
    """
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value/1024/1024
    else:
        st = os.statvfs(folder)
        return st.f_bavail * st.f_frsize/1024


class LASTINPUTINFO(ctypes.Structure):
    """docstring for LASTINPUTINFO"""
    _fields_ = [("cbSize", ctypes.c_long), ("dwTime", ctypes.c_ulong)]


def get_last_input():
    struct_lastinputinfo = LASTINPUTINFO()
    struct_lastinputinfo.cbSize = ctypes.sizeof(LASTINPUTINFO)

    # 获得用户最后输入的相关信息
    user32.GetLastInputInfo(ctypes.byref(struct_lastinputinfo))

    # 获得机器运行的时间
    run_time = kernel32.GetTickCount()

    elapsed = run_time - struct_lastinputinfo.dwTime

    # print "[*] It's been %d milliseconds since the last input event."%elapsed

    return elapsed


def autoargs():
    global vm_id, server_id, wssc_path
    cur_cwd = os.getcwd()
    wssc_path = cur_cwd
    dirs = cur_cwd.split('\\')
    vmname = dirs[-2]
    vm_id = int(vmname[1:])
    server_id = int(dirs[-3])
    logger.info("get vmid,serverid from cwd:%s,%s", vm_id, server_id)


def init():
    parser = optparse.OptionParser()
    parser.add_option(
        "-i",
        "--ip",
        dest="db_ip",
        default="192.168.1.21",
        help="mysql database server IP addrss, default is 192.168.1.235")
    parser.add_option(
        "-n",
        "--name",
        dest="db_name",
        default="vm4",
        help="database name, default is gamedb")
    parser.add_option(
        "-u",
        "--usrname",
        dest="username",
        default="vm",
        help="database login username, default is chinau")
    parser.add_option(
        "-p",
        "--password",
        dest="password",
        default="123456",
        help="database login password, default is 123")
    parser.add_option(
        "-l",
        "--logconf",
        dest="logconf",
        default="./wssc.log.conf",
        help="log config file, default is ./wssc.log.conf")
    parser.add_option(
        "-v",
        "--vid",
        dest="vmid",
        default="0",
        help="log config file, default is 0")
    parser.add_option(
        "-s",
        "--serverid",
        dest="serverid",
        default="0",
        help="log config file, default is 0")
    parser.add_option(
        "-o",
        "--script_path",
        dest="script_path",
        default="",
        help="script path, default script path is...")
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
    logger.info(options)
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
    sql = "select id,cur_task_id,UNIX_TIMESTAMP(a.update_time),task_group_id, terminal_type from vm_cur_task a where a.server_id=%d and status in (1,2)" % (
        server_id)
    # sql = "select  a.id,a.vm_id,max(UNIX_TIMESTAMP(a.update_time)) max_ut from vm_cur_task a where a.server_id=%d "%(server_id)
    res = dbutil.select_sql(sql)
    vms_time = {}
    if not res:
        return vms_time
    for r in res:
        vms_time[r[1]] = {"id": r[0], "ut": r[2], "gid": r[3],"tty":r[4]}
    return vms_time


def vpn_update_time():
    sql = "select UNIX_TIMESTAMP(update_time) from vpn_status where serverid=%d and vpnstatus=1 " % (
        server_id)
    res = dbutil.select_sql(sql)
    if res:
        update_time = res[0][0]
        return update_time
    return None


def is_vpn_dialup_3min():
    '''已拨号成功3分钟'''
    sql = "select 1 from vpn_status where serverid=%d and vpnstatus=1 and UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(update_time)>180" % (
        server_id)
    res = dbutil.select_sql(sql)
    if res:
        print "dial up 3 mins!!!"
        return True
    else:
        return False


def kill_proc_by_pid(proc):
    try:
        logger.info("start to kill pid:%d", proc.pid)
        if proc:
            if proc.pid == 0:
                logger.info("the ppid is 0,ignore it")
                return
            proc.kill()
            logger.info("killed pid:%d", proc.pid)
        else:
            logger.info("the process:%d was not exists ", proc.pid)
    except:
        logger.error("kill proc:%d except", proc.pid)


# 长时间未曾更新任务时间
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
        gid = item['gid']
        tty = item['tty']
        if not time:
            continue
        if time >= redial_time:
            continue
        else:
            if redial_time - time > interval:
                logger.info("===========task proc:%d is not acting======", tid)
                logger.info("task_id:%d,id:%d redial_time-stime>140", tid, id)
                set_task_status(8, id)
                script_name = get_script_name(tid, gid, None, tty)
                cmd_findstr = script_name + " -t %d" % (id)
                proc = find_proc_by_cmdline(cmd_findstr)
                kill_proc_by_pid(proc)
                clean_all_chrome()
                notify_vpn_redial()
                logger.info("===========task proc:%d clean done======", tid)

    return vm_ids


def runcmd(task_id, id, task_type, task_group_id, terminal_type):
    # if task_type in range(0, 6) and task_group_id != 0 and task_group_id<50000:
    # script_name = task_script_names[task_type]
    # elif task_group_id == 0:
    # script_name = "0.py"
    # elif task_group_id >= 50000:
    # script_name = get_task_scriptfile(task_id)
    # else:
    # script_name = str(task_id) + ".py"
    script_name = get_script_name(task_id, task_group_id, task_type,
            terminal_type)
    script = os.path.join(script_path, script_name)
    # script = get_task_scriptfile(task_id)
    print "script:", script
    if not os.path.exists(script):
        logger.error("script:%s not exists", script)
        return False
    os.chdir(script_path)
    commands = ["python", script, "-t", str(id)]
    process = subprocess.Popen(
        commands, creationflags=subprocess.CREATE_NEW_CONSOLE)
    return True


def new_task_come():
    # sql = '''select a.id,a.cur_task_id,a.oprcode,a.cur_profile_id,b.user_type,b.timeout,
    # b.standby_time,a.task_group_id from vm_cur_task a,vm_task b where a.cur_task_id=b.id
    # and a.status=-1 and a.server_id=%d and a.vm_id=%d''' % (int(server_id),
    #                                                         int(vm_id))
    sql = '''select id,cur_task_id,oprcode,cur_profile_id,user_type,timeout,standby_time,task_group_id,
    terminal_type from vm_cur_task where status=-1 and server_id=%d and vm_id=%d''' % (int(server_id),
                                                             int(vm_id))

    # logger.debug(sql)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res:
        return None, None, None, None, None, None, None, None, None
    return res[0][0], res[0][1], res[0][2], res[0][3], res[0][4], res[0][
        5], res[0][6], res[0][7], res[0][8]

def set_task_status_request(status, id):
    sql="insert into task_status_request(task_rid,status,create_time,source) values({0},{1},current_timestamp,'wssc')".format(id,status)
    res = dbutil.execute_sql(sql)
    if res < 0:
        print("update status: failed")
    else:
        print(sql)

def set_task_status(status, id):
    sql = 'update vm_cur_task set status=%d,update_time=current_timestamp where id=%d' % (
        status, id)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception, "%s excute error;ret:%d" % (sql, ret)
    set_task_status_request(status, id)


def update_latest_profile_status(task_id, profile_id, status):
    sql = "update vm_task_profile_latest set status=%d where server_id=%d and vm_id=%d and task_id=%d and profile_id=%d" % (
        status, server_id, vm_id, task_id, profile_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        logger.error("sql:%s, ret:%d", sql, ret)


def get_task_scriptfile(task_id):
    sql = "select script_file from vm_task where id=%d" % (task_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res:
        return None
    script = res[0][0]
    script = script.decode("utf-8").encode("gbk")
    return script


def get_user_type(task_id):
    sql = "select user_type from vm_task where id=%d" % (task_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res:
        return None
    user_type = res[0][0]
    return user_type


def clean_all_chrome():
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            # print proc.info['cmdline']
            if proc.info["cmdline"] is not None and proc.info["cmdline"].find(
                    "firefox.exe") != -1:
                proc.kill()


def find_proc_by_cmdline(cmdline):
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            print proc.info['cmdline']
            logger.info("proc:%s", proc.info['cmdline'])
            logger.info("cmdline:%s", cmdline)
            if proc.info["cmdline"] is not None and proc.info["cmdline"].find(
                    cmdline) != -1:
                logger.info("cmdline:%s is exist", cmdline)
                print cmdline, "is exist"
                return proc
    return None


def notify_vpn_redial():
    sql = 'update vm_isdial_baidu set isdial=1,update_time=current_timestamp where serverid=%d' % (
        server_id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        logger.error("sql:%s, ret:%d", sql, ret)


def get_script_name(task_id, task_group_id, user_type = None, terminal_type=1):
    if user_type is None:
        user_type = get_user_type(task_id)
    if user_type in range(0, 8) and task_group_id != 0 and task_id<50000:
        script_name = task_script_names[user_type]
    elif task_group_id == 0 and terminal_type==1:
        script_name = "0.py"
    elif task_group_id == 0 and terminal_type==2:
        script_name = "0p.py"
    elif task_id >= 50000:
        script_name = get_task_scriptfile(task_id)
    else:
        script_name = str(task_id) + ".py"

    return script_name

def update_task_allot_impl_sub(task_group_id, task_id):
    format_data = {
        "task_id": task_id,
        "group_id": task_group_id
    }
    sql = '''update vm_task_allot_impl set ran_times=ran_times - 1 where id = {group_id} and task_id = {task_id} and time_to_sec(NOW()) 
    between time_to_sec(start_time) and time_to_sec(end_time) and ran_times>0'''.format(**format_data)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        logger.error("update_task_allot_impl_sub")

def is_task_succ(id):
    sql ='''select 1 from vm_cur_task where id={0} and succ_time is not null'''.format(id)
    res = dbutil.select_sql(sql)
    if res and len(res)>0:
        return True
    return False


def clear_timeout_task():
    sql = '''select id,cur_task_id,task_group_id,terminal_type,user_type from vm_cur_task
    where status in (1,2) and server_id=%d and vm_id=%d
    and (ran_minutes>timeout+2  or TO_SECONDS(now())-TO_SECONDS(update_time)>500)''' % (server_id, vm_id)
    # logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        print res
        for r in res:
            id = r[0]
            task_id = r[1]
            task_group_id = r[2]
            tty = r[3]
            user_type = r[4]
            script_name = get_script_name(task_id, task_group_id,user_type, tty)
            cmd_findstr = "%d" % (id)
            logger.info("find proc cmdline:%s", cmd_findstr)
            proc = find_proc_by_cmdline(cmd_findstr)
            if not proc:
                print cmd_findstr, "is not exist"
                logger.info("%s is not exist", cmd_findstr)
                print "task is not running"
                set_task_status(7, id)
                if not is_task_succ(id):
                    update_task_allot_impl_sub(task_group_id, task_id)
            else:
                logger.info("kill timeout task:%s", cmd_findstr )
                print "kill timeout task:", cmd_findstr
                proc.kill()
                set_task_status(6, id)
                if not is_task_succ(id):
                    update_task_allot_impl_sub(task_group_id, task_id)

def clear_rest_timeout_task():
    sql = '''select id,cur_task_id,task_group_id,terminal_type,user_type from vm_cur_task
    where status in (1,2) and server_id=%d and vm_id=%d and user_type=99
    and (ran_minutes>5  or TO_SECONDS(now())-TO_SECONDS(update_time)>500)''' % (server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        for r in res:
            id = r[0]
            task_id = r[1]
            task_group_id = r[2]
            tty = r[3]
            user_type = r[4]
            script_name = get_script_name(task_id, task_group_id,user_type, tty)
            cmd_findstr = script_name + " -t %d" % (id)
            logger.info("find proc cmdline:%s", cmd_findstr)
            proc = find_proc_by_cmdline(cmd_findstr)
            if not proc:
                print cmd_findstr, "is not exist"
                print "task is not running"
                set_task_status(6, id)
            else:
                logger.info("kill timeout task:%s", cmd_findstr )
                print "kill timeout task:", cmd_findstr
                proc.kill()
                set_task_status(6, id)

def clear_running_rest_task():
    sql = '''select id,cur_task_id,task_group_id,terminal_type,user_type from vm_cur_task
    where status in (1) and server_id=%d and vm_id=%d and user_type=99
    ''' % (server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        for r in res:
            id = r[0]
            task_id = r[1]
            task_group_id = r[2]
            tty = r[3]
            user_type = r[4]
            script_name = get_script_name(task_id, task_group_id,user_type, tty)
            cmd_findstr = "%d" % (id)
            logger.info("find proc cmdline:%s", cmd_findstr)
            proc = find_proc_by_cmdline(cmd_findstr)
            if proc:
                logger.info("kill running rest task:%s", cmd_findstr )
                proc.kill()
                set_task_status(13, id)

def clear_running_zero_task():
    sql = '''select id,cur_task_id,task_group_id,terminal_type,user_type from vm_cur_task
    where status in (1,2) and server_id=%d and vm_id=%d and task_group_id=0
    ''' % (server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        for r in res:
            id = r[0]
            task_id = r[1]
            task_group_id = r[2]
            tty = r[3]
            user_type = r[4]
            script_name = get_script_name(task_id, task_group_id,user_type, tty)
            cmd_findstr = "%d" % (id)
            logger.info("find proc cmdline:%s", cmd_findstr)
            proc = find_proc_by_cmdline(cmd_findstr)
            if proc:
                logger.info("kill running rest task:%s", cmd_findstr )
                proc.kill()
                set_task_status(13, id)

def del_timeout_task():
    sql = '''select id,cur_task_id,task_group_id,terminal_type,user_type from vm_cur_task
    where status in (1,2) and server_id=%d and vm_id=%d''' % (server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        print res
        for r in res:
            id = r[0]
            task_id = r[1]
            task_group_id = r[2]
            tty = r[3]
            user_type = r[4]
            script_name = get_script_name(task_id, task_group_id,user_type, tty)
            cmd_findstr = "%d" % (id)
            logger.info("find proc cmdline:%s", cmd_findstr)
            proc = find_proc_by_cmdline(cmd_findstr)
            if not proc:
                logger.info("%s is not exist", cmd_findstr)
                print cmd_findstr, "is not exist"
                print "task is not running"
                set_task_status(7, id)
                if not is_task_succ(id):
                    update_task_allot_impl_sub(task_group_id, task_id)

            # for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            #     if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            #         proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            #         # print proc.info['cmdline']
            #         if proc.info["cmdline"] is not None and proc.info["cmdline"].find(cmd_findstr) != -1:
            #             print cmd_findstr, "is exist"
            #             print "id", id ,"===task_id",task_id, " is running"
            #             return
            # set_task_status(7,id)

def get_firefox():
    proc_list = []
    for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
        # print proc.info['name']
        if proc.info["cmdline"] is not None and len(proc.info["cmdline"]) != 0:
            proc.info["cmdline"] = " ".join(proc.info["cmdline"])
            # print proc.info['cmdline']
            if proc.info["cmdline"] is not None and proc.info["cmdline"].find( "firefox.exe --marionette") != -1:
                print proc.info['cmdline']
                proc_list.append(proc)
    return proc_list

def clean_all_firefox():
    plist = get_firefox()
    if len(plist) >= 2:
        print "****************firefox too much********************"
        closeprocess("firefox")
        closeprocess("geckodriver")
        time.sleep(3)
        for i in range(1,2):
            if len(plist) <=0:
                break
            plist = get_firefox()
            if len(plist) >0:
                closeprocess("firefox")
                closeprocess("geckodriver")
                time.sleep(2)

        print "********************clean all process********************"

def get_wssc_version():
    sql = "select `value` from vm_sys_dict where `key`='wssc_version'"
    res = dbutil.select_sql(sql)
    if not res or len(res)<1:
        return None
    return res[0][0]

def update_status_and_time():
    wssc_version = get_wssc_version()
    warn = 0
    if wssc_version != version:
        warn = 1
    sql = "update vm_list set wssc_version='%s',warn=%d,update_time=CURRENT_TIMESTAMP where server_id = %s and vm_id = %s" % (
        wssc_version, warn, server_id, vm_id)
    res = dbutil.execute_sql(sql)
    if res == 0:
        logger.error("update zero row")
    return res


def removePath(destinationPath):
    try:
        if os.path.exists(destinationPath):
            pathList = os.listdir(destinationPath)
            for path in pathList:
                pathFull = os.path.join(destinationPath, path)
                if os.path.isdir(pathFull):
                    if pathFull.find("_MEI") != -1:
                        print pathFull
                        removePath(pathFull)
                shutil.rmtree(destinationPath, True)
    except Exception, e:
        logger.error("delete tempdir error:%s",
                    e.message)

def removeTmpdir(destinationPath):
    try:
        if os.path.exists(destinationPath):
            pathList = os.listdir(destinationPath)
            for path in pathList:
                pathFull = os.path.join(destinationPath, path)
                if os.path.isdir(pathFull):
                    if pathFull.find("tmp") != -1:
                        print pathFull
                        shutil.rmtree(pathFull, True)
    except Exception, e:
        logger.error("delete tmpdir error:%s",
                    e.message)


def clear_on_newday(temp_dir):
    global cur_date
    today = datetime.date.today()
    print "today:", today, "====", "cur_date:", cur_date
    if today != cur_date:
        logger.info("==========clear tempdir on new day start==========")
        # removePath(temp_dir)
        removeTmpdir("d:\\selenium")
        cur_date = today
        # os.mkdir(temp_dir)
        logger.info("==========clear tempdir on new day end==========")

def clear_by_hours(temp_dir):
    global cur_date,cur_hour
    nowtime = datetime.datetime.now()
    nowhour = nowtime.hour
    print "nowhour:", nowhour, "====", "cur_hour:", cur_hour
    if nowhour != cur_hour:
        logger.info("==========clear tempdir on new hour start==========")
        # removePath(temp_dir)
        removeTmpdir("d:\\selenium")
        cur_hour =nowhour
        # os.mkdir(temp_dir)
        logger.info("==========clear tempdir on new hour end==========")

def reinstall_driver2():
    sql = '''select 1 from vm_cur_task where
    UNIX_TIMESTAMP(update_time)>UNIX_TIMESTAMP(now())-1800
    and status =4 and server_id={0} and vm_id={1}'''.format(server_id,vm_id)
    res = dbutil.select_sql(sql)
    if not res or len(res)<=0:
        #cmd = "python -m pip install --upgrade --no-deps --force-reinstall {0}".format("selenium-3.13.0-py2.py3-none-any.whl")
        cmd = "python -m pip install --upgrade --no-deps --force-reinstall {0}".format("selenium")
        print "reinstall driver cmd:", cmd
        os.system(cmd)


def reinstall_driver(tempdir):
    size = get_free_space_mb("x:\\")
    if size<=80:
        # cmd = "python -m pip install --upgrade --no-deps --force-reinstall {0}".format("selenium-3.13.0-py2.py3-none-any.whl")
        # print "reinstall driver cmd:", cmd
        # os.system(cmd)
        removePath(tempdir)
        removeTmpdir("d:\\selenium")

def main():
    myapp = singleton.singleinstance("wssc.py")
    myapp.run()
    init()
    try:
        while True:
            try:
                while True:
                    # reinstall_driver2()
                    update_status_and_time()
                    reinstall_driver(tempdir)
                    # clean_all_firefox()
                    # clear_by_hours(tempdir)
                    closeprocess("WerFault")
                    # clear_rest_timeout_task()
                    id, task_id, oprcode, profile_id, task_type, timeout, standby, task_group_id, terminal_type = new_task_come(
                    )
                    if id is not None:
                        closeprocess("firefox")
                        closeprocess("geckodriver")
                        #if task_type !=99:
                        #    clear_running_rest_task()
                        #    time.sleep(1)
                        if task_group_id !=0:
                            clear_running_zero_task()
                            time.sleep(1)
                        ntppath = os.path.join(wssc_path,"ntupdate.bat")
                        commands = [ntppath]
                        process = subprocess.Popen(
                            commands, creationflags=subprocess.CREATE_NEW_CONSOLE)
                        print "get task", task_id
                        ret = runcmd(task_id, id, task_type, task_group_id, terminal_type)
                        if ret:
                            set_task_status(1, id)
                        else:
                            update_latest_profile_status(
                                task_id, profile_id, 3)
                            set_task_status(3, id)
                            update_task_allot_impl_sub(task_group_id, task_id)
                    clear_timeout_task()
                    del_timeout_task()
                    kill_zombie_proc()
                    time.sleep(3)

            except Exception, e:
                print 'first while traceback.print_exc():'
                traceback.print_exc()
                logger.error('exception on main_loop', exc_info=True)
                time.sleep(5)
                continue

    except Exception, e:
        print 'traceback.print_exc():'
        traceback.print_exc()
        # logger.error('exception on main_loop', exc_info = True)

def test_clear():
    init()
    get_create_time()
    # clear_on_newday("d:\profiles")

if __name__ == "__main__":
    while True:
        try:
            # print(get_free_space_mb('b:\\'),'MB')
            # clear_by_hours('d:\\profiles')
            # is_task_succ(4225653)
            main()
        except Exception, e:
            print 'traceback.print_exc():'
            traceback.print_exc()
            logger.error('exception on main_loop', exc_info=True)
            time.sleep(5)

# @CreateTime: Sep 13, 2017 11:46 AM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Sep 13, 2017 2:36 PM
# @Description: Modify Here, Please

# -*- coding: utf-8 -*-
import os
import time
import threading
logger = None
g_vManager_path = None
g_current_dir =  os.getcwd() 

#0:succ 1:failed
def startVM(vmName):
    logger.info(" startVM:%s", vmName)
    global g_vManager_path, g_current_dir
    #cmd = "vboxmanage.exe startvm " + vmName + " -type headless"
    cmd = "vboxmanage.exe startvm " + vmName
    logger.info(" cmd:%s", cmd)
    i = 0
    while i < 5:
        os.chdir(g_vManager_path)
        status = os.system(cmd)
        os.chdir(g_current_dir)
        logger.info(" cmd:%s ret=%d", cmd, status)
        if 0 != status:
            i = i + 1
            logger.info(" retry startVM:%s", vmName)
            time.sleep(5)
            continue
        return 0
    return 1


def forceStartNewVM(groupname, groupid, cur_pos, quantity, id):
    logger.info("=========force start new vm %s===========", groupname)
    select_be_started_vm()
    ret = poweroffVM(groupname)
    if ret != 0:
        log_exp_vm(groupid, cur_pos, g_serverid)
        ret_off = poweroffVM(groupname)
        if ret_off != 0:
            sql = "update vm_group set status=%d where id=%d" % (-4, id)
            execute_sql_loop(sql)
            return False
    sql_uprtimes = "update vm_group set status=1,restart_times=restart_times+1 where id=%d" % (
        id)
    logger.info(sql_uprtimes)
    execute_sql_loop(sql_uprtimes)
    return ret


def poweroffVM2(vmName):
    logger.info(" ==========start poweroffVM:%s=============", vmName)
    cmd = "vboxmanage controlvm " + vmName + " poweroff"
    logger.info("cmd:%s", cmd)
    os.chdir(g_vManager_path)
    status = os.system(cmd)
    os.chdir(g_current_dir)
    logger.info(" ==========end poweroffVM:%s,retcode=%d=============", vmName,
                status)
    if status == 0:
        return 0
    else:
        return 1


def poweroffVM(vmName):
    logger.info(" ==========starting shutdownVM:%s=============",
                vmName)
    global g_vManager_path, g_current_dir
    cmd = "vboxmanage controlvm " + vmName + " poweroff"
    logger.info(" cmd:%s", cmd)
    j = 0
    while j < 3:
        os.chdir(g_vManager_path)
        if vmName in list_allrunningvms():
            os.chdir(g_vManager_path)
            status = os.system(cmd)
            os.chdir(g_current_dir)
            logger.info(" cmd ret=%d", status)
            if 0 != status:
                logger.info(
                    " poweroff %s ,cmd %s failed and will sleep 5s to retry",
                     vmName, cmd)
                time.sleep(5)
                j = j + 1
                continue
            j = j + 1
            logger.info(" poweroff %s retry %d times * 5s", vmName,
                        j)
        else:
            logger.info(
                " ==============%s is not running,exit poweroffvm============",
                 vmName)
            return 0
    logger.error(
        " ================poweroff %s failed after %d times * 45s======================",
         vmName, j)
    return 1


def shutdownVM(vmName):
    logger.info(" ==========starting shutdownVM:%s=============",
                vmName)
    global g_vManager_path, g_current_dir
    cmd = "vboxmanage controlvm " + vmName + " acpipowerbutton"
    logger.info(" cmd:%s", cmd)
    j = 0
    while j < 5:
        os.chdir(g_vManager_path)
        if vmName in list_allrunningvms():
            os.chdir(g_vManager_path)
            status = os.system(cmd)
            os.chdir(g_current_dir)
            logger.info(" cmd ret=%d", status)
            if 0 != status:
                logger.info(
                    " shutdown %s ,cmd %s failed and will sleep 5s to retry",
                     vmName, cmd)
                time.sleep(5)
                j = j + 1
                continue
            i = 0
            while i < 10:
                if vmName in list_allrunningvms():
                    time.sleep(5)
                    i = i + 1
                    continue
                logger.info(
                    " =============shutdown %s succ after retry %d times=============",
                     vmName, i)
                return 0
            j = j + 1
            logger.info(" shutdown %s retry %d times * 45s", vmName,
                        j)
        else:
            logger.info(
                " ==============%s is not running,exit shutdownvm============",
                 vmName)
            return 0
    logger.error(
        " ================shutdown %s failed after %d times * 45s======================",
         vmName, j)
    return 1


def savestateVM(vmName):
    logger.info("savestateVM:%s", vmName)
    global g_vManager_path, g_current_dir
    cmd = "vboxmanage controlvm " + vmName + " savestate"
    logger.info("cmd:%s", cmd)
    os.chdir(g_vManager_path)
    status = os.system(cmd)
    os.chdir(g_current_dir)
    logger.info("cmd:%s ret=%d", cmd, status)
    if 0 != status:
        return 1
    return 0


def list_allrunningvms():
    global g_vManager_path, g_current_dir
    os.chdir(g_vManager_path)
    cmd = "vboxmanage list runningvms"
    r = os.popen(cmd)
    res = r.readlines()
    r.close()
    os.chdir(g_current_dir)
    for l in res:
        l = l.strip("\r\n")
        vmname = l.split('"')[1]
        yield vmname

def pauseVM(vmName):
    tname = threading.current_thread().getName()
    logger.info("[%s] ==========starting pauseVM:%s=============", tname,
                vmName)
    global g_vManager_path, g_current_dir, g_cur_running_count
    #print g_vManager_path
    cmd = "vboxmanage controlvm " + vmName + " pause"
    # cmd = "vboxmanage controlvm " + vmName + " acpipowerbutton"
    logger.info("[%s] cmd:%s", tname, cmd)
    j = 0
    while j < 2:
        os.chdir(g_vManager_path)
        if vmName in list_allrunningvms():
            os.chdir(g_vManager_path)
            status = os.system(cmd)
            os.chdir(g_current_dir)
            logger.info("[%s] cmd ret=%d", tname, status)
            if 0 != status:
                logger.info(
                    "[%s] pause %s ,cmd %s failed and will sleep 5s to retry",
                    tname, vmName, cmd)
                time.sleep(2)
                j = j + 1
                continue
            j = j + 1
            logger.info("[%s] pause %s retry %d times * 2s", tname, vmName,
                        j)
        else:
            logger.info(
                "[%s] ==============%s is not running,exit pausevm============",
                tname, vmName)
            return 0
    logger.error(
        "[%s] ================pause %s failed after %d times * 45s======================",
        tname, vmName, j)
    return 1

def resumeVM(vmName):
    tname = threading.current_thread().getName()
    logger.info("[%s] ==========starting reusmeVM:%s=============", tname,
                vmName)
    global g_vManager_path, g_current_dir, g_cur_running_count
    #print g_vManager_path
    cmd = "vboxmanage controlvm " + vmName + " resume"
    # cmd = "vboxmanage controlvm " + vmName + " acpipowerbutton"
    logger.info("[%s] cmd:%s", tname, cmd)
    j = 0
    while j < 2:
        os.chdir(g_vManager_path)
        if vmName in list_allrunningvms():
            os.chdir(g_vManager_path)
            status = os.system(cmd)
            os.chdir(g_current_dir)
            logger.info("[%s] cmd ret=%d", tname, status)
            if 0 != status:
                logger.info(
                    "[%s] resume %s ,cmd %s failed and will sleep 5s to retry",
                    tname, vmName, cmd)
                time.sleep(2)
                j = j + 1
                continue
            j = j + 1
            logger.info("[%s] resume %s retry %d times * 2s", tname, vmName,
                        j)
        else:
            logger.info(
                "[%s] ==============%s is not running,exit resumevm============",
                tname, vmName)
            return 0
    logger.error(
        "[%s] ================resume %s failed after %d times * 45s======================",
        tname, vmName, j)
    return 1


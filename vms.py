# @CreateTime: Sep 13, 2017 3:47 PM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Sep 13, 2017 3:47 PM
# @Description: Modify Here, Please

# -*- coding: utf-8 -*-
import vm_utils
import dbutil

logger = None
dbutil = None
def update_vm_status(id, status):
    sql = "update vm_list set status=%d where id=%d"%(status, id)
    logger.info("%s", sql)
    ret = dbutil.execute_sql(sql)
    if ret <0 :
        raise Exception,"update vm status failed:%s:%d"(sql,ret)

def update_vm_status_starttime(id, status):
    sql = "update vm_list set status=%d,startup_time=CURRENT_TIMESTAMP where id=%d"%(status, id)
    logger.info("%s", sql)
    ret = dbutil.execute_sql(sql)
    if ret <0 :
        raise Exception,"update vm status failed:%s:%d"(sql,ret)

def update_vm_status_endtime(id, status):
    sql = "update vm_list set status=%d,shutdown_time=CURRENT_TIMESTAMP where id=%d"%(status, id)
    logger.info("%s", sql)
    ret = dbutil.execute_sql(sql)
    if ret <0 :
        raise Exception,"update vm status failed:%s:%d"(sql,ret)

def get_vms(server_id):
    sql= "select id,vm_id,vm_name from vm_list where server_id=%d and enabled=0 "%(server_id)
    logger.info("%s", sql)
    res = dbutil.select_sqlwithdict(sql)
    running_vmnames,running_vmids = [],[]
    if res:
        for row in res:
            vm_id = row['vm_id']
            vm_name = row['vm_name']
            id = row["id"]
            running_vmnames.append(vm_name)
            running_vmids.append(vm_id)
    return running_vmnames, running_vmids

def start_vms(server_id, srange, erange):
    #sql= "select id,vm_id,vm_name from vm_list where server_id=%d and enabled=0 and vm_id between %d and %d"%(server_id,srange, erange)
    sql= "select id,vm_id,vm_name from vm_list where server_id=%d and enabled=0 "%(server_id)
    logger.info("%s", sql)
    res = dbutil.select_sqlwithdict(sql)
    running_vmnames,running_vmids = [],[]

    for row in res:
        vm_id = row['vm_id']
        vm_name = row['vm_name']
        id = row["id"]
        ret = vm_utils.poweroffVM(vm_name)
        if ret != 0:
            status = -4
            logger.info("shutdown error:%d,%s", row["id"], row["groupname"])
        ret = vm_utils.startVM(vm_name)
        if ret != 0:
            update_vm_status(-3, id)
        else:
            update_vm_status_starttime(0, id)
            running_vmnames.append(vm_name)
            running_vmids.append(vm_id)
    return running_vmnames, running_vmids

def shutdown_allvm(server_id):
    sql_all = "select id,vm_id,vm_name from vm_list where server_id=" + str(
        server_id)
    sql_updatestatus = "update vm_list set status=%d, shutdown_time=CURRENT_TIMESTAMP where id=%d"
    res = dbutil.select_sqlwithdict(sql_all)
    if res is None or len(res) <= 0:
        logger.info("empty res")
        return
    for row in res:
        ret = vm_utils.poweroffVM(row["vm_name"])
        if ret != 0:
            logger.info("shutdown error:%d,%s", row["vm_id"], row["vm_name"])
            continue
        sql = sql_updatestatus % (1, row["id"])
        logger.info(sql)
        ret = dbutil.execute_sql(sql)
        if ret < 0:
            logger.error("exception on excute_sql:%s", sql)
            continue


def reset_allvm(server_id):
    sql_all = "select id,vm_id,vm_name from vm_list where server_id=" + str(
        server_id)
    sql_updatestatus = "update vm_list set status=%d, shutdown_time=CURRENT_TIMESTAMP where id=%d"
    res = dbutil.select_sqlwithdict(sql_all)
    if res is None or len(res) <= 0:
        logger.info("empty res")
        return
    for row in res:
        ret = vm_utils.resetVM(row["vm_name"])
        if ret != 0:
            logger.info("reset error:%d,%s", row["vm_id"], row["vm_name"])
            continue
        sql = sql_updatestatus % (1, row["id"])
        logger.info(sql)
        ret = dbutil.execute_sql(sql)
        if ret < 0:
            logger.error("exception on excute_sql:%s", sql)
            continue

def pause_allvm(server_id):
    sql_all = "select vm_id,vm_name from vm_list where enabled=0 and server_id=" + str(
        server_id)
    res = dbutil.select_sqlwithdict(sql_all)
    if res is None or len(res) <= 0:
        logger.info("empty res")
        return
    for row in res:
        ret = vm_utils.pauseVM(row["vm_name"])
        if ret != 0:
            logger.info("pause error:%d,%s", row["vm_id"], row["vm_name"])
            continue
        
def resume_allvm(server_id):
    sql_all = "select vm_id, vm_name from vm_list where server_id=" + str(
        server_id)
    res = dbutil.select_sqlwithdict(sql_all)
    if res is None or len(res) <= 0:
        logger.info("empty res")
        return
    for row in res:
        ret = vm_utils.resumeVM(row["vm_name"])
        if ret != 0:
            logger.info("resume error:%d,%s", row["vm_id"], row["vm_name"])
            continue



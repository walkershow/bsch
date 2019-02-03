#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : bsch/vm-scheduler.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 15.05.2018 17:46:1526377570
# Last Modified Date: 22.05.2018 17:21:1526980881
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
import sys
import ConfigParser
import datetime
import optparse
import os
import time
import threading
import signal
import logging
import logging.config
# import colorer  # for colored logger
import dbutil
from task.taskallot import TaskAllot
import task.taskallot
from task.taskallot_rolling import TaskAllotRolling
import vms
import vm_utils
import utils
import task.parallel
from task.parallel import ParallelControl
#from task.rolling_user import UserAllot
from task.user import UserAllot
from task.user_ec import UserAllot_EC
from task.user_rest import UserAllot as UserAllot_Rest
from task.user_rolling7 import UserAllot as UserAllot7
from task.user_reg import UserAllot as UserAllot_Reg
from task.user_iqy import UserAllot as UserAllot_IQY
from task.user_iqyall import UserAllot as UserAllot_IQYALL
from task.user_iqyatv import UserAllot as UserAllot_IQYATV
from logbytask.logtask import LogTask
from manvm import CManVM
from random import choice,shuffle
global g_vManager_path
global g_current_dir
global g_reset
global g_origin_limit
from dbutil import DBUtil  # use when multi db connection needed

g_serverid           = 0
g_rcv                = 0
g_rto                = 0
g_rto_tmp            = 0
g_taskallot          = None
g_logtask            = None
g_task_profile       = None
g_cur_date           = datetime.date.today()
vm_names             = []
vm_ids               = []
g_reset_waittime     = 120
g_pb                 = 1
g_pc                 = None
exit_flag            = False
g_user               = None
g_userrest           = None
g_userec             = None
g_user7              = None


def get_cur_hour():
    now = datetime.datetime.now()
    return now.hour

def is_new_day():
    global g_cur_date
    today = datetime.date.today()
    if g_cur_date == today:
        return False
    return True

def is_run_as_single(area):
    sql = '''select ifnull(single_gid,'') from vpn_status where
    area=%s'''%(area)
    print sql
    res = dbutil.select_sql(sql)
    print "single", res
    print len(res)
    print res[0]
    if not res or len(res)<=0:
        return 0
    single_gid = res[0][0]
    print 'sgid', single_gid
    return single_gid

def get_dialup_arealist():
    sql = '''select area,update_time from vpn_status where vpnstatus=1 and area!='' order by rand()'''
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res:
        return None
    area_dict= {}
    for r in res:
        #print r[1]
        if r[1] is None :
            continue
        area_dict.update({r[0]:r[1]})
    #print area_dict
    return area_dict

def is_valid_area(area):
    '''当前改区域拨号后非0跑<4个，可以继续分配任务'''
    area = str(area)
    areadict= get_dialup_arealist()
    
    print areadict.keys()
    if not areadict:
        print "not area dict"
        return False 
    print str(area)
    if str(area) not in areadict.keys():
        print "are is here"
        return False
    sql = "select count(*) from vm_cur_task where area={0} and task_group_id!=0 \
     and start_time>'{1}' and (status in(-1,1,2) or succ_time is not null)"
    # sql = "select count(*) from vm_cur_task where area={0} and task_group_id!=0 \
    # and start_time>{1} and (status in(-1,1,2) or succ_time is not null)"
    k = area
    v = areadict[k]
    sql_tmp = sql.format(k,v)
    logger.info(sql_tmp)
    res = dbutil.select_sql(sql_tmp)
    if not res:
        logger.info("area:%s,可以跑任务", k)
        return True
    logger.info("area:%s, task num:%d", k ,res[0][0])
    if res[0][0]<4:
        logger.info("area:%s,任务数:%d 可以跑任务", k,res[0][0])
        return True
    return False


def get_valid_area():
    '''当前改区域拨号后非0跑<4个，可以继续分配任务'''
    areadict= get_dialup_arealist()
    if not areadict:
        return -1
    area_str = ','.join(areadict.keys())
    logger.info("获取有效地区列表:%s", area_str)
    area_list = areadict.keys()
    shuffle(area_list)
    logger.info("打乱有效地区列表:%s", area_list)
    sql = "select count(*) from vm_cur_task where area={0} and task_group_id!=0 \
     and start_time>'{1}' and (status in(-1,1,2) or succ_time is not null)"
    # sql = "select count(*) from vm_cur_task where area={0} and task_group_id!=0 \
    # and start_time>{1} and (status in(-1,1,2) or succ_time is not null)"
    for area in area_list:
        k = area
        v = areadict[k]
    #for k,v in areadict.items():
        sql_tmp = sql.format(k,v)
        logger.info(sql_tmp)
        res = dbutil.select_sql(sql_tmp)
        if not res:
            logger.info("area:%s,可以跑任务", k)
            return k
        logger.info("area:%s, task num:%d", k ,res[0][0])
        if res[0][0]<4:
            logger.info("area:%s,任务数:%d 可以跑任务", k,res[0][0])
            return k
        else:
            continue
    return -1

def is_valid_zero_area(area):
    '''当前改区域拨号后非0跑<4个，可以继续分配任务'''
    return False
    areadict= get_dialup_arealist()
    if not areadict:
        return False 
    if area not in areadict.keys():
        return False
    sql = "select count(*) from vm_cur_task where area={0}  \
    and start_time>'{1}' and status in(-1,1,2) "
    k = area
    v = areadict[k]
    sql_tmp = sql.format(k,v)
    logger.info(sql_tmp)
    res = dbutil.select_sql(sql_tmp)
    if not res:
        logger.info("0area:%s,可以零跑任务", k)
        return True
    logger.info("0area:%s, task num:%d", k ,res[0][0])
    if res[0][0]<4:
        logger.info("0area:%s,任务数:%d 可以零跑任务", k,res[0][0])
        return True
    return False

def get_valid_zero_area():
    return -1
    '''当前改区域拨号后非0跑<4个，可以继续分配任务'''
    areadict= get_dialup_arealist()
    if not areadict:
        return -1
    area_str = ','.join(areadict.keys())
    logger.info("获取有效地区列表:%s", area_str)
    sql = "select count(*) from vm_cur_task where area={0}  \
    and start_time>'{1}' and status in(-1,1,2) "
    for k,v in areadict.items():
        sql_tmp = sql.format(k,v)
        print sql_tmp
        logger.info(sql_tmp)
        res = dbutil.select_sql(sql_tmp)
        print 'res', res
        print 'res[0]', res[0]
        if not res:
            logger.info("0area:%s,可以跑任务", k)
            return k
        logger.info("0area:%s, task num:%d", k ,res[0][0])
        if res[0][0]<4:
            logger.info("0area:%s,任务数:%d 可以跑任务", k,res[0][0])
            return k
        else:
            return -1
    return -1

def get_rand_area():
    area_list = get_valid_arealist()
    if not area_list:
        return None
    area = choice(area_list)
    return area

def can_iqy_run():
    #return False
    sql = '''SELECT
                    distinct a.id
                FROM
                    vm_task_group b,
                    vm_task_allot_impl a,
                    vm_allot_task_by_servergroup c,
                    vm_task d,
                    vm_server_group f
                WHERE
                    b.id = a.id
                AND b.task_id = a.task_id
                AND d.id = b.task_id
                AND d. STATUS = 1
                AND f.id = c.server_group_id
                and f.status =1
                AND c.task_group_id = b.id
                AND time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
                AND time_to_sec(a.end_time)
                AND a.ran_times < a.allot_times
                AND b.ran_times < b.times
                AND b.id > 0
                AND c.task_group_id = a.id
                and b.ranking > 0
                and b.priority = 0
                and b.id in(50277)
                AND f.server_id = %d order by ranking desc ''' % (g_serverid)
    logger.info(sql)
    #print "sql",sql
    res = dbutil.select_sql(sql)
    print "======================="
    print "get res in iqy pri ranking", res
    print "======================="
    ids = set()
    for r in res:
        ids.add(r[0])
    if ids:
        return True
    return False

def can_iqyatv_run():
    #return False
    sql = '''SELECT
                    distinct a.id
                FROM
                    vm_task_group b,
                    vm_task_allot_impl a,
                    vm_allot_task_by_servergroup c,
                    vm_task d,
                    vm_server_group f
                WHERE
                    b.id = a.id
                AND b.task_id = a.task_id
                AND d.id = b.task_id
                AND d. STATUS = 1
                AND f.id = c.server_group_id
                and f.status =1
                AND c.task_group_id = b.id
                AND time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
                AND time_to_sec(a.end_time)
                AND a.ran_times < a.allot_times
                AND b.ran_times < b.times
                AND b.id > 0
                AND c.task_group_id = a.id
                and b.ranking > 0
                and b.priority = 0
                and b.id in(50278)
                AND f.server_id = %d order by ranking desc ''' % (g_serverid)
    logger.info(sql)
    #print "sql",sql
    res = dbutil.select_sql(sql)
    print "======================="
    print "get res in iqyatv pri ranking", res
    print "======================="
    ids = set()
    for r in res:
        ids.add(r[0])
    if ids:
        return True
    return False

def get_iqyall_valid_area(vm_id):
    sql = '''select area from vm_users where server_id={0} and vm_id={1}
            and user_type=11 and mobile_no is not null'''.format(g_serverid,
                    vm_id)
    res = dbutil.select_sql(sql)
    if not res:
       return None
    areas = []
    for r in res:
        areas.append(r[0])
    # areas_done = get_iqy_atving_area(vm_id,False)
    # areas_all = list(set(areas).difference(set(areas_done)))
    area = choice(areas)
    return area
    
def get_iqy_valid_area(vm_id):
    # sql = '''select area from vm_users where server_id={0} and vm_id={1}
            # and user_type=11 and status=1 and mobile_no is not null'''.format(g_serverid,
                    # vm_id)
    sql = '''SELECT
                    area
            FROM
                    vm_users 
            WHERE
                    profile_id not IN (
                    SELECT
                            cur_profile_id 
                    FROM
                            vm_cur_task 
                    WHERE
                            server_id = {0} 
                            AND vm_id = {1} 
                            AND user_type in(11,13)
                            and status>3
                            AND UNIX_TIMESTAMP( NOW( ) ) - UNIX_TIMESTAMP( start_time ) <21600
                            union 
                            select 
                            cur_profile_id 
                    FROM
                            vm_cur_task 
                    WHERE
                            server_id = {0} 
                            AND vm_id = {1} 
                            AND user_type = 12
                            and status in(-1,1,2) 
                            )
	AND server_id = {0} 
	AND vm_id =  {1}
	AND user_type = 11 
	AND STATUS = 1 
	AND mobile_no IS NOT NULL'''.format(g_serverid, vm_id)
    print sql
    res = dbutil.select_sql(sql)
    if not res:
       return []
    areas = []
    for r in res:
        areas.append(r[0])
    print("区域", areas)
    # areas_done = get_iqy_atving_area(vm_id)
    # print("iqy atving 区域", areas_done)
    # areas_all = list(set(areas).difference(set(areas_done)))
    # area = choice(areas_all)
    area = choice(areas)
    return area


def get_iqy_atving_area(vm_id, is_vip=True):
    # sql = '''select area from vm_users where server_id={0} and vm_id={1}
            # and user_type=11 and status=1 and mobile_no is not null'''.format(g_serverid,
                    # vm_id)
    sql = ''' SELECT
                            cur_profile_id 
                    FROM
                            vm_cur_task 
                    WHERE
                            server_id = {0} 
                            AND vm_id = {1} 
                            AND user_type = 12
                            and status in(-1,1,2) 
    '''
    sql = sql.format(g_serverid, vm_id)
    print sql
    res = dbutil.select_sql(sql)
    areas = []
    for r in res:
        areas.append(r[0])
    return areas

    
def get_iqyatv_valid_area(vm_id):
    sql = '''select area from vm_users where server_id={0} and vm_id={1}
            and user_type=11 and status=0 and mobile_no is not null'''.format(g_serverid,
                    vm_id)
    res = dbutil.select_sql(sql)
    if not res:
       return None
    areas = []
    for r in res:
        areas.append(r[0])
    area = choice(areas)
    return area

def iqyatv_business(vm_id):
    sql = '''select a.vm_id from vm_cur_task a where a.server_id=%d and
    a.vm_id=%d and a.status in(1,-1,2) '''
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    sqltmp = sql % (g_serverid, vm_id)
    res = dbutil.select_sql(sqltmp)
    print sqltmp
    print res
    if not res:
        print "in iqyatv heer"
        sqltmp = sql_count % (g_serverid, vm_id)
        res = dbutil.select_sql(sqltmp)
        count = 0
        print res
        if res:
            count = res[0][0]
        logger.warn("running task vm:%d,count:%d", vm_id, count)
        if count < g_pb:
            area = get_iqyatv_valid_area(vm_id)
            print "iqyatv area:",area
            #area = '58'
            if not area:
                return False
            else:
                logger.info("当前area:%s", area)
            try:   
            #if True:
                with utils.SimpleFlock("/tmp/area/{0}.lock".format(area), 1):
                    ret = is_valid_area(area)
                    if not ret:
                        logger.info("area:%s 不可用", area)
                        return
                    ret,task_id = g_taskallot.allot_by_priority(
                        vm_id, area, None,12)
                    print "===== get task ======:", task_id
                    if not ret or task_id is None:
                        logger.warn(
                            utils.auto_encoding('''虚拟机:%d 没有non zero任务可运行,
                                分配显示任务'''), vm_id)
                        return False
                    return True


            except Exception, e:
                logger.error('exception on area lock', exc_info=True)
                time.sleep(2)
                return False
                
        else:
            logger.warn(
                utils.auto_encoding('''虚拟机:%d
                    当前运行任务数:%d>=%d'''),
                vm_id, count, g_pb)
            return False
    else:
        logger.info(
            utils.auto_encoding("当前虚拟机:%d,已分配任务或有正在执行的任务"),
            vm_id)
        return False

def iqyall_business(vm_id):
    sql = '''select a.vm_id from vm_cur_task a where a.server_id=%d and
    a.vm_id=%d and a.status in(1,-1,2) '''
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    sqltmp = sql % (g_serverid, vm_id)
    res = dbutil.select_sql(sqltmp)
    print sqltmp
    print res
    if not res:
        print "in iqyall heer"
        sqltmp = sql_count % (g_serverid, vm_id)
        res = dbutil.select_sql(sqltmp)
        count = 0
        print res
        if res:
            count = res[0][0]
        logger.warn("running task vm:%d,count:%d", vm_id, count)
        if count < g_pb:
            area = get_iqyall_valid_area(vm_id)
            print "area:",area
            #area = '58'
            if not area:
                return False
            else:
                logger.info("iqyall 当前area:%s", area)
            try:   
            #if True:
                with utils.SimpleFlock("/tmp/area/{0}.lock".format(area), 1):
                    ret = is_valid_area(area)
                    if not ret:
                        logger.info("iqyall area:%s 不可用", area)
                        return
                    ret,task_id = g_taskallot.allot_by_priority(
                        vm_id, area, None,13)
                    print "===== get task ======:", task_id
                    if not ret or task_id is None:
                        logger.warn(
                            utils.auto_encoding('''虚拟机:%d 没有non zero任务可运行,
                                分配显示任务'''), vm_id)
                        return False
                    return True


            except Exception, e:
                logger.error('exception on area lock', exc_info=True)
                time.sleep(2)
                return False
                
        else:
            logger.warn(
                utils.auto_encoding('''虚拟机:%d
                    当前运行任务数:%d>=%d'''),
                vm_id, count, g_pb)
            return False
    else:
        logger.info(
            utils.auto_encoding("当前虚拟机:%d,已分配任务或有正在执行的任务"),
            vm_id)
        return False

def iqy_business(vm_id):
    #iqyall_business(vm_id)
    sql = '''select a.vm_id from vm_cur_task a where a.server_id=%d and
    a.vm_id=%d and a.status in(1,-1,2) '''
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    sqltmp = sql % (g_serverid, vm_id)
    res = dbutil.select_sql(sqltmp)
    print sqltmp
    print res
    if not res:
        print "in heer"
        sqltmp = sql_count % (g_serverid, vm_id)
        res = dbutil.select_sql(sqltmp)
        count = 0
        print res
        if res:
            count = res[0][0]
        logger.warn("running task vm:%d,count:%d", vm_id, count)
        if count < g_pb:
            area = get_iqy_valid_area(vm_id)
            print "area:",area
            #area = '58'
            if not area:
                return False
            else:
                logger.info("当前area:%s", area)
            try:   
            #if True:
                with utils.SimpleFlock("/tmp/area/{0}.lock".format(area), 1):
                    ret = is_valid_area(area)
                    if not ret:
                        logger.info("area:%s 不可用", area)
                        return
                    ret,task_id = g_taskallot.allot_by_priority(
                        vm_id, area, None,11)
                    print "===== get task ======:", task_id
                    if not ret or task_id is None:
                        logger.warn(
                            utils.auto_encoding('''虚拟机:%d 没有non zero任务可运行,
                                分配显示任务'''), vm_id)
                        return False
                    return True


            except Exception, e:
                logger.error('exception on area lock', exc_info=True)
                time.sleep(2)
                return False
                
        else:
            logger.warn(
                utils.auto_encoding('''虚拟机:%d
                    当前运行任务数:%d>=%d'''),
                vm_id, count, g_pb)
            return False
    else:
        logger.info(
            utils.auto_encoding("当前虚拟机:%d,已分配任务或有正在执行的任务"),
            vm_id)
        return False


def vm_business(vm_id):
    sql = '''select a.vm_id from vm_cur_task a where a.server_id=%d and
    a.user_type!=99 and a.vm_id=%d and a.status in(1,-1,2) '''
    sql_count = "select count(1) from vm_cur_task where server_id=%d and vm_id=%d and status in(1,-1,2)"
    sqltmp = sql % (g_serverid, vm_id)
    # print sqltmp
    # logger.debug(sqltmp)
    res = dbutil.select_sql(sqltmp)
    if not res:
        sqltmp = sql_count % (g_serverid, vm_id)
        res = dbutil.select_sql(sqltmp)
        count = 0
        print res
        if res:
            count = res[0][0]
        # logger.warn("running task vm:%d,count:%d", vm_id, count)
        if count < g_pb:
            print "normal herre"
            area= get_valid_area()
            #area = '94'
            if area == -1:
                logger.info("没有可用非0任务的area")
                area = get_valid_zero_area()
                if area == -1:
                    logger.info("没有可用0跑任务的area")
                else:
                    with utils.SimpleFlock("/tmp/area/{0}.lock".format(area), 1):
                        ret = is_valid_zero_area(area)
                        if not ret:
                            logger.info("0area:%s 不可跑零跑", area)
                            return
                        ret,task_id = g_taskallot.allot_default( vm_id,area)
                        logger.info("get default task:%d", task_id)
                        logger.info("当前零跑area:%s", area)
                return
            else:
                logger.info("当前area:%s", area)
                
            try:   
            #if True:
                with utils.SimpleFlock("/tmp/area/{0}.lock".format(area), 1):
                    ret = is_valid_area(area)
                    if not ret:
                        logger.info("area:%s 不可用", area)
                        return
                    single_gid = is_run_as_single(area)
                    if single_gid: 
                        s_gids = single_gid.split(',')
                        sgid = choice(s_gids)
                    else:
                        sgid = 0
                    print "the single_gid:", sgid
                    ret,task_id = g_taskallot.allot_by_priority(
                        vm_id, area, int(sgid))
                    print "===== get task ======:", task_id
                    if not ret or task_id is None:
                        logger.warn(
                            utils.auto_encoding('''虚拟机:%d 没有non zero任务可运行,
                                分配显示任务'''), vm_id)

                        ret = is_valid_zero_area(area)
                        if not ret:
                            logger.info("0area:%s 不可跑零跑", area)
                            return
                        
                        ret,task_id = g_taskallot.allot_default( vm_id,area)
                        logger.info("get default task:%d", task_id)
                        
                        # print "get default task:", task_id
            except Exception, e:
                logger.error('exception on area lock', exc_info=True)
                time.sleep(2)
                
        else:
            logger.warn(
                utils.auto_encoding('''虚拟机:%d
                    当前运行任务数:%d>=%d'''),
                vm_id, count, g_pb)
    else:
        logger.info(
            utils.auto_encoding("当前虚拟机:%d,已分配任务或有正在执行的任务"),
            vm_id)

def main_loop():
    # 获取运行状态,请求运行的vm
    global g_rcv
    vm_names, vm_ids = vms.get_vms(g_serverid)
    # vm_ids = [1,2]
    # vm_names = ['w1', 'w2']
    while True:
        try:
            #g_rcv = is_run_as_single()
            #print "gcv:", g_rcv
            #if not g_rcv:
            print "==============================================="
            for i in range(0, len(vm_ids)):
                vm_id = vm_ids[i]
                if can_iqyatv_run():
                    ret = iqyatv_business(vm_id)
                    if not ret:
                        print "here0"
                        if can_iqy_run():
                            print "here1"
                            ret = iqy_business(vm_id)
                            print "here1", ret
                            if not ret:
                                vm_business(vm_id)
                        else:
                            vm_business(vm_id)
                elif can_iqy_run():
                    print "here2"
                    ret = iqy_business(vm_id)
                    if not ret:
                        vm_business(vm_id)
                else:
                    print "here3"
                    vm_business(vm_id)
            #else:
            #    vm_id = choice(vm_ids)
            #    vm_business(vm_id)
            time.sleep(3)
        except:
            logger.error('exception on main_loop', exc_info=True)
            time.sleep(3)


def init():
    parser = optparse.OptionParser()
    parser.add_option(
        "-v",
        "--vManagePath",
        dest="vpath",
        default='D:\CMac\VBox-x64',
        help="vboxmanage path, default is d:\cmac\vbox-x64")

    parser.add_option(
        "-i",
        "--ip",
        dest="db_ip",
        default="192.168.1.21",
        help="mysql database server IP addrss, default is 192.168.1.21")
    parser.add_option(
        "-n",
        "--name",
        dest="db_name",
        default="vm4",
        help="database name, default is vm")
    parser.add_option(
        "-u",
        "--usrname",
        dest="username",
        default="vm",
        help="database login username, default is vm")
    parser.add_option(
        "-p",
        "--password",
        dest="password",
        default="123456",
        help="database login password, default is 123456")
    parser.add_option(
        "-l",
        "--logconf",
        dest="logconf",
        default="./vm-scheduler.log.conf",
        help="log config file, default is ./scheduler.log.conf")
    parser.add_option(
        "-r",
        "--reset",
        dest="reset",
        default="0",
        help="reset all vm to initail status  default is 0")
    parser.add_option(
        "-b",
        "--paraell browser",
        dest="pb",
        default="1",
        help="paraell running browser  default is 0")
    parser.add_option(
        "-m",
        "--serverid",
        dest="serverid",
        default="0",
        help="the server id,default is  0")
    parser.add_option(
        "-w",
        "--want_init",
        dest="winit",
        default="0",
        help="init taskgroup,default is false")
    parser.add_option(
        "-k",
        "--reset_waittime",
        dest="rwt",
        default="120",
        help="reset waiting time,default is 120s")
    parser.add_option(
        "-a",
        "--radmon choice vm",
        dest="rcv",
        default="0",
        help="radmon choice vm defaul is 0(false)")
    (options, args) = parser.parse_args()
    global g_current_dir, g_reset
    global g_serverid, g_rto, g_reset_waittime, g_pb, g_rcv
    g_rcv = int(options.rcv)
    g_serverid = int(options.serverid)
    if g_serverid == 0:
        print "serverid is 0,exit"
        sys.exit(1)
    g_reset_waittime = int(options.rwt)
    g_pb = int(options.pb)
    g_want_init_task = int(options.winit)
    # g_vManager_path='C:\Program Files\Oracle\VirtualBox'
    g_reset = int(options.reset)
    g_current_dir = os.getcwd()
    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info(options)

    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger

    # global g_vpn_db
    # g_vpn_db = DBUtil(logger,options.db_ip,3306, "vpntest", options.username, options.password,'utf8')
    #启动时的时间跟设置时间不一致,关机开关开启
    cur_hour = get_cur_hour()
    global g_taskallot, g_logtask, g_task_profile, g_pc, g_user, g_userec, g_user7
    # task.taskallot.logger = logger
    task.parallel.logger = logger
    g_pc = ParallelControl(g_serverid, dbutil, logger)
    g_user = UserAllot(g_serverid, g_pc, dbutil, logger)
    g_userec = UserAllot_EC(g_serverid, g_pc, dbutil, logger)
    g_userrest = UserAllot_Rest(g_serverid, g_pc, dbutil, logger)
    g_user7= UserAllot7(g_serverid, g_pc, dbutil, logger)
    g_user_reg= UserAllot_Reg(g_serverid, g_pc, dbutil, logger)
    g_user_iqy = UserAllot_IQY(g_serverid, g_pc, dbutil, logger)
    g_user_iqyall = UserAllot_IQYALL(g_serverid, g_pc, dbutil, logger)
    g_user_iqyatv = UserAllot_IQYATV(g_serverid, g_pc, dbutil, logger)
    g_taskallot = TaskAllot(g_want_init_task, g_serverid, g_pc, g_user,
            g_userec, g_user7,g_userrest,g_user_reg,g_user_iqy,g_user_iqyatv,g_user_iqyall,dbutil, logger)
    # g_taskallot = TaskAllotRolling(g_want_init_task, g_serverid, g_pc, g_user, dbutil,
            # logger)
    g_logtask = LogTask(dbutil, logger)
    # g_task_profile = TaskProfile(g_serverid, dbutil, g_pc, logger)

    vms.logger = logger
    vms.dbutil = dbutil

    vm_utils.logger = logger
    vm_utils.dbutil = dbutil
    vm_utils.g_vManager_path = options.vpath
    vm_utils.g_current_dir = os.getcwd()
    return True



def main():
    try:
        init()
        print "to main loop"
        main_loop()
    except (KeyboardInterrupt, SystemExit):
        print("exit system,start to shut down all vm...")
        time.sleep(10)
        exit(0)


if __name__ == "__main__":
    main()

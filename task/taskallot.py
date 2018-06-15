#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : taskallot.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 07.04.2018 18:14:1523096068
# Last Modified Date: 31.05.2018 10:43:1527734585
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
'''
@Author: coldplay
@Date: 2017-05-13 16:32:04
@Last Modified by:   coldplay
@Last Modified time: 2017-05-13 16:32:04
'''

import sys
import datetime
import time
import logging
import logging.config
from taskgroup import TaskGroup
import dbutil
import utils
from task import TaskError
from parallel import ParallelControl
sys.path.append("..")


class TaskAllotError(Exception):
    pass


class TaskAllot(object):
    '''任务分配'''
    logger = None

    def __init__(self, want_init, server_id, pc, user, user_ec, user7, db, logger):
        self.db = db
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id
        self.selected_ids = []
        self.pc = pc
        self.user = user
        self.user_ec = user_ec
        self.user7 = user7
        self.logger = logger
        self.task_group = TaskGroup(db)
        # self.lock = utils.Lock("/tmp/lock-sched.lock")

    def log_task_id(self, id, task_id):
        sql = "update vm_allot_task set cur_task_id=%d where id=%d" % (task_id,
                                                                       id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise TaskAllotError, "%s excute error;ret:%d" % (sql, ret)

    def reset_when_newday(self):
        '''新的一天重置所有运行次数'''
        today = datetime.date.today()
        print today, self.cur_date
        if today != self.cur_date:
            self.cur_date = today

            # #统一到一个w = 1的进程进行更新
            # if self.want_init == 1:
            # print "start new day to reinit..."
            # #self.logger.info("start new day to reinit...")
            # TaskGroup.reset_rantimes_today(self.db)
            # TaskGroup.reset_rantimes_allot_impl(self.db)
            # self.cur_date = today
            # print "cur_date", self.cur_date
            # print "end new day to reinit..."
            #self.logger.info("end new day to reinit...")
    def wait_interval(self, gid):
        sql = '''select 1 from vm_cur_task where 
        inter_time>0 and task_group_id=%d and ( 
        update_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        or start_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        or succ_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        ) and start_time>current_date''' % (gid, 5, 5, 5)
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if res and len(res) >= 1:
            return True
        return False

    def get_band_interval_groupids(self):
        '''get inter task which running or ran in 5mins
        '''
        group_ids = []
        sql = '''select task_group_id from vm_cur_task where 
        inter_time>0 and ( 
        update_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        or start_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        or succ_time>=DATE_SUB(NOW(),INTERVAL %d MINUTE)
        ) and start_time>current_date''' % (5, 5, 5)
        self.logger.debug(sql)
        res = dbutil.select_sql(sql)
        for r in res:
            id = r[0]
            group_ids.append(id)

        return group_ids

    def get_band_run_groupids(self):
        '''获取运行状态的任务组
        '''
        group_ids = []
        sql = '''select task_group_id from vm_cur_task where server_id=%d and
           status in(-1,1) and task_group_id !=0 and start_time>current_date
           ''' % (self.server_id)
        self.logger.debug(sql)
        res = dbutil.select_sql(sql)
        for r in res:
            id = r[0]
            #并行数爆了,才加入band group
            if self.pc.is_ran_out_parallel_num(id):
                group_ids.append(id)
        print 'band group_ids:', group_ids
        # 任务多时会导致本可运行运行
        # inter_group_ids = self.get_band_interval_groupids()
        # print 'inter_group_ids:',inter_group_ids
        # group_ids.extend(inter_group_ids)
        # print "extend groupid", group_ids

        pout_ids_set = self.pc.get_ran_out_parallel_task_set()
        print pout_ids_set
        return set(group_ids) | pout_ids_set

    def vpn_update_time(self):
        sql = "select update_time,ip,area from vpn_status where serverid=%d and vpnstatus=1 " % (
            self.server_id)
        res = dbutil.select_sql(sql)
        if res:
            update_time = res[0][0]
            ip = res[0][1]
            area = int(res[0][2])
            print update_time
            return update_time, ip, area
        return None, None,None

    def vm_last_succ_time(self, task_group_id):
        sql = "select max(succ_time) from vm_cur_task where server_id=%d and task_group_id=%d and status>=2" % (
            self.server_id, task_group_id)
        res = dbutil.select_sql(sql)
        if res:
            return res[0][0]
        return '1970-1-1 00:00:00'

    def task_last_succ_time(self, task_id):
        sql = '''select max(succ_time) from vm_cur_task where server_id=%d and
        cur_task_id=%d and status>=2''' % (self.server_id, task_id)
        res = dbutil.select_sql(sql)
        if res:
            return res[0][0]
        return '1970-1-1 00:00:00'

    def gen_rand_minutes(self, standby_time):
        standby_time_arr = standby_time.split(",")
        print "time_arr", standby_time_arr
        stimes = map(int, standby_time_arr)
        if len(stimes)==1:
            stimes.append(stimes[0])
        randtime = random.randint(stimes[0],stimes[1])
        print "rantime",randtime
        return randtime

    def task_interval_setting(self, task_id):
        sql = '''select interval_times,interval_min from vm_task where
        id={0}'''.format(task_id)
        res = dbutil.select_sql(sql)
        if res:
            times= res[0][0]
            minutes = res[0][1]
            ran_min = self.gen_rand_minutes(minutes)
            return times,ran_min
        return None,None
        
    def task_interval_info(self, task_id):
        sql = '''select times,cur_times,minutes from vm_task_interval where
        id={0}'''.format(task_id)
        res = dbutil.select_sql(sql)
        if res:
            times= res[0][0]
            cur_times= res[0][1]
            minutes = res[0][2]
            return times,cur_times,minutes
        return None,None

    def reset_task_interval(self, task_id):
        ran_min = self.gen_rand_minutes(minutes)
        sql = '''update vm_task_interval set
        cur_times=0,minutes={0}'''.format(ran_min)
        ret = dbutil.execute_sql(sql)
        if ret<0:
            logger.info("sql:%s exec failed %d", sql, ret)
    
    def log_task_interval_times(self, task_id):

        times, minutes = self.task_interval_setting(task_id)
        sql = ''' insert into vm_task_interval (task_id,
                times,cur_times,minutes)
        values({0},{1},1,{3}) on duplicate key update cur_times=cur_times+1'''.format(
                task_id, times, minutes)

        # sql = '''update vm_task_interval set
        # cur_times=cur_times+1 where task_id=%d'''.format(task_id)
        ret = dbutil.execute_sql(sql)
        if ret<0:
            logger.info("sql:%s exec failed %d", sql, ret)


    def task_interval(self, task_id, stime):
        times, cur_times, minutes = self.task_interval_info(task_id)
        if cur_time < times:
            return False
        now = datetime.datetime.now()
        if now-stime>minutes*60:
            self.reset_task_interval(task_id)
            return False
        return True
            
        
    def right_to_allot_zero(self, task_id):
        #return True
        succ_time = self.task_last_succ_time(task_id)
        if succ_time is None:
            succ_time = '1970-1-1 00:00:00'
        redial_time, ip, area = self.vpn_update_time()
        self.logger.info("task_id:%d,last_succ_time:%s, redial_time:%s",
                         task_id, succ_time, redial_time)
        rtime, stime = None, None
        if redial_time:
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")

            if stime < rtime:
                return True, area
            else:
                self.logger.warn("task_id:%d succ_time>=redial_time", task_id)
        return False, None

    def right_to_allot(self, task_group_id):
        #return True
        succ_time = self.vm_last_succ_time(task_group_id)
        if succ_time is None:
            succ_time = '1970-1-1 00:00:00'
        redial_time, ip, area = self.vpn_update_time()
        self.logger.info("task_group_id:%d,last_succ_time:%s, redial_time:%s",
                         task_group_id, succ_time, redial_time)
        rtime, stime = None, None
        if redial_time:
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")

            if stime < rtime:
                return True, area
            else:
                self.logger.warn("task_group_id:%d succ_time>=redial_time",
                                 task_group_id)
            # if self.task_interval(task_id, stime):
                # return False
        return False,None

    def get_candidate_gid(self, vm_id, type=1):
        type_str = ">"
        if type == 0:
            type_str = ">"
        else:
            type_str = "="
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
                    AND b.id > 0
                    AND c.task_group_id = a.id
                    and b.priority %s 0
                    AND f.server_id = %d ''' % (type_str, self.server_id)
        if type == 0:
            sql = sql + " order by b.priority"
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        ids = set()
        rid_set = self.get_band_run_groupids()
        if type == 0:
            if res and len(res)>0:
                print "get pri task:", res[0][0]
                if res[0][0] not in rid_set:
                    print "append single task:", res[0][0]
                    self.selected_ids.append(res[0][0])
                    print self.selected_ids
            return 
        for r in res:
            ids.add(r[0])
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)

        self.selected_ids = list(set(ids) - rid_set)
        print self.selected_ids

    def get_candidate_gid2(self, vm_id):
        sql = '''SELECT
                        distinct b.id
                    FROM
                        vm_task_group b,
                        vm_allot_task_by_servergroup c,
                        vm_task d,
                        vm_server_group f
                    WHERE
                        d.id = b.task_id
                    AND d.STATUS = 1
                    AND f.id = c.server_group_id
                    and f.status =1
                    AND c.task_group_id = b.id
                    AND b.ran_times < b.times
                    AND b.id > 0
                    AND f.server_id = %d order by b.id''' % (self.server_id)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        ids = set()
        for r in res:
            ids.add(r[0])
        rid_set = self.get_band_run_groupids()
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)
        self.selected_ids = list(set(ids) - rid_set)

    def allot_by_default(self, vm_id, uty):
        self.logger.info("start to allot default task,%d", uty)
        task = TaskGroup.getDefaultTask(self.db, self.server_id, vm_id, uty)
        if not task:
            self.logger.warn("no default task to run uty:%d", uty)
            return None
        ret, area = self.right_to_allot_zero(task.id)
        if not ret:
            self.logger.warn("wait for vpn dial...")
            return None
        ret = self.user.allot_user(vm_id, 0, task.id, area)
        if not ret:
            self.logger.warn(
                "vm_id:%d,task_id:%d,task_group_id:%d no user to run", vm_id,
                task.id, 0)
            return None
        return task

    def allot_by_priority(self, vm_id):
        try:
            task, gid, ret = None, 0, True
            task = self.allot_by_default(vm_id, 0)
            #if not task:
            #    ret = False
            #    task = self.allot_by_default(vm_id, 6)
            #360
            if not task:
                ret = False
                task = self.allot_by_default(vm_id, 1)
            #58
            if not task:
                ret = False
                task = self.allot_by_default(vm_id, 7)
            if not task:
                ret = False
                self.reset_when_newday()
                self.get_candidate_gid(vm_id, 1)
                self.get_candidate_gid(vm_id, 0)
                # self.get_candidate_gid2(vm_id)
                print "selected ids:", self.selected_ids
                while self.selected_ids:
                    ret = False
                    gid = self.selected_ids.pop()
                    self.logger.info(
                        "====================handle gid:%d====================",
                        gid)
                    try:
                        with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                            # 放在里面否则可能出现多个任务不按间隔时间跑
                            if self.wait_interval(gid):
                                self.logger.info("gid:%d should wait 5 mins",
                                                 gid)
                                continue
                            ret, area = self.right_to_allot(gid)
                            if ret:
                                self.logger.info("get valid gid:%d", gid)
                            else:
                                # self.logger.warn("wait for redial:%d", gid)
                                continue

                            task = self.handle_taskgroup(gid, vm_id, area)
                            if task:
                                self.logger.info("get the task:%d", task.id)
                                ret = True
                                self.add_ran_times(task.id, gid, task.rid)
                                break
                            else:
                                continue
                    except Exception, e:
                        self.logger.error('exception on lock', exc_info=True)
                        self.logger.info("exception in lock, timeout")
                        print "exception in lock", e
                        continue
            else:
                ret = True
                self.add_ran_times(task.id, gid, task.rid)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        return ret

    def get_task_type(self, task_id):
        sql = '''select user_type,is_ad from vm_task where id=%d ''' % (
            task_id)
        res = self.db.select_sql(sql)
        if not res:
            return None,None
        return res[0][0],res[0][1]

    def handle_taskgroup(self, task_group_id, vm_id, area):
        # tg = TaskGroup(task_group_id, self.db)

        task = self.task_group.choose_vaild_task(self.server_id,
                task_group_id)
        if not task:
            return None
        self.logger.warn("==========get the valid task:%d==========", task.id)
        uty, is_ad = self.get_task_type(task.id)
        self.logger.info("task uty:%d, is_ad:%d", uty, is_ad)
        if is_ad:
            #广告专享
            ret = self.user7.allot_user(vm_id, task_group_id, task.id, area)
        elif uty == 6:
            ret = self.user_ec.allot_user(vm_id, task_group_id, task.id)
        else:
            ret = self.user.allot_user(vm_id, task_group_id, task.id, area)
        print "the allot user ret", ret
        if not ret:
            self.logger.warn(
                "vm_id:%d,task_id:%d,task_group_id:%d no user to run", vm_id,
                task.id, task_group_id)
            return None

        return task

    def add_zero_limit_times(self, id):
        sql = "update zero_schedule_list set ran_times=ran_times+1 where id=%d" % (
            id)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s excute error;ret:%d" % (sql, ret)

    def add_ran_times(self, task_id, task_group_id, rid):
        ''' 分配成功后有可用profile 时计数
        '''
        self.logger.warn("task_id:%d, task_group_id:%d", task_id,
                task_group_id)
        # tg = TaskGroup(task_group_id, self.db)
        if task_group_id == 0:
            self.task_group.add_ran_times2(task_id, task_group_id)
            self.add_zero_limit_times(rid)
            # TaskGroup.add_default_ran_times(self.db)
        else:
            # 只更新impl的值得,不更新group,(group由脚本更新)
            #     tg.add_ran_times(task_id)
            # tg.add_impl_ran_times(task_id)
            # self.log_task_interval_times(task_id)
            self.task_group.add_impl_ran_times(task_id)


def allot_test(dbutil):
    '''任务分配测试
    '''
    task_group_id = None
    print len(sys.argv)
    if len(sys.argv) > 1:
        task_group_id = int(sys.argv[1])
        print task_group_id
        TaskGroup.reset_rantimes_by_task_group_id(dbutil, task_group_id)
        # while True:
        #     t.allot_by_priority("d:\\10.bat")
        #     # t.allot_by_rand("d:\\10.bat")
        #     time.sleep(1)
        TaskGroup.reset_rantimes_allot_impl(dbutil, task_group_id)
    t = TaskAllot(1, 1, None, dbutil)
    if task_group_id:
        exit(0)
    while True:
        try:
            t.reset_when_newday()
            time.sleep(10)
        except Exception, e:
            print "except", e
            time.sleep(5)
            continue


def getTask(dbutil, logger):
    from rolling_user import UserAllot
    pc = ParallelControl(11, dbutil, logger)
    user = UserAllot(11, pc, dbutil, logger)
    t = TaskAllot(0, 11, pc, user, None,None, dbutil, logger)

    # t.allot_by_default(2, 0)
    # t.allot_by_default(2, 7)
    # t.allot_by_default(2, 1)
    #t.allot_by_nine(1)
    while True:
        ret = t.allot_by_priority(5)
        print ret
        time.sleep(5)
        # break


def get_default_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # console logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    # dbutil.db_host = "3.3.3.6"
    # dbutil.db_name = "vm3"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    getTask(dbutil, logger)

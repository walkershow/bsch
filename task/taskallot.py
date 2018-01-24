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
from task import TaskError
from parallel import ParallelControl
sys.path.append("..")

logger = None


class TaskAllotError(Exception):
    pass


class TaskAllot(object):
    '''任务分配'''

    def __init__(self, want_init, server_id, pc, db):
        self.db = db
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id
        self.selected_ids = []
        self.pc = pc

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

            #统一到一个w = 1的进程进行更新
            if self.want_init == 1:
                print "start new day to reinit..."
                #logger.info("start new day to reinit...")
                TaskGroup.reset_rantimes_today(self.db)
                TaskGroup.reset_rantimes_allot_impl(self.db)
                self.cur_date = today
                print "cur_date", self.cur_date
                print "end new day to reinit..."
                #logger.info("end new day to reinit...")

    def get_band_run_groupids(self):
        '''获取运行状态的任务组
        '''
        group_ids = []
        sql = "select task_group_id from vm_cur_task where server_id=%d and status in(-1,1) and task_group_id !=0 " % (
            self.server_id)
        logger.debug(sql)
        res = dbutil.select_sql(sql)
        for r in res:
            id = r[0]
            #并行数爆了,才加入band group
            if self.pc.is_ran_out_parallel_num(id):
                group_ids.append(id)
        print group_ids

        pout_ids_set = self.pc.get_ran_out_parallel_task_set()
        print pout_ids_set
        return set(group_ids) | pout_ids_set

    def vpn_update_time(self):
        sql = "select update_time,ip from vpn_status where serverid=%d and vpnstatus=1 " % (
            self.server_id)
        res = dbutil.select_sql(sql)
        if res:
            update_time = res[0][0]
            ip = res[0][1]
            print update_time
            return update_time, ip
        return None, None

    def vm_last_succ_time(self, task_group_id):
        sql = "select max(succ_time) from vm_cur_task where server_id=%d and task_group_id=%d and status>=2" % (
            self.server_id, task_group_id)
        res = dbutil.select_sql(sql)
        if res:
            return res[0][0]
        return '1970-1-1 00:00:00'

    def right_to_allot(self, task_group_id):
        succ_time = self.vm_last_succ_time(task_group_id)
        if succ_time is None:
            succ_time = '1970-1-1 00:00:00'
        redial_time, ip = self.vpn_update_time()
        logger.info("task_group_id:%d,last_succ_time:%s, redial_time:%s",
                    task_group_id, succ_time, redial_time)
        rtime, stime = None, None
        if redial_time:
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")

            if stime < rtime:
                return True
            else:
                logger.warn("task_group_id:%d succ_time>=redial_time",
                            task_group_id)
        return False

    def get_valid_gid(self, get_default):
        if get_default:
            return 0
        while self.selected_ids:
            gid = self.selected_ids.pop()
            if self.right_to_allot(gid):
                logger.info("get valid gid:%d", gid)
                return gid
        return 0

    def allot_by_priority(self, vm_id, get_default):
        try:
            self.reset_when_newday()
            if self.selected_ids:
                gid = self.get_valid_gid(get_default)
                task = self.handle_taskgroup(gid, vm_id)
                if task is None:
                    return None, None, None
                return task.id, gid, task.rid

            sql = '''SELECT
                            a.id
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
                        and b.priority>0
                        AND f.server_id = %d order by b.priority''' % (
                self.server_id)

            res = self.db.select_sql(sql)
            ids = set()
            for r in res:
                ids.add(r[0])
            rid_set = self.get_band_run_groupids()
            band_str = ",".join(str(s) for s in rid_set)
            logger.info("band task_group_id:%s", band_str)

            self.selected_ids = list(set(ids) - rid_set)
            print self.selected_ids
            gid = self.get_valid_gid(get_default)

            if gid == 0:
                #不存在优先级高的任务组,执行随机分配
                logger.info("no priority task, get rand taskgroup")
                return self.allot_by_rand(vm_id, get_default)
            else:
                task = self.handle_taskgroup(gid, vm_id)
                if task is None:
                    return None, None, None
                return task.id, gid, task.rid
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)

    def allot_by_rand(self, vm_id, get_default):
        try:
            sql = '''SELECT
                         a.id
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
                     and b.priority=0
                     AND f.server_id = %d''' % (self.server_id)

            res = self.db.select_sql(sql)
            ids = []
            for r in res:
                ids.append(r[0])
            rid_set = self.get_band_run_groupids()
            # print "band run set", rid_set
            band_str = ",".join(str(s) for s in rid_set)
            logger.info("band task_group_id:%s", band_str)
            self.selected_ids = list(set(ids) - rid_set)
            self.selected_ids.sort()
            # print self.selected_ids

            task = None
            task_group_id = self.get_valid_gid(get_default)
            if task_group_id == 0:
                logger.warn("no else task to run,find default taskgroup")
                task = TaskGroup.getDefaultTask(self.db, self.server_id, vm_id)
                if not task:
                    logger.warn("no default task to run")
                    return None, None, None
                # print task
                task.allot2()
            else:
                task = self.handle_taskgroup(task_group_id, vm_id)
            return task.id, task_group_id, task.rid
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)

    def handle_taskgroup(self, task_group_id, vm_id):
        tg = TaskGroup(task_group_id, self.db)
        task = tg.choose_vaild_task(self.server_id, vm_id)
        if not task:
            return None
        task.allot2()

        return task

    def add_zero_limit_times(self, id):
        sql = "update zero_schedule_list set ran_times=ran_times+1 where id=%d" % (
            id)
        print sql
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s excute error;ret:%d" % (sql, ret)

    def add_ran_times(self, task_id, task_group_id, rid):
        ''' 分配成功后有可用profile 时计数
        '''
        if task_group_id == 0:
            tg = TaskGroup(task_group_id, self.db)
            tg.add_ran_times(task_id)
            self.add_zero_limit_times(rid)
            # TaskGroup.add_default_ran_times(self.db)
        #成功时才技术,放在ad_stat接口了
        # else:
        #     tg.add_ran_times(task_id)
        #     tg.add_impl_ran_times(task_id)


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


def getTask(dbutil):
    pc = ParallelControl(18, dbutil, logger)
    t = TaskAllot(0, 1, pc, dbutil)
    while True:
        t.allot_by_priority("d:\\10.bat", False)
        time.sleep(5)


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
    global logger
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    getTask(dbutil)

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

logger = None


class TaskAllotError(Exception):
    pass


class TaskAllot(object):
    '''任务分配'''

    def __init__(self, want_init, server_id, pc, user, db):
        self.db = db
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id
        self.selected_ids = []
        self.pc = pc
        self.user = user
        self.lock = utils.Lock("/tmp/lock-sched.lock")

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
            self.user.clear_cache()
            self.cur_date = today

            # #统一到一个w = 1的进程进行更新
            # if self.want_init == 1:
            # print "start new day to reinit..."
            # #logger.info("start new day to reinit...")
            # TaskGroup.reset_rantimes_today(self.db)
            # TaskGroup.reset_rantimes_allot_impl(self.db)
            # self.cur_date = today
            # print "cur_date", self.cur_date
            # print "end new day to reinit..."
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

    def task_last_succ_time(self, task_id):
        sql = '''select max(succ_time) from vm_cur_task where server_id=%d and
        cur_task_id=%d and status>=2''' % (self.server_id, task_id)
        res = dbutil.select_sql(sql)
        if res:
            return res[0][0]
        return '1970-1-1 00:00:00'

    def right_to_allot_zero(self, task_id):
        #return True
        succ_time = self.task_last_succ_time(task_id)
        if succ_time is None:
            succ_time = '1970-1-1 00:00:00'
        redial_time, ip = self.vpn_update_time()
        logger.info("task_id:%d,last_succ_time:%s, redial_time:%s", task_id,
                    succ_time, redial_time)
        rtime, stime = None, None
        if redial_time:
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")

            if stime < rtime:
                return True
            else:
                logger.warn("task_id:%d succ_time>=redial_time", task_id)
        return False

    def right_to_allot(self, task_group_id):
        #return True
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

    def acquired_allot_priv(self):
        print "lock"
        self.lock.acquire()

    def release_allot_priv(self):
        self.lock.release()

    def get_candidate_gid(self, vm_id, get_default, type):
        type_str = ">"
        if type == 0:
            type_str = ">"
        else:
            type_str = "="
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
                    and b.priority %s 0
                    AND f.server_id = %d ''' % (type_str, self.server_id)
        if type == 0:
            sql = sql + " order by b.priority"
        logger.info(sql)
        res = self.db.select_sql(sql)
        ids = set()
        for r in res:
            ids.add(r[0])
        rid_set = self.get_band_run_groupids()
        band_str = ",".join(str(s) for s in rid_set)
        print("band task_group_id:%s" % (band_str))
        # logger.info("band task_group_id:%s", band_str)

        self.selected_ids = list(set(ids) - rid_set)
        print self.selected_ids

    def allot_by_type(self, vm_id, get_default, type):
        gid = self.get_valid_gid(get_default)
        return gid

    def allot_by_default(self, vm_id):
        logger.info("allot default task")
        task = TaskGroup.getDefaultTask(self.db, self.server_id, vm_id)
        if not task:
            logger.warn("no default task to run")
            return None
        if not self.right_to_allot_zero(task.id):
            logger.warn("wait for vpn dial...")
            return None
        ret = self.user.allot_user(vm_id, 0, task.id)
        if not ret:
            logger.warn("vm_id:%d,task_id:%d,task_group_id:%d no user to run",
                        vm_id, task.id, 0)
            return None
        return task

    def allot_by_priority(self, vm_id, get_default):
        try:
            task, gid, ret = None, None, False
            got_task = False
            if get_default:
                task = self.allot_by_default(vm_id)
                gid = 0
            else:
                self.acquired_allot_priv()
                self.reset_when_newday()
                self.get_candidate_gid(vm_id, get_default, 1)
                print "selected ids:", self.selected_ids
                while self.selected_ids:
                    gid = self.selected_ids.pop()
                    print "handle gid:", gid
                    if self.right_to_allot(gid):
                        logger.info("get valid gid:%d", gid)
                    else:
                        logger.warn("wait for redial:%d", gid)
                        continue
                    task = self.handle_taskgroup(gid, vm_id)
                    if task:
                        logger.info("get the task:%d", task.id)
                        got_task = True
                        break
                    else:
                        continue

                if not got_task:
                    logger.warn('''no else task to run,find default
                            taskgroup''')
                    task = self.allot_by_default(vm_id)
                    if not task:
                        ret = False
                    else:
                        ret = True
                else:
                    ret = True
                    self.add_ran_times(task.id, gid, task.rid)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        # finally:
        self.release_allot_priv()
        return ret

    def handle_taskgroup(self, task_group_id, vm_id):
        tg = TaskGroup(task_group_id, self.db)
        task = tg.choose_vaild_task(self.server_id, vm_id)
        if not task:
            return None
        logger.warn("==========get the valid task==========")
        ret = self.user.allot_user(vm_id, task_group_id, task.id)
        print "the allot user ret", ret
        if not ret:
            logger.warn("vm_id:%d,task_id:%d,task_group_id:%d no user to run",
                        vm_id, task.id, task_group_id)
            return None

        return task

    def add_zero_limit_times(self, id):
        sql = "update zero_schedule_list set ran_times=ran_times+1 where id=%d" % (
            id)
        logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s excute error;ret:%d" % (sql, ret)

    def add_ran_times(self, task_id, task_group_id, rid):
        ''' 分配成功后有可用profile 时计数
        '''
        print "add_ran_times"
        tg = TaskGroup(task_group_id, self.db)
        if task_group_id == 0:
            print "task_group_id is 0"
            tg.add_ran_times(task_id)
            self.add_zero_limit_times(rid)
            # TaskGroup.add_default_ran_times(self.db)
        else:
            # 只更新impl的值得,不更新group,(group由脚本更新)
            #     tg.add_ran_times(task_id)
            tg.add_impl_ran_times(task_id)


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
    from user import UserAllot
    pc = ParallelControl(11, dbutil, logger)
    user = UserAllot(11, pc, dbutil, logger)
    t = TaskAllot(11, pc, user, dbutil)

    # t.allot_by_default(5)
    #t.allot_by_nine(1)
    while True:
        ret = t.allot_by_priority(1, False)
        print ret
        time.sleep(5)
        break


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
    #dbutil.db_host = "192.168.1.21"
    dbutil.db_host = "3.3.3.6"
    dbutil.db_name = "vm3"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    getTask(dbutil)

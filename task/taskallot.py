#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : taskallot.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 07.04.2018 18:14:1523096068
# Last Modified Date: 31.05.2018 10:43:1527734585
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# -*- coding: utf-8 -*-
"""
@Author: coldplay
@Date: 2017-05-13 16:32:04
@Last Modified by:   coldplay
@Last Modified time: 2017-05-13 16:32:04
"""

import datetime
import logging
import logging.config
import sys
import time

sys.path.append("..")
import dbutil
import utils
from parallel import ParallelControl
from task import TaskError
from taskgroup import TaskGroup


class TaskAllotError(Exception):
    pass


class TaskAllot(object):
    """任务分配"""

    logger = None

    def __init__(
        self,
        want_init,
        server_id,
        pc,
        user,
        user_ec,
        user7,
        user_rest,
        user_reg,
        user_iqy,
        user_iqyatv,
        user_iqyall,
        db,
        logger,
    ):
        self.db = db
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id
        self.pc = pc
        self.user = user
        self.user_ec = user_ec
        self.user7 = user7
        self.user_rest = user_rest
        self.user_reg = user_reg
        self.user_iqy = user_iqy
        self.user_iqyall = user_iqyall
        self.user_iqyatv = user_iqyatv
        self.logger = logger
        self.task_group = TaskGroup(db)
        self.ranking_dict = {}
        self.selected_ids = []
        self.priority_ids = []
        self.is_iqy = False
        # self.lock = utils.Lock("/tmp/lock-sched.lock")

    def log_task_id(self, id, task_id):
        sql = "update vm_allot_task set cur_task_id=%d where id=%d" % (task_id, id)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise TaskAllotError, "%s excute error;ret:%d" % (sql, ret)

    def reset_when_newday(self):
        """新的一天重置所有运行次数"""
        today = datetime.date.today()
        if today != self.cur_date:
            self.cur_date = today

            # #统一到一个w = 1的进程进行更新
            # if self.want_init == 1:
            # #self.logger.info("start new day to reinit...")
            # TaskGroup.reset_rantimes_today(self.db)
            # TaskGroup.reset_rantimes_allot_impl(self.db)
            # self.cur_date = today
            # self.logger.info("end new day to reinit...")

    def get_taskgroup_lastid(self, gid):
        sql = """select last_id,parralle_times from log_taskgroup_lastid where
        task_group_id={0}""".format(
            gid
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if res and len(res) >= 1:
            return res[0][0], res[0][1]
        return None, None

    def set_taskgroup_lastid(self, id, gid):
        # 默认每个间隔2次
        sql = """ insert into
        log_taskgroup_lastid(task_group_id,last_id,parralle_times)
        values({0},{1},{2}) on duplicate key update last_id={1}""".format(
            gid, id, 2
        )
        self.logger.info(sql)
        ret = dbutil.execute_sql(sql)
        if ret < 0:
            logger.error("set_taskgroup_lastId failed:%d", ret)

    def is_inited_start(self, gid):
        sql = """select max(id),inter_time from vm_cur_task where cur_task_id={0} and
        inter_time>0 and start_time>current_date""".format(
            gid
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if res and len(res) >= 1:
            return res[0][0], res[0][1]
        return None, None

    def wait_interval(self, gid):
        max_id, inter_time = self.is_inited_start(gid)
        print "get max_id:", max_id
        if not max_id:
            return True
        sql = """select 1 from vm_cur_task where id=%d and ( 
        round((UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time))/60)>=%d
        )""" % (
            max_id,
            inter_time,
        )

        self.logger.info(sql)
        res = dbutil.select_sql(sql)

        if res and len(res) >= 1:
            self.set_taskgroup_lastid(max_id, gid)
            return True
        return False

    def can_allot_rest(self, gid):
        # 是否能分配这个类型的剩下次数
        max_id, pt = self.get_taskgroup_lastid(gid)
        if not max_id:
            return False
        sql = """select count(1) from vm_cur_task where id>%d and
        cur_task_id=%d and start_time>current_date""" % (
            max_id,
            gid,
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if res and len(res) >= 1:
            count = res[0][0]
            if count >= pt:
                return False
            return True
        return False

    def get_group_type(self, gid):
        sql = """select type_id from vm_task_group_type where
        task_group_id={0}""".format(
            gid
        )
        self.logger.debug(sql)
        res = dbutil.select_sql(sql)
        if not res:
            return None
        return res[0][0]

    def get_band_run_groupids(self, area):
        """获取运行状态的任务组
        """
        group_ids = []
        type_ids = ()
        sql = """select task_group_id from vm_cur_task where area=%s and
           status in(-1,1,2) and task_group_id !=0 and start_time>current_date
           """ % (
            area
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if not res:
            return set()
        for r in res:
            id = r[0]
            # 并行数爆了,才加入band group
            if self.pc.is_ran_out_parallel_num(id):
                group_ids.append(id)

        pout_ids_set = self.pc.get_ran_out_parallel_task_set()
        return set(group_ids) | pout_ids_set

    def get_dialup_ip(self, area):
        sql = """select ip from vpn_status where area='{0}' and
            vpnstatus=1""".format(
            area
        )
        res = self.db.select_sql(sql)
        if not res or len(res) < 1:
            return None
        r = res[0]
        ip = r[0]
        return ip

    def get_ip_reuse_time(self):
        sql = "select value from vm_sys_dict where `key` = 'ip_reuse_time'"
        res = self.db.select_sql(sql)
        if not res or len(res) < 1:
            return None
        t = res[0][0]
        return t

    def is_repeat_ip(self, area, task_group_id):
        return False
        ip_reuse_time = self.get_ip_reuse_time()  # 取得IP可重用时间
        ip = self.get_dialup_ip(area)
        self.logger.info("ip:%s,reuse time:%s", ip, ip_reuse_time)
        if ip is None or ip_reuse_time is None:
            return True
        # sql = '''select count(*) as c from vm_cur_task where (begin_ip='{0}' or
        # hot_ip='{1}') and area={2} and task_group_id={3} and (status
        # " in(-1,1,2,4) or succ_time is not null) and TIME_TO_SEC(timediff(now(),start_time))<{4}'''.format(ip,ip,area,task_group_id,ip_reuse_time)
        sql = (
            "select count(*) as c from vm_cur_task where (begin_ip='{0}' or hot_ip='{1}') and area={2} and task_group_id={3}  "
            " and (status in(-1,1,2,4) or succ_time is not null)  and TIME_TO_SEC(timediff(now(),start_time))<{4}".format(
                ip, ip, area, task_group_id, ip_reuse_time
            )
        )
        res = self.db.select_sql(sql)
        if not res or len(res) < 1:
            return False
        c = res[0][0]
        if c >= 1:  # 有两个以上相同任务IP
            self.logger.error("gid:%d area:%s 重复ip", task_group_id, area)
            self.get_taskinfo(area, task_group_id, ip)
            return True
        return False

    def get_taskinfo(self, area, task_group_id, ip):
        sql = """select task_group_id, cur_task_id, status, area from vm_cur_task where area={0} and
        task_group_id={1} and begin_ip='{2}' """.format(
            area, task_group_id, ip
        )
        self.logger.error(sql)
        res = self.db.select_sql(sql)
        gid = res[0][0]
        tid = res[0][1]
        status = res[0][2]
        area = res[0][3]
        self.logger.error("task_info:%d,%d,%d,%s", gid, tid, status, area)

    def vpn_update_time(self, area):
        sql = """select update_time,ip,area from vpn_status where area=%s and
    vpnstatus=1 """ % (
            area
        )
        res = dbutil.select_sql(sql)
        if res:
            update_time = res[0][0]
            ip = res[0][1]
            area = int(res[0][2])
            return update_time, ip, area
        return None, None, None

    def vm_last_succ_time(self, area, task_group_id):
        sql = """select max(succ_time) from vm_cur_task where 
         area=%s and task_group_id=%d and status>=2""" % (
            area,
            task_group_id,
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if res:
            self.logger.info("dial time:%s", res[0][0])
            return res[0][0]
        return "1970-1-1 00:00:00"

    def is_grouptype_running(self, type_id):
        sql = """select a.cur_task_id,a.succ_time from vm_cur_task a, vm_task_group_type b where 
        a.server_id={0} and a.task_group_id=b.task_group_id and
        b.type_id={1}  and status in(-1,1)""".format(
            self.server_id, type_id
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        print res, len(res)
        count = len(res)
        if count <= 0:
            return False
        return True

    def grouptype_last_succ_time(self, type_id):
        sql = """select a.cur_task_id,a.succ_time from vm_cur_task a, vm_task_group_type b where 
        a.server_id={0} and a.task_group_id=b.task_group_id and
        b.type_id={1}  order by a.succ_time desc limit 5""".format(
            self.server_id, type_id
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        print "== == == == == == == == == =="
        print res, len(res)
        count = len(res)
        if count <= 0:
            return True, "1970-1-1 00:00:00"
        else:
            task_id = res[0][0]
            if self.is_grouptype_running(type_id):
                return False, None
            else:
                succ_time = res[0][1]
                return True, succ_time

    def task_last_succ_time(self, task_id):
        sql = """select max(succ_time) from vm_cur_task where server_id=%d and
        cur_task_id=%d and status>=2""" % (
            self.server_id,
            task_id,
        )
        res = dbutil.select_sql(sql)
        if res:
            return res[0][0]
        return "1970-1-1 00:00:00"

    def gen_rand_minutes(self, standby_time):
        standby_time_arr = standby_time.split(",")
        stimes = map(int, standby_time_arr)
        if len(stimes) == 1:
            stimes.append(stimes[0])
        randtime = random.randint(stimes[0], stimes[1])
        return randtime

    def task_interval_setting(self, task_id):
        sql = """select interval_times,interval_min from vm_task where
        id={0}""".format(
            task_id
        )
        res = dbutil.select_sql(sql)
        if res:
            times = res[0][0]
            minutes = res[0][1]
            ran_min = self.gen_rand_minutes(minutes)
            return times, ran_min
        return None, None

    def task_interval_info(self, task_id):
        sql = """select times,cur_times,minutes from vm_task_interval where
        id={0}""".format(
            task_id
        )
        res = dbutil.select_sql(sql)
        if res:
            times = res[0][0]
            cur_times = res[0][1]
            minutes = res[0][2]
            return times, cur_times, minutes
        return None, None

    def reset_task_interval(self, task_id):
        ran_min = self.gen_rand_minutes(minutes)
        sql = """update vm_task_interval set
        cur_times=0,minutes={0}""".format(
            ran_min
        )
        ret = dbutil.execute_sql(sql)
        if ret < 0:
            logger.info("sql:%s exec failed %d", sql, ret)

    def task_interval(self, task_id, stime):
        times, cur_times, minutes = self.task_interval_info(task_id)
        if cur_time < times:
            return False
        now = datetime.datetime.now()
        if now - stime > minutes * 60:
            self.reset_task_interval(task_id)
            return False
        return True

    def get_taskgroup_freq(self, task_group_id):
        sql = """select times,`interval` from vm_task_frequency where
        task_group_id={0}""".format(
            task_group_id
        )
        res = dbutil.select_sql(sql)
        if not res:
            return None, None
        else:
            return res[0][0], res[0][1]

    def is_running_times_enough(self, task_group_id):
        freq, inteval = self.get_taskgroup_freq(task_group_id)
        if freq is None:
            return True
        sql = """select count(*) from vm_cur_task  where
        UNIX_TIMESTAMP(start_time)>(UNIX_TIMESTAMP(now()) -
        {0}) and task_group_id={1} and status in (-1,1,2)""".format(
            inteval, task_group_id
        )
        self.logger.info("running times enough:%s", sql)
        res = dbutil.select_sql(sql)
        if not res:
            self.logger.info("task_group_id:%d, times is none", task_group_id)
            return True
        cur_times = res[0][0]
        self.logger.info(
            "task_group_id:%d cur_times:%d running time:%d",
            task_group_id,
            cur_times,
            freq,
        )
        if cur_times < freq:
            return True
        return False

    def is_times_enough(self, task_group_id):
        freq, inteval = self.get_taskgroup_freq(task_group_id)
        if freq is None:
            return True
        sql = """select count(*) from vm_cur_task  where
        UNIX_TIMESTAMP(start_time)>(UNIX_TIMESTAMP(now()) -
        {0}) and task_group_id={1}""".format(
            inteval, task_group_id
        )
        res = dbutil.select_sql(sql)
        if not res:
            logger.info("task_group_id:%d, times is none", task_group_id)
            return True
        cur_times = res[0][0]
        self.logger.info(
            "task_group_id:%d cur_times:%d enough time:%d",
            task_group_id,
            cur_times,
            freq,
        )
        if cur_times < freq:
            return True
        return False

    def get_task_group_type(self, task_group_id):
        sql = """select type_id from vm_task_group_type where
        task_group_id={0}""".format(
            task_group_id
        )
        res = self.db.select_sql(sql)
        if not res:
            return 0
        return res[0][0]

    def is_group_times_enough(self, task_group_id, area, redial_time):
        group_type = self.get_task_group_type(task_group_id)
        if group_type == 0:
            return False
        sql = """select count(*) from vm_cur_task  where
        start_time>'{0}' and group_type={1} and area={2}""".format(
            redial_time, group_type, area
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if not res:
            logger.info("task_group_id:%d,area:%s times is none", task_group_id, area)
            return False
        cur_times = res[0][0]
        self.logger.info(
            "area:%s,task_group_id:%d cur_times:%d enough time:%d",
            area,
            task_group_id,
            cur_times,
            2,
        )
        if cur_times < 2:
            return False
        return True

    def is_impl_times_enough(self, gid, tid):
        sql = """select 1 from vm_task_allot_impl where 
        time_to_sec(NOW()) >= time_to_sec(start_time) and
        time_to_sec(now())<time_to_sec(end_time) and ran_times>=allot_times and
        id={0} and task_id={1}""".format(
            gid, tid
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if not res:
            self.logger.info("task_group_id:%d,tid:%d times is none", gid, tid)
            return False
        return True

    def right_to_allot_zero(self, task_id, area):
        # return True,area
        succ_time = self.task_last_succ_time(task_id)
        if succ_time is None:
            succ_time = "1970-1-1 00:00:00"
        redial_time, ip, area = self.vpn_update_time(area)
        self.logger.info(
            "task_id:%d,last_succ_time:%s, redial_time:%s",
            task_id,
            succ_time,
            redial_time,
        )
        rtime, stime = None, None
        if redial_time:
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")

            if stime < rtime:
                return True, area
            else:
                self.logger.warn("task_id:%d succ_time>=redial_time", task_id)
        return False, None

    def right_to_allot_iqy(self, vm_id, task_group_id):
        if not self.is_running_times_enough(task_group_id):
            self.logger.info("task_group_id:%d running enough time", task_group_id)
            return False
        return True

    def right_to_allot_proxy(self, vm_id, task_group_id):
        if not self.is_running_times_enough(task_group_id):
            self.logger.info("task_group_id:%d running enough time", task_group_id)
            return False
        return True

    def get_group_dial_type(task_group_id):
        """deprecated
        """
        sql = "select dial_type from vm_task_group_setting where task_group_id={0}".format(
            task_group_id
        )
        self.logger.info(sql)
        res = dbutil.select_sql(sql)
        if not res:
            self.logger.info("task_group_id:%d,dial_type is 0", task_group_id)
            return 0
        type = res[0][0]
        return type

    def right_to_allot(self, vm_id, area, task_group_id):
        if area is None:
            return False, None
        if not self.is_times_enough(task_group_id):
            self.logger.info("task_group_id:%d enough time", task_group_id)
            return False, None
        succ_time = self.vm_last_succ_time(area, task_group_id)
        if succ_time is None:
            succ_time = "1970-1-1 00:00:00"
        redial_time, ip, atmp = self.vpn_update_time(area)
        self.logger.info(
            "task_group_id:%d,last_succ_time:%s, redial_time:%s",
            task_group_id,
            succ_time,
            redial_time,
        )
        rtime, stime = None, None
        print redial_time, ip, atmp
        if redial_time:
            if self.is_group_times_enough(task_group_id, area, redial_time):
                self.logger.info("task_group_id:%d group enough time", task_group_id)
                return False, None
            rtime = time.strptime(str(redial_time), "%Y-%m-%d %H:%M:%S")
            stime = time.strptime(str(succ_time), "%Y-%m-%d %H:%M:%S")
            if stime < rtime:
                return True, area
            else:
                self.logger.warn(
                    "task_group_id:%d succ_time>=redial_time", task_group_id
                )
        return False, None

    def is_vpn_dialup_3min(self, area):
        # return False
        if area is None:
            return True
        """ 已拨号成功3分钟 """
        sql = (
            "select 1 from vpn_status where area=%s and vpnstatus=1 and UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(update_time)>180"
            % (area)
        )
        print sql
        res = dbutil.select_sql(sql)
        if res:
            print "dial up 3 mins!!!"
            return True
        else:
            return False

    def is_exclusive_gid(self, gid):
        sql = """
                   select 1 from vm_exclusive_gid where task_group_id={0} and exclusive=1
        ;""".format(
            gid
        )
        res = dbutil.select_sql(sql)
        if not res or len(res) < 1:
            return False
        return True

    def get_rankings(self, vm_id):
        sql = "select distinct ranking from vm_task_group where ran_times<times"
        self.logger.info(sql)
        print "sql", sql
        res = self.db.select_sql(sql)
        rankings = []
        for r in res:
            rankings.append(r[0])
        return rankings

    def get_ranking_gid(self, vm_id):
        rankings = self.get_ranking(vm_id)
        for ranking in rankings:
            ids = self.get_candidate_gid(vm_id, ranking)

    def can_single_gid_run(self, area, single_gid):
        sql = """SELECT
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
                    and b.id = %d
                    and b.priority > 0
                    AND f.server_id = %d """ % (
            single_gid,
            self.server_id,
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in single gid", res
        print "======================="
        ids = set()
        rid_set = self.get_band_run_groupids(area)
        for r in res:
            ids.add(r[0])
        allot_single, allot_others = False, True
        if ids:
            allot_others = False
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)

        single_ids = list(set(ids) - rid_set)

        if single_ids:
            allot_single = True
        return allot_single, allot_others

    def get_iqy_gid(self, area, is_iqy):
        sql = """SELECT
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
                        and d.user_type = %d
                        and b.priority = 0
                        AND f.server_id = %d order by ranking desc """ % (
            is_iqy,
            self.server_id,
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri ranking", res
        print "======================="
        ids = set()
        rid_set = self.get_band_run_groupids(area)
        for r in res:
            ids.add(r[0])
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)

        self.priority_ids = list(set(ids) - rid_set)
        return self.priority_ids

    def get_proxy_gid_noranking(self):
        sql = """SELECT
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
                    and b.ranking = 0
                    and d.user_type >= 20 
                    and b.priority = 0
                    AND f.server_id = %d order by ranking desc """ % (
            self.server_id,
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri ranking", res
        print "======================="
        ids = set()
        for r in res:
            ids.add(r[0])

        self.priority_ids = list(set(ids))
        return self.priority_ids

    def get_proxy_gid_ranking(self):

        sql = """SELECT
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
                    and d.user_type >= 20 
                    and b.priority = 0
                    AND f.server_id = %d order by ranking desc """ % (
            self.server_id,
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri ranking", res
        print "======================="
        ids = set()
        for r in res:
            ids.add(r[0])

        self.priority_ids = list(set(ids))
        return self.priority_ids

    def get_iqy_gid_nodial(self, is_iqy):
        sql = """SELECT
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
                    and d.user_type = %d
                    and b.priority = 0
                    AND f.server_id = %d order by ranking desc """ % (
            is_iqy,
            self.server_id,
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri ranking", res
        print "======================="
        ids = set()
        for r in res:
            ids.add(r[0])

        self.priority_ids = list(set(ids))
        return self.priority_ids

    def get_priority_gid(self, area):
        sql = """SELECT
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
                    and d.user_type not in (11,12,13)
                    and d.user_type < 20
                    and b.priority = 0
                    AND f.server_id = %d order by ranking desc """ % (
            self.server_id
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri ranking", res
        print "======================="
        ids = set()
        rid_set = self.get_band_run_groupids(area)
        for r in res:
            ids.add(r[0])
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)

        self.priority_ids = list(set(ids) - rid_set)
        return self.priority_ids

    def get_candidate_gid(self, vm_id, area):
        sql = """SELECT
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
                    and b.ranking = 0
                    and d.user_type !=11
                    and d.user_type < 20
                    and b.priority = 0
                    AND f.server_id = %d """ % (
            self.server_id
        )
        self.logger.info(sql)
        # print "sql",sql
        res = self.db.select_sql(sql)
        print "======================="
        print "get res in pri", res
        print "======================="
        ids = set()
        rid_set = self.get_band_run_groupids(area)
        for r in res:
            ids.add(r[0])
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)

        self.selected_ids = list(set(ids) - rid_set)
        print self.selected_ids
        return self.selected_ids

    def get_candidate_gid2(self, vm_id, area):
        sql = """SELECT
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
                    and d.user_type = 99
                    and b.priority = 0
                    AND f.server_id = %d order by b.id""" % (
            self.server_id
        )
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        ids = set()
        for r in res:
            ids.add(r[0])
        rid_set = self.get_band_run_groupids(area)
        band_str = ",".join(str(s) for s in rid_set)
        self.logger.info("band task_group_id:%s", band_str)
        self.selected_ids = list(set(ids) - rid_set)

    def allot_by_default(self, vm_id, area):
        self.logger.info("start to allot default task area:%s", area)
        task = TaskGroup.getDefaultTask(self.db)
        if not task:
            self.logger.warn("no default task to run ")
            return None
        ret, area = self.right_to_allot_zero(task.id, area)
        if not ret:
            self.logger.warn("zero task:%d wait for vpn dial...", task.id)
            return None
        ret = self.user.allot_user(vm_id, 0, task.id, area)
        if not ret:
            self.logger.warn(
                "vm_id:%d,task_id:%d,task_group_id:%d no user to run", vm_id, task.id, 0
            )
            return None
        return task

    def get_single_task(self, vm_id, area, gid):
        task = None
        allot_single, allot_others = False, False
        if self.is_repeat_ip(area, gid):
            return None, False
        self.logger.info("====================handle gid:%d====================", gid)
        try:
            with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                allot_single, allot_others = self.can_single_gid_run(area, gid)
                # 如果是独占任务，不分配其他
                if self.is_exclusive_gid(gid):
                    allot_others = False
                print allot_single, allot_others
                # 改到area了，不要
                if allot_single:
                    ret, area1 = self.right_to_allot(vm_id, area, gid)
                    print ret, area1
                    if ret:
                        self.logger.info("get valid gid:%d", gid)

                        task = self.handle_taskgroup(gid, vm_id, area)
                        if task:
                            self.logger.info("get the gid:%d  task:%d", gid, task.id)
                            ret = True
                        # self.add_ran_times(task.id, gid, task.rid)
        except Exception, e:
            self.logger.error("exception on lock", exc_info=True)
        return task, allot_others

    def get_iqy_task(self, vm_id, area, is_iqy):
        task, gid = None, None
        ret = False
        self.reset_when_newday()
        if not self.priority_ids:
            self.get_iqy_gid(area, is_iqy)

        while self.priority_ids:
            band_str = ",".join(str(s) for s in self.priority_ids)
            ret = False
            gid = self.priority_ids.pop()
            if self.is_repeat_ip(area, gid):
                return None, None
            self.logger.info(
                "====================handle gid:%d====================", gid
            )
            try:
                # if True:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    # 放在里面否则可能出现多个任务不按间隔时间跑
                    rid_set = self.get_band_run_groupids(area)
                    print "band groupid:", rid_set
                    if gid in rid_set:
                        self.logger.error("gid:%d is banded", gid)
                        continue
                    ret, area1 = self.right_to_allot(vm_id, area, gid)
                    if ret:
                        self.logger.info("get valid gid:%d", gid)
                    else:
                        # self.logger.warn("wait for redial:%d", gid)
                        continue

                    task = self.handle_taskgroup(gid, vm_id, area)
                    if task:
                        self.logger.info("get the gid:%d  task:%d", gid, task.id)
                        ret = True
                        # self.add_ran_times(task.id, gid, task.rid)
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.priority_ids.append(gid)
                time.sleep(2)
                continue
        return task, gid

    def get_proxy_task_noranking(self, vm_id):
        task, gid = None, None
        ret = False
        self.reset_when_newday()
        if not self.priority_ids:
            self.get_proxy_gid_noranking()

        while self.priority_ids:
            band_str = ",".join(str(s) for s in self.priority_ids)
            ret = False
            gid = self.priority_ids.pop()
            self.logger.info(
                "====================handle proxy gid:%d====================", gid
            )
            try:
                # if True:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    ret = self.right_to_allot_proxy(vm_id, gid)
                    if ret:
                        self.logger.info("get proxy valid gid:%d", gid)
                    else:
                        continue

                    task = self.handle_taskgroup(gid, vm_id, None)
                    if task:
                        self.logger.info("get the proxy gid:%d  task:%d", gid, task.id)
                        ret = True
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.priority_ids.append(gid)
                time.sleep(2)
                continue
        return task, gid

    def get_proxy_task_ranking(self, vm_id):
        task, gid = None, None
        ret = False
        self.reset_when_newday()
        if not self.priority_ids:
            self.get_proxy_gid_ranking()

        while self.priority_ids:
            band_str = ",".join(str(s) for s in self.priority_ids)
            ret = False
            gid = self.priority_ids.pop()
            self.logger.info(
                "====================handle proxy gid:%d====================", gid
            )
            try:
                # if True:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    ret = self.right_to_allot_proxy(vm_id, gid)
                    if ret:
                        self.logger.info("get proxy valid gid:%d", gid)
                    else:
                        continue

                    task = self.handle_taskgroup(gid, vm_id, None)
                    if task:
                        self.logger.info("get the proxy gid:%d  task:%d", gid, task.id)
                        ret = True
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.priority_ids.append(gid)
                time.sleep(2)
                continue
        return task, gid

    def get_iqy_task_nodial(self, vm_id, is_iqy):
        task, gid = None, None
        ret = False
        self.reset_when_newday()
        if not self.priority_ids:
            self.get_iqy_gid_nodial(is_iqy)

        while self.priority_ids:
            band_str = ",".join(str(s) for s in self.priority_ids)
            ret = False
            gid = self.priority_ids.pop()
            self.logger.info(
                "====================handle nogid:%d====================", gid
            )
            try:
                # if True:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    ret = self.right_to_allot_iqy(vm_id, gid)
                    if ret:
                        self.logger.info("get iqyall valid gid:%d", gid)
                    else:
                        # self.logger.warn("wait for redial:%d", gid)
                        continue

                    task = self.handle_taskgroup(gid, vm_id, None)
                    if task:
                        self.logger.info("get the iqyall gid:%d  task:%d", gid, task.id)
                        ret = True
                        # self.add_ran_times(task.id, gid, task.rid)
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.priority_ids.append(gid)
                time.sleep(2)
                continue
        return task, gid

    def get_ranking_task(self, vm_id, area):
        task, gid = None, None
        ret = False
        self.reset_when_newday()
        if not self.priority_ids:
            self.get_priority_gid(area)

        while self.priority_ids:
            band_str = ",".join(str(s) for s in self.priority_ids)
            ret = False
            gid = self.priority_ids.pop()
            if self.is_repeat_ip(area, gid):
                return None, None
            self.logger.info(
                "====================handle gid:%d====================", gid
            )
            try:
                # if True:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    # 放在里面否则可能出现多个任务不按间隔时间跑
                    rid_set = self.get_band_run_groupids(area)
                    print "band groupid:", rid_set
                    if gid in rid_set:
                        self.logger.error("gid:%d is banded", gid)
                        continue
                    ret, area1 = self.right_to_allot(vm_id, area, gid)
                    if ret:
                        self.logger.info("get valid gid:%d", gid)
                    else:
                        continue

                    task = self.handle_taskgroup(gid, vm_id, area)
                    if task:
                        self.logger.info("get the gid:%d  task:%d", gid, task.id)
                        ret = True
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.priority_ids.append(gid)
                time.sleep(2)
                continue
        return task, gid

    def get_allot_task(self, vm_id, brest, area):
        task, gid = None, None
        if area is None:
            return task, gid
        ret = False
        self.reset_when_newday()
        if not brest:
            if not self.selected_ids:
                self.get_candidate_gid(vm_id, area)
        else:
            if not self.selected_ids:
                self.get_candidate_gid2(vm_id, area)

        while self.selected_ids:
            band_str = ",".join(str(s) for s in self.selected_ids)
            ret = False
            gid = self.selected_ids.pop()
            if self.is_repeat_ip(area, gid):
                return None, False
            print "gogog"
            self.logger.info(
                "====================handle gid:%d====================", gid
            )

            try:
                with utils.SimpleFlock("/tmp/{0}.lock".format(gid), 1):
                    # 放在里面否则可能出现多个任务不按间隔时间跑
                    self.logger.info("get area:%s", area)
                    rid_set = self.get_band_run_groupids(area)
                    print "band groupid:", rid_set
                    if gid in rid_set:
                        self.logger.error("gid:%d is banded", gid)
                        continue
                    ret, area1 = self.right_to_allot(vm_id, area, gid)
                    if ret:
                        self.logger.info("get valid gid:%d", gid)
                    else:
                        # self.logger.warn("wait for redial:%d", gid)
                        continue

                    task = self.handle_taskgroup(gid, vm_id, area)
                    if task:
                        self.logger.info("get the gid:%d  task:%d", gid, task.id)
                        ret = True
                        # self.add_ran_times(task.id, gid, task.rid)
                        break
                    else:
                        continue
            except Exception, e:
                self.logger.error("exception on lock", exc_info=True)
                self.logger.info("exception in lock, timeout")
                # self.selected_ids.append(gid)
                # time.sleep(20)
                continue
        return task, gid

    def allot_by_priority(self, vm_id, area, single_gid, is_iqy=0):
        try:
            self.is_iqy = is_iqy
            # if True:
            if self.is_vpn_dialup_3min(area):
                return False, None
            task, gid, ret = None, 0, False
            if single_gid:
                task, allot_others = self.get_single_task(vm_id, area, single_gid)
                if not task and not allot_others:
                    return False, None
            if not task:
                if is_iqy:
                    task, gid = self.get_iqy_task(vm_id, area, is_iqy)
                else:
                    task, gid = self.get_ranking_task(vm_id, area)
                    if not task:
                        task, gid = self.get_allot_task(vm_id, False, area)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        if task:
            return ret, task.id
        else:
            return ret, None

    def allot_by_proxy(self, vm_id):
        try:
            ret = True
            task, gid = None, 0
            task, gid = self.get_proxy_task_ranking(vm_id)
            if not task:
                task, gid = self.get_proxy_task_noranking(vm_id)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        if task:
            return ret, task.id
        else:
            return ret, None

    def allot_without_dial(self, vm_id, is_iqy):
        try:
            task, gid = None, 0
            task, gid = self.get_iqy_task_nodial(vm_id, is_iqy)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        if task:
            return ret, task.id
        else:
            return ret, None

    def allot_rest(self, vm_id, area=10):
        try:
            task, gid, ret = None, 0, True
            task, gid = self.get_allot_task(vm_id, True, area)
            if task:
                ret = True
                self.add_ran_times(task.id, gid, task.rid)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        if task:
            return ret, task.id
        else:
            return ret, None

    def allot_default(self, vm_id, area):
        try:
            task, gid, ret = None, 0, True
            task = self.allot_by_default(vm_id, area)
            self.logger.info("default task:%d", task.id)
            if task:
                ret = True
                self.add_ran_times(task.id, gid, task.rid)
        except TaskError, t:
            raise TaskAllotError, "excute error:%s" % (t.message)
            ret = False
        if task:
            return ret, task.id
        else:
            return ret, None

    def get_task_type(self, task_id):
        sql = """select user_type,is_ad,binding_areas from vm_task where id=%d """ % (
            task_id
        )
        res = self.db.select_sql(sql)
        if not res:
            return None, None, None
        return res[0][0], res[0][1], res[0][2]

    def handle_taskgroup(self, task_group_id, vm_id, area):
        task = self.task_group.choose_vaild_task(self.server_id, task_group_id)
        if not task:
            return None

        if not self.wait_interval(task.id):
            self.logger.warn("task_id: %d 需等待5分钟", task.id)
            return None
        self.logger.warn("==========get the valid task:%d==========", task.id)
        uty, is_ad, binding_areas = self.get_task_type(task.id)
        b_areas = []
        if binding_areas:
            b_areas = binding_areas.split(",")
        if b_areas:
            if area not in b_areas:
                self.logger.info("当前区域:%s 不在绑定区域内[%s]", area, binding_areas)
                return None

        self.logger.info("task uty:%d, is_ad:%d", uty, is_ad)
        if is_ad:
            # 广告专享
            # ret = self.user7.allot_user(vm_id, task_group_id, task.id, area)
            ret = self.user.allot_user(vm_id, task_group_id, task.id, area)
        elif uty == 6:
            ret = self.user_ec.allot_user(vm_id, task_group_id, task.id)
        elif uty == 99:
            ret = self.user_rest.allot_user(vm_id, task_group_id, task.id)
        elif uty == 10:
            ret = self.user_reg.allot_user(vm_id, task_group_id, task.id, area)
        elif uty == 11:
            ret = self.user_iqy.allot_user(vm_id, task_group_id, task.id, area)
        elif uty == 12:
            self.logger.info("user iqyatv")
            ret = self.user_iqyatv.allot_user(vm_id, task_group_id, task.id, area)
        elif uty == 13:
            self.logger.info("user iqyall")
            area = 0
            ret = self.user_iqyall.allot_user(vm_id, task_group_id, task.id, area)
        elif uty >= 20:
            ret = self.user_rand.allot_user(vm_id, task_group_id, task.id, None)
        else:
            ret = self.user.allot_user(vm_id, task_group_id, task.id, area)
        if not ret:
            self.logger.warn(
                "vm_id:%d,task_id:%d,task_group_id:%d no user to run",
                vm_id,
                task.id,
                task_group_id,
            )
            return None

        self.add_ran_times(task.id, task_group_id, task.rid)
        return task

    def add_zero_limit_times(self, id):
        sql = "update zero_schedule_list set ran_times=ran_times+1 where id=%d" % (id)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s excute error;ret:%d" % (sql, ret)

    def add_ran_times(self, task_id, task_group_id, rid):
        """ 分配成功后有可用profile 时计数
        """
        self.logger.warn("task_id:%d, task_group_id:%d", task_id, task_group_id)
        # tg = TaskGroup(task_group_id, self.db)
        if task_group_id == 0:
            self.task_group.add_ran_times2(task_id, task_group_id)
            # self.add_zero_limit_times(rid)
            # TaskGroup.add_default_ran_times(self.db)
        else:
            # 只更新impl的值得,不更新group,(group由脚本更新)
            #     tg.add_ran_times(task_id)
            # tg.add_impl_ran_times(task_id)
            self.task_group.add_impl_ran_times(task_group_id, task_id)


def allot_test(dbutil):
    """任务分配测试
    """
    task_group_id = None
    if len(sys.argv) > 1:
        task_group_id = int(sys.argv[1])
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
            time.sleep(5)
            continue


# def getTask(dbutil, logger):
def getTask():
    import random

    # from rolling_user import UserAllot
    from user import UserAllot
    from user_rest import UserAllot as UserRest
    from user_reg import UserAllot as UserReg
    from user_iqy import UserAllot as UserIQY
    from user_rolling7 import UserAllot as UserAllot7

    dbutil.db_host = "192.168.1.21"
    # dbutil.db_host = "3.3.3.6"
    dbutil.db_name = "vm4"
    # dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    # sid=[8,11,16,18,19,21,23]
    # s = random.choice(sid)
    s = server_id
    print ("==========server id:%d==========", s)
    # time.sleep(10)
    pc = ParallelControl(s, dbutil, logger)
    user = UserAllot(s, pc, dbutil, logger)
    urest = UserRest(s, pc, dbutil, logger)
    ureg = UserReg(s, pc, dbutil, logger)
    uiqy = UserIQY(s, pc, dbutil, logger)
    user7 = UserAllot7(s, pc, dbutil, logger)
    t = TaskAllot(
        0, s, pc, user, None, user7, urest, ureg, uiqy, None, None, dbutil, logger
    )

    # t.allot_by_default(1, 1)
    # t.allot_by_default(2, 7)
    # t.allot_by_default(2, 1)
    # t.allot_by_nine(1)
    # ret,task_id = t.allot_by_priority(1, 0)
    while True:
        # areas = range(1,30)
        # area = random.choice(areas)
        ret = t.allot_by_priority(1, 32, None)
        time.sleep(3)
    # 单机跑测试
    # while True:
    # ret = t.allot_by_priority(5, 115)
    # time.sleep(3)
    # break


def get_default_logger():
    logger = logging.getLogger()
    # logger.setLevel(logging.INFO)

    # console logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


server_id = 1
if __name__ == "__main__":
    dbutil.db_host = "192.168.1.21"
    # dbutil.db_host = "3.3.3.6"
    # dbutil.db_name = "vm3"
    dbutil.db_name = "vm4"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    # logger = get_default_logger()
    global server_id
    server_id = int(sys.argv[1])
    getTask()

    # import threading
    # # for i in range(0,1):
    # t2 = threading.Thread(target=getTask, name="test")
    # t2.start()
    # t3 = threading.Thread(target=getTask, name="test")
    # t3.start()
    # t4 = threading.Thread(target=getTask, name="test")
    # t4.start()
    # t5 = threading.Thread(target=getTask, name="test")
    # t5.start()
    # t6 = threading.Thread(target=getTask, name="test")
    # t6.start()
    # t7 = threading.Thread(target=getTask, name="test")
    # t7.start()
    # getTask(dbutil, logger)

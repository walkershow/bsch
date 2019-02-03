
import sys
import random
import logging
import logging.config
from task_profiles import TaskProfile
sys.path.append("..")
import dbutil
from logbytask.logtask import LogTask, LogTaskError

from zero_running_rule import ZeroTask, ZeroTaskError


class TaskProfile_Rand(TaskProfile):


    def get_used_profiles(self, vm_id, user_type, terminal_type, task_group_id,
            area):
        profiles = []
        self.reuse_profiles(vm_id)
        sql = '''select profile_id from vm_task_profile_latest where
        server_id=%d and vm_id=%d and
        user_type=%d and terminal_type=%d and task_group_id=%d and area=%s''' % (self.server_id, vm_id,
                                                user_type, terminal_type,
                                                task_group_id, area)
        print sql
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        print "gg"
        return profiles

    def get_inited_profiles(self, vm_id, tty, uty, area):
        sql = '''select a.profile_id from vm_users a where a.server_id=%d and a.vm_id=%d 
        and user_type=%d and a.terminal_type = %d and area=%s ''' 
        sql = sql % (self.server_id, vm_id, uty, tty, area )
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        profile_ids = []
        # if not res:
            # return []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids

    def get_task_usable_profiles(self, vm_id, user_type, terminal_type, 
            task_group_id, area):
        all_profiles = self.get_inited_profiles(vm_id, terminal_type,
                                                user_type, area)
        used_profiles = self.get_used_profiles(vm_id, user_type, terminal_type,
                task_group_id, area)
        usable_profiles = list(
            set(all_profiles).difference(set(used_profiles)))
        profile_id = None
        if usable_profiles:
            profile_id = random.choice(usable_profiles)
        return profile_id

    def get_dialup_ip(self, area):
        sql = '''select ip from vpn_status where area='{0}' and
            vpnstatus=1'''.format(area)
        res = self.db.select_sql(sql)
        if not res or len(res)<1:
            return None
        r = res[0]
        ip = r[0]
        return ip


    def set_cur_task_profile(self, vm_id, task_id, task_group_id, cookie_type,area):
        dialup_ip  = self.get_dialup_ip(area)
        if dialup_ip is None:
            print "dialup_ip is None"
            return False

        group_type = self.get_task_group_type(task_group_id)
        r = self.get_task_type(task_id)
        print r['standby_time'],r['inter_time']
        randtime = self.gen_rand_standby_time(r['standby_time'])
        #if cookie_type == 0 and task_group_id!=0:
        #    profile_id = 0 
        #else:
        if cookie_type==0:
            self.logger.info("set zero task profile")
            profile_id = self.zt.get_usable_profiles(vm_id, r['user_type'],
                                                     r['terminal_type'], area)
        else:
            profile_id = self.get_task_usable_profiles(vm_id,r['user_type'], r['terminal_type'],
                     task_group_id, area)
        if not profile_id:
            return False
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id,
                                                   task_id)

        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,
        task_group_id,status,status2,start_time,oprcode,ran_minutes,user_type,
        terminal_type,standby_time, timeout,
        copy_cookie,click_mode,inter_time,area,group_type, begin_ip)
         value(%d,%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d,
                 %d,%d,%d,%d,%d,%s,%d,'%s')''' %(
            self.server_id, vm_id, task_id, profile_id, task_group_id,
            -1, -1, oprcode,r['user_type'], r['terminal_type'],
            randtime,r['timeout'],
            r['copy_cookie'], r['click_mode'], r['inter_time'], area,
            group_type, dialup_ip)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
        self.logger.info(
            "allot profile succ info:server_id:%d,vm_id:%d,task_id:%d,task_type:%d,profile_id:%d",
            self.server_id, vm_id, task_id, r['type'], profile_id)
        if task_id != 0:
            self.log_task_profile_latest(vm_id, task_group_id, task_id, r['type'], profile_id,
                                         oprcode, -1, r['user_type'],
                                         r['terminal_type'], area)
        self.log_task.log(
            self.server_id,
            vm_id,
            task_id,
            status=-1,
            start_time="CURRENT_TIMESTAMP")
        return True


def get_default_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

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
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logger = get_default_logger()
    t = TaskProfile_Rest(9, dbutil, None, logger)
    t.set_cur_task_profile(1, 36, 54,1)

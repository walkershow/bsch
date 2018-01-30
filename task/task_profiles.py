# -*- coding: utf-8 -*-

# @CreateTime: Sep 13, 2017 3:47 PM
# @Author: coldplay
# @Contact: coldplay
# @Last Modified By: coldplay
# @Last Modified Time: Jan 16, 2018 5:48 PM
# @Description: Modify Here, Please
import sys
import dbutil
import random
sys.path.append("..")
from zero_running_rule import ZeroTask,ZeroTaskError
from logbytask.logtask import LogTask,LogTaskError
# from task.parallel import ParallelControl,ParallelControlException

class TaskProfile(object):
    db = None
    logger = None
    server_id = None
    log_task = None
    pc = None
    zt = None
    
    def __init__(self, server_id, dbs, pc, log_handler):
        TaskProfile.db = dbs
        TaskProfile.logger = log_handler
        TaskProfile.server_id = server_id
        TaskProfile.log_task = LogTask(dbs, log_handler)
        TaskProfile.pc = pc
        TaskProfile.zt = ZeroTask(server_id, dbs)

    def get_task_type(self, task_id):
        sql = "select type,user_type,terminal_type from vm_task where id=%d "%(task_id)
        res = self.db.select_sql(sql)
        if not res:
            return None,None
        return res[0][0],res[0][1],res[0][2]

    def get_reenable_day(self, task_type):
        sql = "select re_enable_hour_start_range,re_enable_hour_end_range from vm_task_reenable "\
        " where id = %d"%(task_type)
        res = self.db.select_sql(sql)
        if not res:
            raise Exception,"there's no task_type setting"
        start = res[0][0]
        end = res[0][1]
        day = random.randint(start, end)
        print "days:", day
        return day
    
    #need task_type,no!!!!
    def reuse_profiles(self, vm_id):
        # return
        sql = "delete from vm_task_profile_latest where server_id=%d and vm_id=%d and status in(-2,-1,1,2,4,6) "\
        "and TIMESTAMPDIFF(HOUR, start_time, now())>=re_enable_hours "
        sql = sql%(self.server_id, vm_id)
        # self.logger.info(sql)
        ret= self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s exec failed ret:%d"%(sql, ret)
        self.reuse_profiles2(vm_id)

    def reuse_profiles2(self, vm_id):
        # return
        sql = "delete from vm_task_profile_latest where server_id=%d and vm_id=%d and status in(3,5,7,8,9)"
        sql = sql%(self.server_id, vm_id)
        self.logger.info(sql)
        ret= self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s exec failed ret:%d"%(sql, ret)

    def get_used_profiles(self, vm_id, user_type, terminal_type ):
        profiles = []
        sql = ""
        self.reuse_profiles(vm_id)
        sql = "select profile_id from vm_task_profile_latest where server_id=%d and vm_id=%d and  "\
        "user_type=%d and terminal_type=%d"\
            %(self.server_id, vm_id, user_type, terminal_type)
        res = self.db.select_sql(sql)
        for r in res:
            id = r[0]
            profiles.append(id)
        return profiles


    def get_inited_profiles(self, vm_id, tty, uty, day):
        sql = "select a.profile_id from vm_users a where a.server_id=%d and a.vm_id=%d "\
        "and user_type=%d and a.terminal_type = %d and TIMESTAMPDIFF(DAY,a.create_time,now())=%d "
        sql = sql%(self.server_id, vm_id, uty, tty, day)
        self.logger.info(sql)
        res = self.db.select_sql(sql)
        profile_ids = []
        for r in res:
            profile_ids.append(r[0])
        return profile_ids


    def get_task_usable_profiles(self, vm_id, user_type, terminal_type, day):
        all_profiles = self.get_inited_profiles(vm_id, terminal_type, user_type, day)
        used_profiles = self.get_used_profiles(vm_id, user_type, terminal_type)
        # print all_profiles, used_profiles
        usable_profiles = list(set(all_profiles).difference(set(used_profiles)))
        # print usable_profiles
        profile_id = None
        if usable_profiles:
            profile_id = random.choice(usable_profiles)
        return profile_id

    
    def set_cur_task_profile(self, vm_id, task_id, task_group_id, day):
        is_default = False
        if task_group_id == 0:
            is_default = True
        task_type, user_type, terminal_type = self.get_task_type(task_id) 
        self.logger.info("task id:%d task_type:%d,user_type:%d, terminal_type:%d",task_id, task_type, user_type,terminal_type)
        # print "set_cur_task_profile:",task_type, terminal_typ
        if is_default:
            profile_id = self.zt.get_usable_profiles(vm_id, user_type, terminal_type) 
        else:
            profile_id = self.get_task_usable_profiles(vm_id, user_type, terminal_type, day)
        # print profile_id
        if not profile_id:
            self.logger.warn("vm_id:%d task id:%d no profile to use!!!", vm_id ,task_id)
            return False
        self.logger.info("vm_id:%d task id:%d will run allot profile id:%d", vm_id,task_id, profile_id)
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id, task_id)
        self.pc.add_allocated_num(task_group_id)

        sql = "insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,task_group_id,status,start_time,oprcode,ran_minutes,user_type, terminal_type)"\
        " value(%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d) "%(
            self.server_id, vm_id, task_id, profile_id, task_group_id, -1, oprcode,user_type, terminal_type)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s exec failed ret:%d"%(sql, ret)
        self.logger.info("allot profile succ info:server_id:%d,vm_id:%d,task_id:%d,task_type:%d,profile_id:%d",
                    self.server_id,vm_id, task_id, task_type, profile_id)
        # print self.server_id,vm_id, task_id, task_type, profile_id
        if task_id != 0:
            self.log_task_profile_latest(vm_id, task_id, task_type, profile_id, oprcode, -1, user_type, terminal_type)
        self.log_task.log(self.server_id, vm_id, task_id, status=-1, start_time="CURRENT_TIMESTAMP")
        return True


    def log_task_profile_latest(self, vm_id, task_id, task_type, profile_id, oprcode, status, user_type,terminal_type):
        re_enable_hours = self.get_reenable_day(task_type)
        print  self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours 
        sql = "insert into vm_task_profile_latest(server_id,vm_id,profile_id,task_type,task_id,start_time,re_enable_hours, oprcode, status, user_type,terminal_type)"\
        " values(%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d, %d, %d, %d, %d) on duplicate key update  task_type=%d,"\
        " start_time=CURRENT_TIMESTAMP, re_enable_hours=%d, oprcode=%d, status=%d"%(
           self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours, oprcode, status, user_type, terminal_type,
              task_type, re_enable_hours, oprcode, status)
        self.logger.debug("latest:%s",sql)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s exec failed ret:%d"%(sql, ret)
        self.log_task_profile(vm_id, task_id, task_type, profile_id, re_enable_hours, oprcode, user_type, terminal_type)
    
    def log_task_profile(self, vm_id, task_id, task_type, profile_id, re_enable_hours, oprcode, user_type, terminal_type):
        sql = "insert into vm_task_profile_log(server_id,vm_id,profile_id,task_type,task_id,log_time,re_enable_hours,oprcode, user_type,terminal_type)"\
        " values(%d,%d,%d,%d,%d,CURRENT_TIMESTAMP, %d, %d, %d, %d) "%(
            self.server_id, vm_id, profile_id, task_type, task_id, re_enable_hours, oprcode, user_type, terminal_type)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s exec failed ret:%d"%(sql, ret)


if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm2"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    pc = ParallelControl(g_serverid, dbutil)
    t=TaskProfile(1, dbutil, pc, None)
    t.set_cur_task_profile(1,1,1)
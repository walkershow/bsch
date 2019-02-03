
import sys
import random
import logging
import logging.config
from task_profiles import TaskProfile
sys.path.append("..")
import dbutil
from logbytask.logtask import LogTask, LogTaskError



class TaskProfile_Rest(TaskProfile):

    def get_rand_profiles(self, vm_id, tty, uty ,area):
        print tty, uty
        sql = " select a.profile_id from vm_profiles a,profiles b "\
            "where a.server_id={0} and a.vm_id={1} "\
            "and b.terminal_type = {3} and area={4} and a.profile_id=b.id order by rand() limit 1" .format(
                self.server_id, vm_id, uty, tty ,area)
        print sql
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
            return None
        return res[0][0]


    def set_cur_task_profile(self, vm_id, task_id, task_group_id, day,area):
        r = self.get_task_type(task_id)
        print r['standby_time'],r['inter_time']
        randtime = 1
        profile_id = self.get_rand_profiles(vm_id, r['terminal_type'],
                r['user_type'])
        if not profile_id:
            return False
        self.log_task.gen_oprcode_bytask(self.server_id, vm_id, task_id)
        oprcode = self.log_task.get_oprcode_bytask(self.server_id, vm_id,
                                                   task_id)

        sql = '''insert into vm_cur_task(server_id,vm_id,cur_task_id,cur_profile_id,
        task_group_id,status,start_time,oprcode,ran_minutes,user_type,
        terminal_type,standby_time, timeout,
        copy_cookie,click_mode,inter_time,area)
         value(%d,%d,%d,%d,%d,%d,CURRENT_TIMESTAMP,%d,0,%d,%d,
                 %d,%d,%d,%d,%d,%d)''' %(
            self.server_id, vm_id, task_id, profile_id, task_group_id,
            -1, oprcode,r['user_type'], r['terminal_type'],
            randtime,r['timeout'],
            r['copy_cookie'], r['click_mode'], r['inter_time'], area)
        ret = self.db.execute_sql(sql)
        if ret < 0:
            raise Exception, "%s exec failed ret:%d" % (sql, ret)
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
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logger = get_default_logger()
    t = TaskProfile_Rest(9, dbutil, None, logger)
    t.set_cur_task_profile(1, 36, 54,1)

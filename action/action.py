# -*- coding: utf-8 -*-
###
# @Author: coldplay 
# @Date: 2017-08-03 10:53:55 
# @Last Modified by:   coldplay 
# @Last Modified time: 2017-08-03 10:53:55 
###
import sys
import random
import bisect
import logging
import logging.config
sys.path.append("..")
import dbutil

class ActionError(Exception):
    pass

class WeightedRandomGenerator(object):
    def __init__(self, weights):
        self.totals = []
        running_total = 0

        for w in weights:
            running_total += w
            self.totals.append(running_total)

    def next(self):
        rnd = random.random() * self.totals[-1]
        return bisect.bisect_right(self.totals, rnd)

    def __call__(self):
        return self.next()

class MakeActionScript(object):
    def __init__(self, taskid, db):
        self.db = db
        self.taskid =taskid
    
    def gen_type_times(self,typeid):
        sql ="select FLOOR(start_range + (RAND() * (end_range-start_range))) from vm_action_type_times where type_id=%d and task_id=%d"%(typeid, self.taskid)
        print sql
        res = self.db.select_sql(sql)
        if not res:
            raise Exception,"%s excute error"%(sql)
        return int(res[0][0])
    
    def get_script(self,actionid):
        sql ="select script,to_continue from vm_action where id=%d and task_id=%d"
        sql_action = sql%(actionid, self.taskid)
        res = self.db.select_sql(sql_action)
        if not res:
            raise Exception,"%s excute error"%(sql_action)
        return res[0][0],res[0][1]

    def get_start_action(self):
        sql ="select script,to_continue from vm_action where id=0 and task_id=%d"%(self.taskid)
        res = self.db.select_sql(sql)
        if not res:
            raise Exception,"%s excute error"%(sql)
        return res[0][0]

    def get_end_action(self):
        sql ="select script,to_continue from vm_action where id=100 and task_id=%d"%(self.taskid)
        res = self.db.select_sql(sql)
        if not res:
            raise Exception,"%s excute error"%(sql)
        return res[0][0]

    def get_type_set(self,typeid):
        to_end = False
        sql ="select actionids,weight from vm_action_type_detail where type_id=%d and task_id=%d order by id"
        sql_typeid = sql%(typeid, self.taskid)
        print sql_typeid
        res = self.db.select_sql(sql_typeid)
        if not res:
            raise Exception,"%s excute error"%(sql_typeid)
        print res
        weights = [int(r[1]) for r in res]
        print weights
        actionids_list = [r[0] for r in res]
        wrg = WeightedRandomGenerator(weights)
        loop_times = self.gen_type_times(typeid)
        script_txt = ""
        for i in range(loop_times):
            n = wrg()
            actionids = actionids_list[n]
            actionid_list = [ int(s) for s in actionids.split(",") ]

            for id in actionid_list:
                script, to_continued = self.get_script(id)
                script_txt += script + "\r\n"
                if to_continued == 0:
                    return script_txt, 99

        return script_txt, typeid+1

    def make(self):
        sql = "select type_id from vm_action_type_times where task_id=%d order by type_id"
        #sql = "select type_id from vm_action_type_times where type_id=1 order by type_id"
        print "taskid", self.taskid
        sql = sql%(self.taskid)
        res = self.db.select_sql(sql)
        if not res:
            raise Exception,"%s excute error"%(sql)
        script_txt = self.get_start_action() + "\r\n"
        for r in res:
            typeid = r[0]
            txt, next_id = self.get_type_set(typeid) 
            script_txt +=  txt
            if next_id == 99:
                break
        script_txt += self.get_end_action()
        return script_txt

        
def main():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    mac = MakeActionScript(99,dbutil)
    str = mac.make()
    print str
    
if __name__ == '__main__':
    main()
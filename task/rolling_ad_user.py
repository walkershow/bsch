# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-12 16:29:06 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-12 16:29:06 
'''

import sys
import datetime
import os
import shutil
import time
import threading
import logging
import logging.config
sys.path.append("..")
import dbutil
import utils

logger = None


class RollingADUser(object):
    '''任务
    '''
    def __init__(self, db,logger):
        self.db = db 
        self.logger = logger

    def get_broken_vpn_servers(self):
        sql = '''SELECT a.serverid from vpn_status a, vm_server_list b where UNIX_TIMESTAMP(a.update_time)>UNIX_TIMESTAMP(current_date)-3600*24
                 and TIMESTAMPDIFF(minute,  a.update_time, CURRENT_TIMESTAMP())>30 and
                a.serverid=b.id and b.status=1'''
        res = dbutil.select_sql(sql)
        timeout_array = []
        if not res or len(res)<=0:
            return []
        for r in res:
            print r
            server_id = r[0]
            timeout_array.append(server_id)
        return timeout_array

    def get_useout_serverid(self):
        useout_dict = {}
        sql = '''select task_id,used_out_server_ids from vm_task_rolling7 where
        used_out_server_ids is not null and
        used_out_server_ids != '' order by task_id'''
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
            logger.info("%s sql get empty res"%(sql))
            return None
        broken_servers= self.get_broken_vpn_servers()
        bs_str = ','.join(map(str,broken_servers))
        logger.info('broken server:%s', bs_str)
        for r in res:
            tid = r[0]
            sid_str = r[1]
            sids = [int(x) for x in sid_str.split(',')]
            sids2 = sorted(sids)
            c = list(set(sids2+broken_servers))
            useout_dict[tid]=c
            
        return useout_dict

    def get_task_pri(self, task_id):
        sql = '''select priority from vm_task_group where
        task_id={0}'''.format(task_id)
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
            return None
        return res[0][0]

    def get_all_running_server(self):
        sql = '''select id from vm_server_list where status=1'''
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
            logger.info("%s sql get empty res"%(sql))
            return None
        sids = []
        for r in res:
            sids.append(r[0])
        return sorted(sids)

    def get_task_running_server(self, task_id):
        sql='''select b.id from vm_task_group a, vm_server_list b where
        a.priority = b.run_as_single and b.status=1 and
        a.task_id={0}'''.format(task_id)
        #print "running sql:",sql
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
           return None
        sids = []
        for r in res:
            sids.append(r[0])
        return sorted(sids)

    def delete_rolling_log(self, task_id):
        sql = '''delete from vm_task_rolling7 where task_id={0}'''.format(task_id)
        self.logger.info(sql)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s sql execute failed"%(sql)

    def reset_task_rolling(self):
        while True:
            u_sids = self.get_useout_serverid()
            if not u_sids:
                time.sleep(5)
                continue
            for t,v in u_sids.items():
                pri = self.get_task_pri(t)
                if pri>0:
                    sids = self.get_task_running_server(t)
                else:
                    sids = self.get_all_running_server()
                print "t:",t,"v:",v
                print "t:",t,"sids:", sids
                if v == sids:
                    print 'same:',u_sids, sids
                    us_str = ','.join(map(str,v))
                    s_str = ','.join(map(str,sids))
                    logger.info("same task_id:%d userout:%s,\
                            serverlist:%s",t,us_str,s_str)
                    self.delete_rolling_log(t)
                time.sleep(1)
            time.sleep(10)


def main():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logconf_file = 'adrolling.log.conf'

    if not os.path.exists(logconf_file):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(logconf_file)
    global logger
    logger = logging.getLogger()
    rdu = RollingADUser(dbutil,logger)
    rdu.reset_task_rolling()

if __name__ == '__main__':
    main()

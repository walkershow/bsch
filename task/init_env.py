# -*- coding: utf-8 -*-
'''
@Author: coldplay
@Date: 2017-05-12 16:29:06
@Last Modified by:   coldplay
@Last Modified Time: Jan 4, 2018 10:40 AM
'''

import sys
import random
import logging
import logging.config
import os
sys.path.append("..")
import dbutil

logger = None

profile_dir = '/home/pi/profiles'

def init_vm(server_id,vm_id):
    sql = '''insert into vm_list(vm_id, server_id,status,enabled,vm_name)
    values({0},{1},{2},{3},"1")'''.format(vm_id,server_id,1,1)
    print sql
    ret = dbutil.execute_sql(sql)
    if not ret:
        print "sql:{0},execute failed".format(sql)
        
def init_server_group(server_id,group_id):
    sql = '''insert into vm_server_group(id,server_id,status)
           values({0},{1},{2})'''.format(group_id, server_id,1)     
    print sql 
    ret = dbutil.execute_sql(sql)
    if not ret:
        print "sql:{0},execute failed".format(sql)

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    init_vm(1001, 1)
    init_server_group(1001,1001)

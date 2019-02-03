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
def init():
    sql = '''insert into profiles(id, path) values({0},'{1}')
    on duplicate key update path="{1}" '''
    for i in xrange(1, 5001):
        print "get :%d",i
        path = os.path.join(profile_dir, str(i))
        print path
        sql_tmp = sql.format(i, path)
        print sql_tmp
        ret = dbutil.execute_sql(sql_tmp)
        if not ret:
            print "sql:{0},execute failed".format(sql_tmp)
        




if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    init()

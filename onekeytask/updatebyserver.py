# -*- coding:utf-8 -*-
#@Author: coldplay 
#@Date: 2017-08-15 14:43:18 
#@Last Modified by:   coldplay 
#@Last Modified time: 2017-08-15 14:43:18 
#
import sys
import ConfigParser
import optparse
import time
import os
import threading
import signal
import logging
import logging.config
import subprocess
import subprocess
import string
sys.path.append("..")
import dbutil

cmdid = 0
serverid = 0
utype = 0
sleep_time = 0

def init():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--ip", dest="db_ip", default="192.168.1.21",
            help="mysql database server IP addrss, default is 192.168.1.235" ) 
    parser.add_option("-n", "--name", dest="db_name", default="vm2",
            help="database name, default is gamedb" ) 
    parser.add_option("-u", "--usrname", dest="username", default="vm",
        help="database login username, default is chinau" )
    parser.add_option("-p", "--password" , dest="password", default="123456",
            help="database login password, default is 123" )
    parser.add_option("-l", "--logconf", dest="logconf", default="./updatebyserver.log.conf",
        help="log config file, default is ./updatebyserver.log.conf" )
    parser.add_option("-c", "--cmdid", dest="cmdid", default="",
        help="cmdid" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-t", "--updatetype", dest="utype", default="0",
        help="update type" )
    parser.add_option("-j", "--sleep", dest="sleeptime", default="5",
        help="sleep time" )
    (options, args) = parser.parse_args()

    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)
    global serverid
    serverid = int(options.serverid)

    global cmdid 
    cmdid = int(options.cmdid)
    
    global utype 
    utype = int(options.utype)
    global sleep_time 
    sleep_time = int(options.sleeptime)

    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info( options )
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    return True

def update_task():
    sql = "select  distinct server_id  from vm_update_task order by server_id"
    sql_vm ="select  id from vm_update_task where server_id=%d" 
    sql_up = "update vm_update_task set status=0 where server_id=%d and cmd_id=%d"
    sql_res = "select server_id,vm_id,status from vm_update_task where status=-1 and cmd_id=%d order by server_id,vm_id"
    try:
        if serverid == 0:
            res= dbutil.select_sql(sql)
            logger.info("select sql:%s", sql)
            if len(res) != 0:
                for r in res:
                    server_id = r[0]
                    sql_up_tmp = sql_up%(server_id, cmdid)
                    logger.info(sql_up_tmp)
                    ret = dbutil.execute_sql(sql_up_tmp)
                    time.sleep(1)
        else:
            server_id = serverid 
            sql_up_tmp = sql_up%(server_id, cmdid)
            logger.info(sql_up_tmp)
            ret = dbutil.execute_sql(sql_up_tmp)
            time.sleep(1)

        res = dbutil.select_sql(sql_res%(cmdid))
        for r in res:
            print "server_id:",r[0],"--vm_id:",r[1],"--status:",r[2]

    except:
        logger.error(
            'exception on main ', exc_info=True)

    print("still running!!!")

def onekey_task():
    sql = "select  distinct server_id  from vm_onekey_task order by server_id"
    sql_up = "update vm_onekey_task set status=0 where server_id=%d and cmd_id=%d"
    sql_res = "select server_id,status from vm_onekey_task where status=-1 and cmd_id=%d order by server_id"
    try:
        if serverid == 0:
            res= dbutil.select_sql(sql)
            logger.info("select sql:%s", sql)
            if len(res) != 0:
                for r in res:
                    server_id = r[0]
                    sql_up_tmp = sql_up%(server_id, cmdid)
                    logger.info(sql_up_tmp)
                    ret = dbutil.execute_sql(sql_up_tmp)
                    time.sleep(sleep_time)
        else:
            server_id = serverid 
            sql_up_tmp = sql_up%(server_id, cmdid)
            logger.info(sql_up_tmp)
            ret = dbutil.execute_sql(sql_up_tmp)
            time.sleep(sleep_time)

        res = dbutil.select_sql(sql_res%(cmdid))
        for r in res:
            print "server_id:",r[0],"--status:",r[1]

    except:
        logger.error(
            'exception on main ', exc_info=True)

    print("still running!!!")
def main():
    init()
    if utype == 0:
        onekey_task()
    else:
        update_task()


if __name__ == "__main__":
    main()

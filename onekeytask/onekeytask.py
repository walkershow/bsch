##
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
sys.path.append("..")
import dbutil

groupid = 0
serverid = 0

def init():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--ip", dest="db_ip", default="192.168.1.21",
            help="mysql database server IP addrss, default is 192.168.1.235" ) 
    parser.add_option("-n", "--name", dest="db_name", default="vm",
            help="database name, default is gamedb" ) 
    parser.add_option("-u", "--usrname", dest="username", default="vm",
        help="database login username, default is chinau" )
    parser.add_option("-p", "--password" , dest="password", default="123456",
            help="database login password, default is 123" )
    parser.add_option("-l", "--logconf", dest="logconf", default="./vm-onekeytask.log.conf",
        help="log config file, default is ./vm_onekeytask.log.conf" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    (options, args) = parser.parse_args()
    global serverid
    serverid = options.serverid

    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)

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

def main():
    init()
    sql = "select a.cmd,b.id from vm_cmd a,vm_onekey_task b where a.id=b.cmd_id and b.server_id=%s and b.status=0 order by b.sort_order"
    sql_up = "update vm_onekey_task set status=%d where id=%d"
    while True:
        try:
            sql_sel =  sql%(serverid)
            res= dbutil.select_sql(sql_sel)
            logger.info("select sql:%s", sql_sel) 
            for r in res:
                cmd = r[0].encode("gbk")
                id = r[1]
                ret = os.system(cmd)
                logger.info("cmd:%s run ret:%d ", r[0], ret)
                if ret==0:
                    sql_upstatus = sql_up%(1, id)
                else:
                    sql_upstatus = sql_up%(-1, id)
                ret = dbutil.execute_sql(sql_upstatus)
                logger.info("update sql:%s, ret:%d", sql_upstatus, ret) 
                time.sleep(5)
        except:
            logger.error(
                'exception on reset ', exc_info=True)
            time.sleep(5)
            continue

        time.sleep(10)
        print("still running!!!")


if __name__ == "__main__":
    main()
#!/usr/bin/env python
# coding=utf-8
import os
import time
import optparse
import ConfigParser
import dbutil
from tv import dial, dialoff

ip, area_name = "", ""
server_id = None
db = None


def record_vpn_ip_areaname(status, ip, areaname):
    sql = '''update vpn_status set vpnstatus={3},ip={0},area_name={1} where
    server_id={2}'''.format(ip, areaname, server_id, status)
    logger.info(sql)
    ret = db.execute_sql(sql)
    if ret < 0:
        logger.error("sql:%s, ret:%d", sql, ret)


def dialvpn():
    print "dial before start task"
    if dialoff():
        record_vpn_ip_areaname(2, ip, area_name)
    ip, area_name = dial()
    print ip, area_name
    if not ip:
        raise Exception,"dial unsuccessful"
    else:
        print "dial succ and get ip:", ip
        record_vpn_ip_areaname(1, ip, area_name)
    print "dial end"


def init():
    global server_id, db
    parser = optparse.OptionParser()
    parser.add_option("-s", "--server_id", dest="serverid")
    (options, args) = parser.parse_args()
    server_id = options.serverid

    cf = ConfigParser.ConfigParser()
    workpath = os.getcwd()
    confpath = "{0}/conf/db.conf".format(workpath)
    cf.read(confpath)
    dbname = 'DB_vm'
    db_host = cf.get(dbname, "db_host")
    db_name = cf.get(dbname, "db_name")
    db_user = cf.get(dbname, "db_user")
    db_pwd = cf.get(dbname, "db_pwd")
    db_charset = cf.get(dbname, "db_charset")
    db = dbutil.DBUtil(None, db_host, 3306, db_name, db_user, db_pwd, db_charset)


def main():
    init()  #配置任务
    while True:
        try:
            dialvpn()  #执行任务
        except Exception, e:
            print(e)
            print('sleep 30s and redial')
            time.sleep(30)
            continue
            
        print ("sleep 5 mins")
        time.sleep(300)


if __name__ == "__main__":
    main()

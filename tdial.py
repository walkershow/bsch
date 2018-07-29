#!/usr/bin/env python
# coding=utf-8
import os
import time
import optparse
import ConfigParser
import dbutil
import chardet
from tv import dial, dialoff, is_pptp_succ

server_id = None
db = None

def is_vpn_2(binit):
    if binit:
        return True
    sql = '''select vpnstatus from vpn_status where serverid={0} and vpnstatus=2'''.format(server_id)
    res =db.select_sql(sql)
    if res and len(res)>0:
        return True
    return False

def record_vpn_ip_areaname(status, ip, areaname):
    sql = '''update vpn_status set vpnstatus={3},ip='{0}',area_name='{1}' where
    serverid={2}'''.format(ip, areaname.encode('utf-8'), server_id, status)
    ret = db.execute_sql(sql)
    if ret < 0:
        print("sql:%s, ret:%d", sql, ret)


def dialvpn():
    global ip, area_name
    ip, area_name = "", ""
    print "dial before start task"
    dialoff()
    record_vpn_ip_areaname(2, ip, area_name)
    ip, area_name = dial()
    if area_name is None:
        raise Exception,"area is None"
    print ip, area_name
    if not ip:
        raise Exception,"dial unsuccessful"
    elif area_name.find(u'汕头')!=-1 or area_name.find(u'CHINA')!=-1:
        raise Exception,"dial unsuccessful, area not correct"
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
    binit = True
    while True:
        try:
            #if is_vpn_2(binit):
                #dialvpn()  #执行任务
            if not is_pptp_succ() or is_vpn_2(binit):
                print "sleep 40s then redial..."
                time.sleep(60)
                record_vpn_ip_areaname(2, "", "")
                dialvpn()
        except Exception, e:
            print(e)
            print('sleep 10s and redial')
            time.sleep(10)
            continue
        binit = False
        print ("sleep 5 mins")
        time.sleep(5)


if __name__ == "__main__":
    main()

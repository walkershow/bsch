# -*- coding: utf-8 -*-
'''
 @Author: coldplay
 @Date: 2017-04-12 14:29:23
 @Last Modified by:   coldplay
 @Last Modified time: 2017-04-12 14:29:23
'''
import sys
import optparse
import os
import logging
import logging.config
sys.path.append("..")
import dbutil

vm_id = 0
server_id = 0
logger = None


def autoargs():
    global vm_id, server_id
    cur_cwd = os.getcwd()
    dirs = cur_cwd.split('\\')
    vmname = dirs[-2]
    vm_id = int(vmname[1:])
    server_id = int(dirs[-3])
    logger.info("get vmid,serverid from cwd:%s,%s", vm_id, server_id)


def get_default_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # console logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def init():
    parser = optparse.OptionParser()
    parser.add_option(
        "-i",
        "--ip",
        dest="db_ip",
        default="192.168.1.21",
        help="mysql database server IP addrss, default is 192.168.1.235")
    parser.add_option(
        "-n",
        "--name",
        dest="db_name",
        default="vm3",
        help="database name, default is gamedb")
    parser.add_option(
        "-u",
        "--usrname",
        dest="username",
        default="vm",
        help="database login username, default is chinau")
    parser.add_option(
        "-p",
        "--password",
        dest="password",
        default="123456",
        help="database login password, default is 123")
    parser.add_option(
        "-v",
        "--vid",
        dest="vmid",
        default="0",
        help="log config file, default is 0")
    parser.add_option(
        "-s",
        "--serverid",
        dest="serverid",
        default="0",
        help="log config file, default is 0")
    (options, args) = parser.parse_args()
    global vm_id
    vm_id = int(options.vmid)
    global server_id
    server_id = int(options.serverid)

    if vm_id == 0 or server_id == 0:
        autoargs()
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    return True


def del_cookie_file(pdir, cookie_file):
    try:
        full_path = os.path.join(pdir, cookie_file)
        print full_path
        if not os.path.exists(full_path):
            return True
        os.remove(os.path.join(pdir, cookie_file))
    except IOError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
        return False
    return True


def left_profiles():
    lp = []
    sql = '''SELECT
                a.id
          FROM
              vm_users a,
              profiles b
              WHERE
                a.create_time IS NOT NULL
                AND a.server_id = {0}
                AND a.vm_id = {1}
                and a.profile_id = b.id
                and a.user_type = 0
                GROUP BY
                    date_format(a.create_time, '%Y-%m-%d %H'),
                    a.server_id,
                    a.vm_id,
                    a.terminal_type
                    '''.format(server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        for r in res:
            path = r[0]
            lp.append(path)
    return lp


def all_profiles():
    ap = []
    sql = ''' select a.id
            FROM
                vm_users a,
                  profiles b
            WHERE
                 a.server_id = {0}
            AND a.vm_id = {1}
            and a.profile_id = b.id
            '''.format(server_id, vm_id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        for r in res:
            path = r[0]
            ap.append(path)
    return ap


def del_profile(id):
    logger.info("start del profile id:%d", id)
    sql = "select b.path from vm_users a, profiles b where a.profile_id=b.id "\
        "and a.id={0} ".format(id)
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if res:
        path = res[0][0]
        if not del_cookie_file(path, "cookies.sqlite"):
            logger.info("end del profile id:%d", id)
            return
    sql = "delete from vm_users where id={0}".format(id)
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception, "%s exec failed ret:%d" % (sql, ret)
    logger.info("end del profile id:%d", id)


def deleted_profiles():
    ap = all_profiles()
    lp = left_profiles()
    dp = list(set(ap).difference(set(lp)))
    print len(dp)
    if dp:
        for i in dp:
            del_profile(i)


if __name__ == '__main__':
    global logger
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_logger()
    init()
    deleted_profiles()

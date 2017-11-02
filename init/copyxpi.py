# -*- coding: utf-8 -*-

'''
 @Author: coldplay 
 @Date: 2017-04-12 14:29:23 
 @Last Modified by:   coldplay 
 @Last Modified time: 2017-04-12 14:29:23 
'''
import sys
import ConfigParser
import datetime
import optparse
import os
import shutil
import time
import random
import fnmatch
import threading
import signal
import logging
import logging.config
import ctypes
sys.path.append("..")
import dbutil
import subprocess
import re
import shutil

vm_id = 0
server_id = 0
xpi_dir = ""
tty = 1
xpi_files=[]

def autoargs():
    global vm_id, server_id
    cur_cwd = os.getcwd()
    dirs = cur_cwd.split('\\')
    vmname = dirs[-3]
    vm_id= int(vmname[1:])
    server_id= int(dirs[-4])
    logger.info("get vmid,serverid from cwd:%s,%s",vm_id, server_id)


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
    parser.add_option("-l", "--logconf", dest="logconf", default="./copyxpi.log.conf",
        help="log config file, default is ./copyxpi.log.conf" )
    parser.add_option("-v", "--vid", dest="vmid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-s", "--serverid", dest="serverid", default="0",
        help="log config file, default is 0" )
    parser.add_option("-x", "--xpidir", dest="xpidir", default="d:\\xpi",
        help="src xpi dir" )
    parser.add_option("-t", "--tty", dest="tty", default="1",
        help="default tty is 1" )

    (options, args) = parser.parse_args()
    global vm_id
    vm_id = int(options.vmid)
    global server_id,xpi_dir,tty
    server_id = int(options.serverid)
    print options.xpidir

    xpi_dir = options.xpidir
    if not os.path.exists(options.logconf):
        print 'no exist:', options.logconf
        sys.exit(1)
    tty = int(options.tty)

    
    logging.config.fileConfig(options.logconf)
    global logger
    logger = logging.getLogger()
    logger.info( options )
    if vm_id == 0 or server_id == 0:
        autoargs()
    dbutil.db_host = options.db_ip
    dbutil.db_name = options.db_name
    dbutil.db_user = options.username
    dbutil.db_pwd = options.password
    dbutil.logger = logger
    return True


def file_extension(path): 
  return os.path.splitext(path)[1] 

def getsrcxpi():
    print xpi_dir
    global xpi_files
    for f in os.listdir(xpi_dir):
        # print fo
        if file_extension(f) == ".xpi":
            xpi_files.append(f)

def getrandxpi():
    xpi = random.choice(xpi_files)
    print xpi
    xpi_path = os.path.join(xpi_dir ,xpi)
    return xpi_path

def delxpi(old):
    cmd = "del /f/s/q %s"%(old)
    logger.info("delxpi:%s", cmd)
    ret = os.system(cmd)
    logger.info("cmd:%s ret=%d",  cmd, ret)
    if 0 != ret:
        return 1
    return 0

def copyxpi(old, new):
    if os.path.exists(new):
        os.remove(new)
    shutil.copyfile(old, new)
    # cmd = "xcopy %s %s /y" %(old, new)
    # logger.info("copyxpi:%s", cmd)
    # ret = os.system(cmd)
    # logger.info("cmd:%s ret=%d",  cmd, ret)
    # if 0 != ret:
    #     return 1
    # return 0

def replace0002(file_path):
    #将文件读取到内存中
    with open(file_path,"r") as f:
        lines = f.readlines() 
    #写的方式打开文件
    with open(file_path,"w") as f_w:
        for line in lines:
            if "uaChosen" in line:
                line = re.sub(r'uaChosen", ".*"', 'uaChosen", "0,0,0"', line)
                print line
            # print line
            #替换
            f_w.write(line)

def replace000(file_path):
    f = open (file_path, "r+")
    # content = re.sub(r'mouth, ".*"', '""', content)
    # sre = r'"extensions.agentSpoof.uaChosen", "(.*)"'
    # sre = 'uaChosen", "\(.*\)"'
    # tre = 'uaChosen", "\(0,0,0\)"'
    sre = 'uaCh'
    tre = 'uaC'
    open(file_path, 'w').write(re.sub(sre, tre, f.read()))

def main():
    init()
    getsrcxpi()
    if not xpi_files:
        print "xpi is empty"
        exit(0)
    xpi_count = len(xpi_files)
    profile_name = "jid1-AVgCeF1zoVzMjA@jetpack.xpi"
    if tty == 1:
        tty_str = "1,3"
        print "pc xpi files count:", xpi_count
    elif tty == 2:
        tty_str = "2,4"
        print "wap xpi files count:", xpi_count
    else:
        print "unkown tty", tty
        exit(0)
    sql = "select a.path,a.terminal_type from profiles a,vm_profiles b where"\
    " a.id=b.profile_id and b.server_id=%d and b.vm_id=%d and a.terminal_type in(%s)  order by b.profile_id"%(
        server_id, vm_id, tty_str
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    for r in res:
        path = r[0]
        ttype = r[1]
        prefs_path = os.path.join(path, 'prefs.js')
        replace0002(prefs_path)
        
        ext_path = os.path.join(path, "extensions")
        # print profile_name
        target_xpi = os.path.join(ext_path, profile_name)
        randxpi = getrandxpi()
        # delxpi(target_xpi)
        copyxpi(randxpi, target_xpi)

if __name__ == "__main__":
    main()
    # content = 'I opened my mouth, "Good morning!" I said cheerfully'
    # content = 'user_pref("extensions.agentSpoof.uaChosen", "random_desktop")'
    # content = re.sub(r'uaChosen", ".*"', 'uaChosen", "ddd"', content)
    # print(content)

    # replace0002("d:\prefs.js")
    # replace000(r"d:\prefs.js")
    # replace001(r"d:\1.txt")
    # main()


        
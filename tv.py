#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : tv.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 09.07.2018 15:04:1531119896
# Last Modified Date: 10.07.2018 17:47:1531216032
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
# coding=utf-8
import os
import time
import requests
import traceback
import json

def is_pptp_succ():
    message = os.popen('ip a|grep ppp').readlines()
    if message:
        inet = message[-1]
        if inet.find("inet")!=-1:
            print 'succ'
            return True 
        return False

def add_route():
    cmd = "route add default dev ppp0"
    ret = os.system(cmd)
    return ret

def dial():
    cmd = "pon debo"
    ret = os.system(cmd)
    if ret != 0:
        return None,None
    for i in range(10):
        if is_pptp_succ():
            ip,area_name = get_dialup_ip()
            if ip:
                add_route()
            return ip,area_name
        time.sleep(5)
    return None,None

def dialoff():
    ret = False
    if is_pptp_succ():
        cmd = "pkill pptp"
        ret = os.system(cmd)
        ret = True
    return True if ret else False

def get_dialup_ip():
    try:
        url = 'http://pv.sohu.com/cityjson?ie=utf-8'
        r = requests.get(url, timeout=5)
    except requests.exceptions.Connectimeout:
        print "connect timeout"
        return None,None
    retstr = r.content[19:].strip(";")
    t = json.loads(retstr)
    return t['cip'],t['cname']


def main():
    ip, name = dial()
    print ip,name
    if ip:
        print 'dial up'
    else:
        print "dial failed"

if __name__ == "__main__":
    while True:
        try:
            # clear_by_hours('d:\\profiles')
            print "dial..."
            # get_dialup_ip()
            main()
            print "dial ok..."
        except Exception, e:
            print 'traceback.print_exc():'
            traceback.print_exc()
            time.sleep(5)

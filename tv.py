#!/usr/bin/env python
# coding=utf-8
import os
import time
import traceback

def is_pptp_succ():
    message = os.popen('ip a|grep ppp').readlines()
    if message:
        inet = message[-1]
        if inet.find("inet")!=-1:
            print 'succ'
            return True 
        return False

def dial():
    if is_pptp_succ():
        dialoff()
    cmd = "pon debo"
    ret = os.system(cmd)
    if ret != 0:
        return False
    for i in range(10):
        if is_pptp_succ():
            return True
        time.sleep(5)
    return False

def dialoff():
    cmd = "pkill pptp"
    ret = os.system(cmd)
    return True if ret else False

def main():
    dial()

if __name__ == "__main__":
    while True:
        try:
            # clear_by_hours('d:\\profiles')
            print "dial..."
            main()
            print "dial ok..."
        except Exception, e:
            print 'traceback.print_exc():'
            traceback.print_exc()
            time.sleep(5)

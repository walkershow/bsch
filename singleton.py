#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : singleton.py
# Author            : coldplay <coldplay_gz@sina.cn>
# Date              : 18.05.2018 14:35:1526625357
# Last Modified Date: 18.05.2018 14:35:1526625357
# Last Modified By  : coldplay <coldplay_gz@sina.cn>
import sys
import time
from win32api import CloseHandle, GetLastError
from win32event import CreateMutex
from winerror import ERROR_ALREADY_EXISTS


class singleinstance: 
    """ Limits application to single instance """

    def __init__(self, name):
        self.mutexname = "testmutex_{%s}" %(name)
        print(self.mutexname)
        self.mutex = CreateMutex(None, False, self.mutexname) 
        self.lasterror = GetLastError()
      
    def aleradyrunning(self):
        return (self.lasterror == ERROR_ALREADY_EXISTS) 
    
    def run(self):
        if self.aleradyrunning():
            for i in range(10):
                time.sleep(3)
                if self.mutex:
                    CloseHandle(self.mutex) 
                self.lasterror = 0
                self.mutex = CreateMutex(None, False, self.mutexname)
                self.lasterror = GetLastError()
                if self.aleradyrunning():
                    print("Can't get lock")
                else:
                    return
            sys.exit(0)

    def __del__(self): 
        if self.mutex:
            CloseHandle(self.mutex) 

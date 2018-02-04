# -*- coding: utf-8 -*-
import re
import urllib2
import platform


class GetOutip:
    def getip(self):
        try:
            myip = self.visit("http://ip.cha127.com/")
        except Exception, e:
            try:
                myip = self.visit("http://www.ip138.com/ip2city.asp")
            except Exception, e:
                myip = "no ip!!!"
        return myip

    def visit(self, url):
        opener = urllib2.urlopen(url, timeout=1)
        if url == opener.geturl():
            str = opener.read()
        return re.search('\d+\.\d+\.\d+\.\d+', str).group(0)


def auto_encoding(str_enc):
    sysstr = platform.system()
    if sysstr == 'Windows':
       str_enc = str_enc.decode('utf-8').encode('gbk')
    return str_enc

import os  
import fcntl  
  
class Lock:   
    def __init__(self, filename):  
        self.filename = filename  
        # This will create it if it does not exist already  
        self.handle = open(filename, 'w')  
      
    # Bitwise OR fcntl.LOCK_NB if you need a non-blocking lock   
    def acquire(self):  
        fcntl.flock(self.handle, fcntl.LOCK_EX)  
          
    def release(self):  
        fcntl.flock(self.handle, fcntl.LOCK_UN)  
          
    def __del__(self):  
        self.handle.close()  

# print "getmyip"
# getmyip = GetOutip()
# localip = getmyip.getip()
# print localip

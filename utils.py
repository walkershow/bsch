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
import time
import errno
import random

def random_pick(some_list,probabilities):
    x=random.uniform(0,1)
    cumulative_probability=0.0
    for item,item_probability in zip(some_list,probabilities):
        cumulative_probability+=item_probability
        if x < cumulative_probability: break
    return item   

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


class SimpleFlock:
    """Provides the simplest possible interface to flock-based file locking. Intended for use with the `with` syntax. It will create/truncate/delete the lock file as necessary."""

    def __init__(self, path, timeout = None):
        self._path = path
        self._timeout = timeout
        self._fd = None

    def __enter__(self):
        self._fd = os.open(self._path, os.O_CREAT)
        start_lock_search = time.time()
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired!
                return
            except IOError, ex:
                if ex.errno != errno.EAGAIN: # Resource temporarily unavailable
                    raise
                elif self._timeout is not None and time.time() > (start_lock_search + self._timeout):
                    # Exceeded the user-specified timeout.
                    # return False
                    raise

            # TODO It would be nice to avoid an arbitrary sleep here, but spinning
            # without a delay is also undesirable.
            time.sleep(0.1)

    def __exit__(self, *args):
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None

    def __del__(self):  
        if self._fd:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None

if __name__ == "__main__":
    # print "Acquiring lock..."
    # with SimpleFlock("locktest", 2):
        # print "Lock acquired."
        # time.sleep(10)
    # print "Lock released."
# print "getmyip"
# getmyip = GetOutip()
# localip = getmyip.getip()
# print localip
    a=[0,1]
    b=[0.1,0.9]
    while True:
        ret = random_pick(a,b)
        print ret

# -*- coding: utf-8 -*-
import re,urllib2

class GetOutip:
    def getip(self):
        try:
            myip = self.visit("http://ip.cha127.com/")
        except Exception,e:
            try:
                myip = self.visit("http://www.ip138.com/ip2city.asp")
            except Exception,e:
                myip = "no ip!!!"
        return myip
    def visit(self,url):
        opener = urllib2.urlopen(url,timeout=1)
        if url == opener.geturl():
            str = opener.read()
        return re.search('\d+\.\d+\.\d+\.\d+',str).group(0)

print "getmyip"
getmyip = GetOutip()
localip = getmyip.getip()
print localip
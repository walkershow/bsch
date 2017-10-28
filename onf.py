#coding=utf8
import urllib
import requests
import sys
import os

server_id,vm_id = 0,0

def autoargs():
    global vm_id, server_id
    cur_cwd = os.getcwd()
    dirs = cur_cwd.split('\\')
    vmname = dirs[-2]
    vm_id= int(vmname[1:])
    server_id= int(dirs[-3])
    print("get vmid,serverid from cwd:%s,%s",vm_id, server_id)

if __name__ == '__main__':
    if len(sys.argv)>1:
            server_id = int(sys.argv[1])
            vm_id = int(sys.argv[2])
    else:
        autoargs()
    if server_id ==0 or vm_id ==0:
        print "server id or vm id is 0"
        exit(0)
    
    url="http://192.168.1.21/vm/getprofile3?serverid=%d&vmid=%d"%(server_id, vm_id) 
    print url
    return_data = requests.get(url)
    print return_data
    profile = return_data.text
    print profile
    if profile.strip() == 'nil':
        print 'nil...'
        exit(0)
    cmd = "start "+ profile
    print cmd
    os.system(cmd)
    # exit(0)
# -*- coding:utf-8 -*-
import socket
import time
import dbutil

dbutil.db_host = "192.168.1.21"
dbutil.db_name = "vm"
dbutil.db_user = "vm"
dbutil.db_port = 3306
dbutil.db_pwd = "123456"

# sql = "insert into company_ip(id,ip,update_time) values(1,'%s',CURRENT_TIMESTAMP) on duplicate key update ip='%s',\
#     update_time=CURRENT_TIMESTAMP "
while True:
    ip_port = ('120.76.220.126',8009)
    sk = socket.socket()
    sk.connect(ip_port)
    data = sk.recv(1024)
    print 'receive:',data
    # sql_tmp = sql %(data,data)
    # print sql_tmp
    # ret = dbutil.execute_sql(sql_tmp)
    # if ret<0:
    #     print "sql",sql,"execute error"
    # inp = input('please input:')
    # sk.sendall('hello')
    # if inp == 'exit':
        # break
    sk.close()
    # time.sleep()

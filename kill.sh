ps -ef |grep vm-sch |awk '{print $2}'|xargs kill -9

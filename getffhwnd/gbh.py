
#coding=utf8
import win32gui
import win32api
import win32con
import time
import datetime
import psutil
import re
import exceptions
classname = "MozillaWindowClass"
titlename = u"Mozilla Firefox"
titlename2 = u""
logger = None
def list_pids():
    pids = psutil.pids()
    for pid in pids:
        try:
            p = psutil.Process(pid)
            print("pid-%d,pname-%s,ppid-%d" %(pid,p.name(),p.ppid()))
            pp = psutil.Process(p.ppid())
            if pp:
                logger.info("pid-%d,pname-%s,ppname:%s,ppid-%d" %(pid,p.name(),pp.name(),p.ppid()))
            else:
                logger.info("pid-%d,pname-%s,ppid-%d" %(pid,p.name(),p.ppid()))
        except: 
            logger.error("process id:%d no longer exist",pid)
            # logger.error('exception on list_pids', exc_info = True)
            continue


def get_pid(name, pids):
    ff_pids = []
    for p in psutil.process_iter():
        if p.name().lower() == name.lower():
            # list_pids()
            print "===================="
            print p.name(),p.pid,p.ppid()
            logger.info("====================")
            logger.info("get the proc name:%s,pid:%d,ppid:%d",p.name(),p.pid, p.ppid())
            mem = p.memory_info()
            print mem
            logger.info("rss:%d, vms:%d",mem.rss, mem.vms)
            
            if p.pid not in pids :
                pp = get_p_by_pid(p.ppid())
                if not pp:
                    print ("the parent id is gone:%d", p.ppid())
                    logger.info("the parent id is gone:%d", p.ppid())
                    ff_pids.append(int(p.pid))
                    # return p.pid
                # find_ret = pp.name().lower().find( name.lower() )
                # print "find_ret", find_ret
                elif pp.name().lower() != name.lower():
                    logger.info("pp.name:%s != name:%s,ppid:%d", pp.name(),name,pp.pid)
                    print "ppid:",pp.pid, "ppname:",pp.name()
                    print p.pid
                    ff_pids.append(int(p.pid))
                    # return p.pid
                else:
                    continue
            else:
                logger.info("==========")
                logger.info("pid:%d is exists", p.pid)
    # return 0
    return ff_pids

def get_p_by_pid(pid ):
    for p in psutil.process_iter():
        # print "explorer.exe pid:%d", ppid
        if p.pid == pid :
            return p
    return None

def isRealWindows(hwnd):
    if not win32gui.IsWindowVisible(hwnd):
        # print "not real win:",hwnd
        return False
    if win32gui.GetParent(hwnd) != 0 :
        # print "have parent:", hwnd
        return False
    return True

def getwin(title):
    def callback(hwnd, windows):
        if not isRealWindows(hwnd):
            return
        text = win32gui.GetWindowText(hwnd)
        # print text,title
        pp =  unicode(text,'gbk')
        # print unicode(text,'gbk')
        if pp.find(title)!=-1:
            # phwnd = win32gui.GetParent()
            windows.append((hwnd, text))

    windows = []
    win32gui.EnumWindows(callback, windows)
    return windows


def find_ff_hwnd(hwnds, ds=90):
    init_time = datetime.datetime.now() 
    hwnd = 0
    while True:
        wins = getwin(titlename) 
        if wins:
            hwnd = wins[0][0]
        cur_time = datetime.datetime.now()
        delta = cur_time - init_time
        if delta.seconds>ds:
            return 0
        # print delta
        if not wins:
            print "continue to find hwnd"
            time.sleep(3)
            continue
        if hwnd not in hwnds:
            return hwnd
            print "log ff hwnd",hwnd
        time.sleep(3)
    return hwnd

#获取句柄
def find_ff_hwnd2(hwnds, ds=90):
    init_time = datetime.datetime.now() 
    while True:
        hwnd = win32gui.FindWindow(classname, titlename)
        if not hwnd:
            hwnd = win32gui.FindWindow(classname, titlename2)
        cur_time = datetime.datetime.now()
        delta = cur_time - init_time
        if delta.seconds>ds:
            return 0
        # print delta
        if not hwnd:
            print "continue to find hwnd"
            time.sleep(3)
            continue
        if hwnd not in hwnds:
            return hwnd
            print "log ff hwnd",hwnd
        time.sleep(3)
    return 0

def close_ff_win(hwnd):
    try:
        # if win32gui.IsWindow(hwnd):
        # redirect(hwnd)
        refresh(hwnd)
        time.sleep(20)

        logger.info("postmessage to close win:%d",hwnd)
        win32gui.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
        # else:
        #     logger.info("close ff win the hwnd is not windows:%d", hwnd)

    except:
        print "close ff exp",hwnd

def close_ff(hwnd, hwnds):
    # if not win32gui.IsWindow(hwnd):
    #     logger.info("close ff the hwnd is not windows:%d", hwnd)
    #     return False
    # i = 0 
    # while True:
    logger.info("close ff win:%d",hwnd)
    # redirect(hwnd)
    # time.sleep(3)
    close_ff_win(hwnd)
    # if i>4:
    #     logger.info("retry to close ff time is out,return failed:%d", hwnd)
    #     return False
    if win32gui.IsWindow(hwnd):
        logger.info("ff:%d still exists", hwnd)
        # i = i+1
        # time.sleep(5)
    else:
        logger.info("close ff succ:%d", hwnd)
        
        # return True

def refresh(hwnd):
    print ("refresh ......")
    logger.info("refresh.......")
    win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_F5, 0)#发送F9键  
    win32gui.SendMessage(hwnd, win32con.WM_KEYUP, win32con.VK_F5, 0)  

def mouseclick(hwnd):
    print("redirect to www.baidu.com......")
    win32gui.SendMessage(hwnd, win32con.WM_LBUTTONDOWN,  0, 138&(67<<16))#发送F9键  
    win32gui.SendMessage(hwnd, win32con.WM_LBUTTONUP,0, 138&(67<<16))  
    str = "www.baidu.commmm"
    for c in str:
        # print c
        win32gui.SendMessage(hwnd, win32con.WM_CHAR, ord(c), 0)
    # time.sleep(2)
    print getwin(titlename)
    win32gui.SendMessage(hwnd,win32con.WM_KEYDOWN,  win32con.VK_RETURN,0)
    win32gui.SendMessage(hwnd,win32con.WM_KEYUP,  win32con.VK_RETURN,0)

def redirect(hwnd):
    # win32gui.PostMessage(hwnd,win32con.WM_SYSKEYDOWN,68,1<<29); 
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_CONTROL, 0)#发送F9键  
    # logger.info("redirect to www.baidu.com......")
    print("redirect to www.baidu.com......")
    win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_F4, 0)#发送F9键  
    win32gui.SendMessage(hwnd, win32con.WM_KEYUP, win32con.VK_F4, 0)  
    str = "www.baidu.com"
    for c in str:
        # print c
        win32gui.SendMessage(hwnd, win32con.WM_CHAR, ord(c), 0)
    # time.sleep(2)
    win32gui.SendMessage(hwnd,win32con.WM_KEYDOWN,  win32con.VK_RETURN,0)
    win32gui.SendMessage(hwnd,win32con.WM_KEYUP,  win32con.VK_RETURN,0)
    
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_CONTROL, 0)  
    # win32api.keybd_event(win32con.VK_LCONTROL,0, 0, 0);  
    # win32gui.PostMessage(hwnd, win32
    # con.WM_KEYDOWN, win32con.VK_F4, 0x001E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_F4, 0xC01E0001);   

    # win32api.keybd_event(win32con.VK_LCONTROL,0, win32con.KEYEVENTF_KEYUP, 0); 
      
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, 72, 0x001E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 72, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, 84, 0x001E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 84, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, 84, 0x001E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 84, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, 80, 0x001E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 80, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0x001E0001);   
    # # win32gui.PostMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, win32api.MapVirtualKey(win32con.VK_RETURN,0), 0xC01E0001);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 13, 0);   
    # win32gui.PostMessage(hwnd, win32con.WM_KEYUP, 75, 0xC01E0001);   


if __name__ == '__main__':
    # hwnd = find_ff_hwnd([])
    # print hwnd
    # print type(hwnd)
    # close_ff(int(hwnd))
    import sys
    hwnd = sys.argv[1]
    print int(hwnd)
    h = find_ff_hwnd([], 5)
    print h
    wins = getwin(titlename)
    print wins
    print    wins[0][0]
    h = wins[0][0]
    # mouseclick(h)
    # mouseclick(h)
    redirect(h)
    # refresh(h)
    # activeAddrBar(h)
    # close_ff_win(int(hwnd))
    # print win32gui.IsWindow(265748)
    # if not win32gui.IsWindow(265748):
    #     print "nono"
    # if win32gui.IsWindow(265748):
    #     print "gogo"
    # print win32gui.IsWindow(96996)
    # list_pids()

    # print get_pid("firefox.exe")

# -*- encoding: utf-8 -*-

import atexit
import os
import sys
import time
import signal
from multiprocessing import Pool
from datetime import datetime
from dateutil.relativedelta import relativedelta
from PB.pb_io import find_file, str_format
from configobj import ConfigObj

# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/L1.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)

g_path_h8dat = g_var_cfg['PATH']['IN']['h8_dat']

# description: 一个守护进程的简单包装类, 具备常用的start|stop|restart|status功能, 使用方便
#             需要改造为守护进程的程序只需要重写基类的run函数就可以了
# date: 2015-10-29
# usage: 启动: python daemon_class.py start
#       关闭: python daemon_class.py stop
#       状态: python daemon_class.py status
#       重启: python daemon_class.py restart
#       查看: ps -axj | grep daemon_class


class CDaemon:
    '''
    a generic daemon class.
    usage: subclass the CDaemon class and override the run() method
    stderr  表示错误日志文件绝对路径, 收集启动过程中的错误日志
    verbose 表示将启动运行过程中的异常错误信息打印到终端,便于调试,建议非调试模式下关闭, 默认为1, 表示开启
    save_path 表示守护进程pid文件的绝对路径
    '''

    def __init__(self, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022, verbose=1):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = save_path  # pid文件绝对路径
        self.home_dir = home_dir
        self.verbose = verbose  # 调试开关
        self.umask = umask
        self.daemon_alive = True

    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' %
                             (e.errno, e.strerror))
            sys.exit(1)

        os.chdir(self.home_dir)
        os.setsid()
        os.umask(self.umask)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' %
                             (e.errno, e.strerror))
            sys.exit(1)

        sys.stdout.flush()
        sys.stderr.flush()

        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        if self.stderr:
            se = file(self.stderr, 'a+', 0)
        else:
            se = so

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        def sig_handler(signum, frame):
            self.daemon_alive = False
        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)

        if self.verbose >= 1:
            print 'daemon process started ...'

        atexit.register(self.del_pid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write('%s\n' % pid)

    def get_pid(self):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
        except SystemExit:
            pid = None
        return pid

    def del_pid(self):
        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

    def start(self, *args, **kwargs):
        self.run(*args, **kwargs)
        if self.verbose >= 1:
            print 'ready to starting ......'
        # check for a pid file to see if the daemon already runs
        pid = self.get_pid()
        if pid:
            msg = 'pid file %s already exists, is it already running?\n'
            sys.stderr.write(msg % self.pidfile)
            sys.exit(1)
        # start the daemon
        # self.daemonize()
        self.run(*args, **kwargs)

    def stop(self):
        if self.verbose >= 1:
            print 'stopping ...'
        pid = self.get_pid()
        if not pid:
            msg = 'pid file [%s] does not exist. Not running?\n' % self.pidfile
            sys.stderr.write(msg)
            if os.path.exists(self.pidfile):
                os.remove(self.pidfile)
            return
        # try to kill the daemon process
        try:
            i = 0
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
                i = i + 1
                if i % 10 == 0:
                    os.kill(pid, signal.SIGHUP)
        except OSError, err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)
            if self.verbose >= 1:
                print 'Stopped!'

    def restart(self, *args, **kwargs):
        self.stop()
        self.start(*args, **kwargs)

    def is_running(self):
        pid = self.get_pid()
        # print(pid)
        return pid and os.path.exists('/proc/%d' % pid)

    def run(self, *args, **kwargs):
        'NOTE: override the method in subclass'
        print 'base class run()'


class ClientDaemon(CDaemon):

    def __init__(self, name, save_path, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, home_dir='.', umask=022, verbose=1):
        CDaemon.__init__(
            self, save_path, stdin, stdout, stderr, home_dir, umask, verbose)
        self.name = name  # 派生守护进程类的名称

    def run(self, output_fn, **kwargs):
        job_name_list = ['HIMAWARI_AHI_4000_BROADCAST',
                         'HIMAWARI_AHI_1000_BROADCAST',
                         'HIMAWARI_AHI_2000_ORIGINAL',
                         'HIMAWARI_AHI_1000_ORIGINAL',
                         'HIMAWARI_AHI_500_ORIGINAL']
        job_name_list = ['HIMAWARI_AHI_1000_BROADCAST']
        while True:

            pool = Pool(processes=1)
            for job_name in job_name_list:
                print job_name
                pool.apply_async(get_file_list, (job_name,))
            pool.close()
            pool.join()
            time.sleep(3)
#         fd = open(output_fn, 'w')
#         while True:
#             line = time.ctime() + '\n'
#             fd.write(line)
#             fd.flush()
#             time.sleep(1)
#         fd.close()


def HIMAWARI_AHI_1000_BROADCAST():
    pass


def HIMAWARI_AHI_4000_BROADCAST():
    pass


def HIMAWARI_AHI_2000_ORIGINAL():
    pass


def HIMAWARI_AHI_1000_ORIGINAL():
    pass


def HIMAWARI_AHI_500_ORIGINAL():
    pass


def get_file_list(job_name):

    date_start = (datetime.utcnow() - relativedelta(minutes=40))
    date_end = datetime.utcnow()

    date1 = datetime.strptime(date_start.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_end.strftime('%Y%m%d'), '%Y%m%d')
#     date2 = date2 + relativedelta(days=1)

    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        date1 = date1 + relativedelta(das=1)
        sat_dtype = job_name.split('_')[-1]
        in_path = os.path.join(g_path_h8dat, ymd)
        in_path = str_format(in_path, {'H8_TYPE': sat_dtype})


def get_job_func(job_name):
    """
    u 返回jobid对应的函数名称 ，jobid唯一性
    :return:
    """
    job_id_func = {
        "HIMAWARI_AHI_4000_BROADCAST": HIMAWARI_AHI_4000_BROADCAST,
        "HIMAWARI_AHI_1000_BROADCAST": HIMAWARI_AHI_1000_BROADCAST,
        "HIMAWARI_AHI_2000_ORIGINAL": HIMAWARI_AHI_2000_ORIGINAL,
        "HIMAWARI_AHI_1000_ORIGINAL": HIMAWARI_AHI_1000_ORIGINAL,
        "HIMAWARI_AHI_500_ORIGINAL": HIMAWARI_AHI_500_ORIGINAL,
    }
    return job_id_func.get(job_name)


def run_job(job_name):

    if not os.path.isfile('./test.log'):
        fd = open('./test.log', 'w')
    else:
        fd = open('./test.log', 'a+')
    line = time.ctime() + '\n'
    fd.write(line)
    fd.write('%s\n' % job_name)
    fd.flush()
    fd.close()

if __name__ == '__main__':
    help_msg = 'Usage: python %s <start|stop|restart|status>' % sys.argv[0]
    if len(sys.argv) != 2:
        print help_msg
        sys.exit(1)
    p_name = 'clientd'  # 守护进程名称
    pid_fn = '/tmp/daemon_class.pid'  # 守护进程pid文件的绝对路径
    log_fn = '/tmp/daemon_class.log'  # 守护进程日志文件的绝对路径
    err_fn = '/tmp/daemon_class.err.log'  # 守护进程启动过程中的错误日志,内部出错能从这里看到
    cD = ClientDaemon(p_name, pid_fn, stderr=err_fn, verbose=1)

    if sys.argv[1] == 'start':
        cD.start(log_fn)
    elif sys.argv[1] == 'stop':
        cD.stop()
    elif sys.argv[1] == 'restart':
        cD.restart(log_fn)
    elif sys.argv[1] == 'status':
        alive = cD.is_running()
        if alive:
            print 'process [%s] is running ......' % cD.get_pid()
        else:
            print 'daemon process [%s] stopped' % cD.name
    else:
        print 'invalid argument!'
        print help_msg
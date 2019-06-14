# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Pool
import getopt
import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dateutil.relativedelta import relativedelta

from PB import pb_io


python = 'python2.7 -W ignore'
mpi_run = 'mpirun'
mpi_main = 'mpi.py'
cores = 56


@contextmanager
def time_block(flag, var_log):
    """
    计算一个代码块的运行时间
    :param flag: 标签
    :param on: 是否开启
    :return:
    """
    time_start = datetime.now()
    try:
        yield
    finally:
        time_end = datetime.now()
        all_time = (time_end - time_start).seconds
#             print u"{} 耗时: {}".format(flag, all_time)
        var_log.info(u"{} 耗时: {}s".format(flag, all_time))


def usage():
    print(u"""
    -h / --help :使用帮助
    -v / --verson: 显示版本号
    -j / --job : 作业步骤 -j 0110 or --job 0110
    -s / --sat : 卫星信息  -s FY3B+MERSI_AQUA+MODIS or --sat FY3B+MERSI_AQUA+MODIS
    -t / --time :日期   -t 20180101-20180101 or --time 20180101-20180101
    -p / --port : 端口  -p 10001  防止作业重复运行
    """)


def get_args():
    try:
        opts, _ = getopt.getopt(
            sys.argv[1:], "hv:j:s:t:p:P:", ["version", "help", "job=", "sat=", "time=", "port=", "thread="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(1)

    for key, val in opts:
        if key in ('-v', '--version'):
            verbose = '1.0.1'
            print 'Version: %s' % verbose
            sys.exit()

        elif key in ("-h", "--help"):
            usage()
            sys.exit()
        elif key in ("-s", "--sat"):
            args_pair = val

        elif key in ("-j", "--job"):
            args_id = val

        elif key in ("-t", "--time"):
            args_date = val

        elif key in ("-p", "--port"):
            args_port = int(val)

        elif key in ("-T", "--thread"):
            args_thread = int(val)
        else:
            assert False, "unhandled option"

    return args_pair, args_id, args_date, args_port, args_thread


def get_pair_list(args_pair, g_var_cfg):
    """
    u 获取参数中的卫星对
    return : list
    """

    if args_pair.lower() == 'all':
        args_pair_list = g_var_cfg['PAIRS'].keys()
    else:
        args_pair_list = args_pair.split(',')
    return args_pair_list


def get_date_list(args_pair_list, args_date, g_var_cfg):
    """
    u 获取参数中的时间范围,args_data 支持all 和 时间范围
    return : dict
    """
    args_date_list = dict()
    cfg_launch_date = g_var_cfg['LAUNCH_DATE']
    cfg_rolldays = g_var_cfg['CROND']['rolldays']

    # 从卫星对清单中获取每个卫星对，进行时间获取
    for pair in args_pair_list:
        # 时间字典的key用卫星对来标记
        short_sat = pair.split('+')[0]
        if pair not in args_date_list:
            args_date_list[pair] = []

        if args_date.lower() == 'all':  # 发星以来的日期
            date_start = cfg_launch_date[short_sat]
            date_end = datetime.utcnow()
            date_start = datetime.strptime(date_start, '%Y%m%d')
            args_date_list[pair].append((date_start, date_end))

        elif args_date.lower() == 'auto':  # 日期滚动
            for rday in cfg_rolldays:
                rd = int(rday)
                date_start = (datetime.utcnow() - relativedelta(minutes=rd))
                date_end = datetime.utcnow()
                args_date_list[pair].append((date_start, date_end))

        else:  # 手动输入的日期
            date_start, date_end = args_date.split('-')
            date_start = datetime.strptime(date_start, '%Y%m%d%H%M%S')
            date_end = datetime.strptime(date_end, '%Y%m%d%H%M%S')
            args_date_list[pair].append((date_start, date_end))

    return args_date_list


def get_job_id_list(args_pair_list, args_id, g_var_cfg):
    """
    u 获取参数中的作业编号,args_id支持all 和  自定义id(0310,0311)
    return: dict
    """
    args_id_list = dict()

    for pair in args_pair_list:
        if pair not in args_id_list:
            args_id_list[pair] = []
        if args_id.lower() == 'all':  # 自动作业流
            # 若果是all就根据卫星对获取对应的作业流
            job_flow = g_var_cfg['PAIRS'][pair]['job_flow']
            job_flow_def = g_var_cfg['JOB_FLOW_DEF'][job_flow]
            args_id_list[pair] = job_flow_def
        else:  # 手动作业流
            args_id_list[pair] = ['job_%s' % id_ for id_ in args_id.split(',')]

    return args_id_list


def run_command(args_dict, threads, var_log, sat_pair, job_id, date_s, date_e):

    # 开启进程池

    for key_name in sorted(args_dict.keys(), reverse=True):
        with time_block(u'%s %s %s' % (sat_pair, job_id, key_name), var_log):
            pool = Pool(processes=int(threads))
            for cmd in args_dict[key_name]:
                pool.apply_async(command, (cmd,))
            pool.close()
            pool.join()


def command(cmd):
    '''
    args_cmd: python a.py 20180101  (完整的执行参数)
    '''
    print cmd
    try:
        P1 = subprocess.Popen(cmd.split())
    except Exception, e:
        print (e)
        return

    timeout = 3600 * 1
    t_beginning = time.time()
    seconds_passed = 0

    while (P1.poll() is None):

        seconds_passed = time.time() - t_beginning

        if timeout and seconds_passed > timeout:
            print seconds_passed
            P1.kill()
        time.sleep(1)
    P1.wait()


def run_command_parallel(arg_list):

    arg_list = [each + '\n' for each in arg_list]
    fp = open('filelist.txt', 'w')
    fp.writelines(arg_list)
    fp.close()

    cmd = '%s -np %d -machinefile hostfile %s %s' % (
        mpi_run, cores, python, mpi_main)
    os.system(cmd)


def get_cmd_list(job_exe, sat_pair, job_id, date_s, date_e, g_path_interface):

    #     cmd_list = []
    # 记录原来的时间
    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')

    reg = u'.*(\d{8})_(\d{4}).*.yaml'
    args_dict = {}
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        date1 = date1 + relativedelta(days=1)
        path_yaml = os.path.join(g_path_interface, sat_pair, job_id, ymd)
        if os.path.isdir(path_yaml):
            file_list_yaml = pb_io.find_file(path_yaml, '.*.yaml')
            for file_yaml in file_list_yaml:
                file_name = os.path.basename(file_yaml)

                m = re.match(reg, file_name)
                if m:
                    ymd = m.group(1)
                    hm = m.group(2)
                    ymd_hm = '%s_%s' % (ymd, hm)

                    # 不在时间段内的数据不处理
                    file_datetime = datetime.strptime(
                        '%s%s' % (ymd, hm), '%Y%m%d%H%M')
                    if date_s > file_datetime or file_datetime > date_e:
                        continue
                    if ymd_hm not in args_dict:
                        args_dict[ymd_hm] = []
                    cmd = '%s %s %s' % (python, job_exe, file_yaml)
                    args_dict[ymd_hm].append(cmd)

    return args_dict

if __name__ == '__main__':
    pass

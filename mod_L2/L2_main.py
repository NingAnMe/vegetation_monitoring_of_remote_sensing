# -*- coding: utf-8 -*-

import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import Pool
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from PB.CSC.pb_csc_console import SocketServer, LogServer
from lib.com_lib_crontrol import get_args, get_date_list, get_job_id_list, get_pair_list
from lib.com_lib_crontrol import run_command, run_command_parallel, get_cmd_list
from PB.pb_io import write_yaml_file, find_file, str_format


__description__ = u'dm模块调度'
__author__ = 'wangpeng'
__date__ = '2018-10-30'
__version__ = '1.0.0_beat'


# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/L2.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)

# 调度文件中的路径信息
g_path_h8L1 = g_var_cfg['PATH']['IN']['h8_L1']
g_path_h8L2 = g_var_cfg['PATH']['IN']['h8_L2']
g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']
g_path_h8fig = g_var_cfg['PATH']['OUT']['h8_fig']
g_path_h8_ndvi = g_var_cfg['PATH']['MID']['h8_ndvi']
g_path_h8png = g_var_cfg['PATH']['OUT']['h8_png']

g_path_interface = g_var_cfg['PATH']['OUT']['interface']

# 初始化调度日志
g_path_log = g_var_cfg['PATH']['OUT']['log']
g_var_log = LogServer(g_path_log)

# 覆盖接口文件标记
g_rewrite_interface = g_var_cfg['CROND']['rewrite_interface'].lower()
g_run_jobs = g_var_cfg['CROND']['run_jobs'].lower()
g_run_mode = g_var_cfg['CROND']['run_mode']

TEST = True


@contextmanager
def time_block(flag, switch=True):
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
        if switch is True:
            time_end = datetime.now()
            all_time = (time_end - time_start).seconds
            print u"{} 耗时: {}".format(flag, all_time)


def get_job_id_func(job_id):
    """
    u 返回jobid对应的函数名称 ，jobid唯一性
    :return:
    """
    job_id_func = {
        "job_0110": job_0110,
        "job_0210": job_0210,
        "job_1010": job_1010,
    }
    return job_id_func.get(job_id)


def main():

    # 获取必要的三个参数(卫星对，作业编号，日期范围 , 端口, 线程)
    args_pair, args_id, args_date, port, threads = get_args()
    g_var_log.info(u'调度开始: %s %s %s' % (args_pair, args_id, args_date))
    # 端口大于0 就开启
    if port > 0:
        sserver = SocketServer()
        if sserver.createSocket(port) == False:
            sserver.closeSocket(port)
            g_var_log.info(u'----已经有一个实例在实行 %d' % os.getpid())
            sys.exit(-1)

    # 1 获取卫星对清单
    args_pair_list = get_pair_list(args_pair, g_var_cfg)
    # 2 获取作业流清单
    args_id_list = get_job_id_list(args_pair_list, args_id, g_var_cfg)
    # 3 获取日期的清单
    args_date_list = get_date_list(args_pair_list, args_date, g_var_cfg)

    #  开始根据卫星对处理作业流
    for sat_pair in args_pair_list:  # 卫星对
        for job_id in args_id_list[sat_pair]:  # 作业编号
            process_name = g_var_cfg['BAND_JOB_MODE'][job_id]  # 作业进程
            for date_s, date_e in args_date_list[sat_pair]:  # 处理时间
                get_job_id_func(job_id)(
                    process_name, sat_pair, job_id, date_s, date_e, threads)

    # 开始获取执行指令信息
    if 'on' in g_run_jobs:
        for sat_pair in args_pair_list:  # 卫星对
            run_var_log = LogServer(g_path_log, sat_pair)
            for job_id in args_id_list[sat_pair]:  # 作业编号
                process_name = g_var_cfg['BAND_JOB_MODE'][job_id]  # 作业进程
                for date_s, date_e in args_date_list[sat_pair]:  # 处理时间
                    # 获取参数文件
                    args_dict = get_cmd_list(
                        process_name, sat_pair, job_id, date_s, date_e, g_path_interface)

                    if 'onenode' in g_run_mode:
                        run_command(
                            args_dict, threads, run_var_log, sat_pair, job_id, date_s, date_e)
                    elif 'cluster' in g_run_mode:
                        run_command_parallel(args_dict)
                    else:
                        print 'error: parallel_mode args input onenode or cluster'
                        sys.exit(-1)
    else:
        print 'run jobs off...'

# 以上部分完全可复用，在不同不摸直接复制即可


def job_0110(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
#         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0110(sat_pair, job_id, ymd)
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0110(sat_pair, job_id, ymd):

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    area = g_var_cfg['PAIRS'][sat_pair]['area']
    area = [float(each) for each in area]
    lon_w, _, _, lat_n = area

    res_m = int(res * 100 * 1000)

    filename = 'H08_GEO_%s_%sM.hdf5' % (sat_dtype, res_m)

    # 输入
    in_path = os.path.join(g_path_h8L2, ymd)
    in_path_geo = os.path.join(g_path_h8geo, filename)

    # 遍历文件
    # Himawari08_AHI_L2_Aerosol_20190516_0750.hdf
    reg = u'Himawari08_AHI_L2_(.*)_(\d{8})_(\d{4}).hdf$'
    file_list = find_file(in_path, reg)

    print in_path

    # 输出
    out_path = os.path.join(g_path_h8fig, ymd)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    for in_file in file_list:
        file_name = os.path.basename(in_file)
        m = re.match(reg, file_name)
        if m:
            ptype = m.group(1)
            ymd = m.group(2)
            hm = m.group(3)
            ymd_hm = '%s_%s' % (ymd, hm)

            # 接口文件存在跳过
            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s_%s.yaml' % (ymd_hm, ptype))
            if 'on' in g_rewrite_interface:
                pass
            elif 'off' in g_rewrite_interface:
                if os.path.isfile(out_path_interface):
                    continue

            dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm, 'type': ptype},
                     'PROJ': {'lt_lon': lon_w, 'lt_lat': lat_n, 'res': res},
                     'PATH': {'opath': out_path, 'ipath': in_file, 'ipath_geo': in_path_geo,
                              'log': str(g_path_log)}}
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))


def job_0210(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
#         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0210(sat_pair, job_id, ymd)
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0210(sat_pair, job_id, ymd):

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)

    # 输入
    in_path = os.path.join(g_path_h8L1, ymd)
    filename = 'H08_GEO_ORIGINAL_%sM.hdf5' % (res_m)
    in_path_geo = os.path.join(g_path_h8geo, filename)

    # 输出
    out_path1 = os.path.join(g_path_h8L2, ymd)
    out_path2 = os.path.join(g_path_h8fig, ymd)

    if not os.path.isdir(out_path1):
        os.makedirs(out_path1)

    if not os.path.isdir(out_path2):
        os.makedirs(out_path2)

    # 遍历文件
    reg = u'AHI8_OBI_%04dM_NOM_(\d{8})_(\d{4}).hdf$' % res_m
    file_list = find_file(in_path, reg)

    # 逐个文件处理
    for in_file in file_list:
        file_name = os.path.basename(in_file)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(1)
            hm = m.group(2)
            ymd_hm = '%s_%s' % (ymd, hm)
            # 接口文件存在跳过
            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s.yaml' % (ymd_hm))
            if 'on' in g_rewrite_interface:
                pass
            elif 'off' in g_rewrite_interface:
                if os.path.isfile(out_path_interface):
                    continue

            outfile = os.path.join(
                out_path1, 'Himawari08_AHI_L2_Surface_NDVI_%s_%s.hdf' % (ymd, hm))
            outfig = os.path.join(
                out_path2, 'HIMAWARI_AHI_L2_Surface_%s_%s_NDVI.tiff' % (ymd, hm))

            dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm},
                     'PATH': {'opath': outfile, 'ipath': in_file,
                              'ipath_geo': in_path_geo, 'opath_fig': outfig,
                              'log': str(g_path_log)}}
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))


def job_1010(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
#         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_1010(sat_pair, job_id, ymd)
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_1010(sat_pair, job_id, ymd):

    dtype = 'L2'

    # 输入
    in_path = os.path.join(g_path_h8fig, ymd)

    # 遍历所有文件
    # Himawari08_AHI_L2_Aerosol_20190516_0750.hdf
    reg = u'HIMAWARI_AHI_(.*)_(.*)_(\d{8})_(\d{4})_(.*).tiff$'
    file_list = find_file(in_path, reg)

    # 输出
    for in_file in file_list:
        file_name = os.path.basename(in_file)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(3)
            hm = m.group(4)
            ptype = m.group(5)

            # 接口文件存在跳过
            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s_%s_%s.yaml' % (ymd, hm, ptype))
            if 'on' in g_rewrite_interface:
                pass
            elif 'off' in g_rewrite_interface:
                if os.path.isfile(out_path_interface):
                    continue

            out_path = os.path.join(g_path_h8png, ptype, ymd, hm)
#             out_path = str_format(out_path, {'H8_TYPE': sat_dtype})
            if not os.path.isdir(out_path):
                os.makedirs(out_path)

            dict1 = {'INFO': {'pair': sat_pair, 'ymd': '%s_%s' % (ymd, hm),
                              'ptype': ptype, 'dtype': dtype},
                     'PATH': {'ipath': in_file, 'opath': out_path,
                              'log': str(g_path_log)}}
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))

if __name__ == '__main__':
    main()

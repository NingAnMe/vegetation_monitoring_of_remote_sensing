# -*- coding: utf-8 -*-

import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import Pool
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from PB.CSC.pb_csc_console import SocketServer, LogServer
from lib.com_lib_crontrol import get_args, get_date_list, get_job_id_list, get_pair_list
from lib.com_lib_crontrol import run_command, run_command_parallel, get_cmd_list
from lib.com_lib_crontrol import time_block
from PB.pb_io import write_yaml_file, find_file, str_format


__description__ = u'dm模块调度'
__author__ = 'wangpeng'
__date__ = '2018-10-30'
__version__ = '1.0.0_beat'


# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/L1.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)

# 调度文件中的路径信息
g_path_h8src = g_var_cfg['PATH']['IN']['h8_src']
g_path_h8dat = g_var_cfg['PATH']['IN']['h8_dat']
g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']
g_path_h8nc = g_var_cfg['PATH']['MID']['h8_nc']
g_path_h8ang = g_var_cfg['PATH']['MID']['h8_ang']
g_path_h8hdf = g_var_cfg['PATH']['OUT']['h8_hdf']
g_path_h8fig = g_var_cfg['PATH']['OUT']['h8_fig']
g_path_h8png = g_var_cfg['PATH']['OUT']['h8_png']

g_path_interface = g_var_cfg['PATH']['OUT']['interface']

# 初始化调度日志
g_path_log = g_var_cfg['PATH']['OUT']['log']
g_var_log = LogServer(g_path_log)

# 覆盖接口文件标记
g_rewrite_interface = g_var_cfg['CROND']['rewrite_interface'].lower()
g_run_jobs = g_var_cfg['CROND']['run_jobs'].lower()
g_run_mode = g_var_cfg['CROND']['run_mode']


def get_job_id_func(job_id):
    """
    u 返回jobid对应的函数名称 ，jobid唯一性
    :return:
    """
    job_id_func = {
        "job_0110": job_0110,
        "job_0210": job_0210,
        "job_0310": job_0310,
        "job_0410": job_0410,
        "job_0510": job_0510,
        "job_0610": job_0610,
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
            #             run_var_log = LogServer(g_path_log, sat_pair)
            for job_id in args_id_list[sat_pair]:  # 作业编号
                process_name = g_var_cfg['BAND_JOB_MODE'][job_id]  # 作业进程
                for date_s, date_e in args_date_list[sat_pair]:  # 处理时间
                    # 获取参数文件
                    args_dict = get_cmd_list(
                        process_name, sat_pair, job_id, date_s, date_e, g_path_interface)

                    if 'onenode' in g_run_mode:
                        run_command(
                            args_dict, threads, g_path_log, sat_pair, job_id, date_s, date_e)
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
        pool.apply_async(create_job_0110, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0110(sat_pair, job_id, ymd):

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # HS_H08_20190507_0000_B04_FLDK_R10_S0410.DAT.bz2
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_FLDK_\w{3}_(\w{5}).DAT.bz2\Z'

    # 输入
    in_path = os.path.join(g_path_h8src, ymd[:4], ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})
    print in_path
    file_list = find_file(in_path, reg)
    print len(file_list)

    # 输出
    out_path = os.path.join(g_path_h8dat, ymd)
    out_path = str_format(out_path, {'H8_TYPE': sat_dtype})
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    for in_file in file_list:
        file_name = os.path.basename(in_file)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(1)
            hm = m.group(2)
            band = m.group(3)
            segm = m.group(4)

        out_file_name = file_name.split('.bz2')[0]
        out_file = os.path.join(out_path, out_file_name)

        out_path_interface = os.path.join(
            g_path_interface, sat_pair, job_id, ymd, '%s_%s_%s_%s.yaml' % (ymd, hm, band, segm))
        if 'on' in g_rewrite_interface:
            pass
        elif 'off' in g_rewrite_interface:
            if os.path.isfile(out_path_interface):
                continue
        dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd},
                 'PATH': {'opath': out_file, 'ipath': in_file, 'log': str(g_path_log)}}
        if not os.path.isfile(out_file):
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))


def job_0210(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0210(sat_pair, job_id, ymd):

    # 解析配置文件
    band_list = g_var_cfg['PAIRS'][sat_pair]['band']
    segm_list = g_var_cfg['PAIRS'][sat_pair]['segment']
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    row = int(g_var_cfg['PAIRS'][sat_pair]['row'])
    col = int(g_var_cfg['PAIRS'][sat_pair]['col'])
    area = g_var_cfg['PAIRS'][sat_pair]['area']
    # 区域解析
    area = [float(each) for each in area]
    lon_w, _, _, lat_n = area

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    if 'BROADCAST' in sat_dtype:
        reg = u'IMG_DK01(\w{3})_(\d{8})(\d{4})_(\w{3})'

    else:
        reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_FLDK_\w{3}_(\w{5}).DAT\Z'

    # 输入
    in_path = os.path.join(g_path_h8dat, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})
    file_list = find_file(in_path, reg)

    # 输出
    out_path = os.path.join(g_path_h8nc, ymd)
    out_path = str_format(out_path, {'H8_TYPE': sat_dtype})

    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 存放YYYY_MM_BAND 的条带文件
    h8_dict = {}
    # 逐个文件处理
    for each in file_list:
        file_name = os.path.basename(each)
        m = re.match(reg, file_name)
        if m:
            if 'BROADCAST' in sat_dtype:
                ymd = m.group(2)
                hm = m.group(3)
                band = m.group(1)
                segm = m.group(4)
            else:
                ymd = m.group(1)
                hm = m.group(2)
                band = m.group(3)
                segm = m.group(4)

            # 时间当作key
            ymd_hm = '%s_%s' % (ymd, hm)
            if ymd_hm not in h8_dict:
                h8_dict[ymd_hm] = {}

            # 固定通道
            if band in band_list and segm in segm_list:
                # 通道当作key
                if band not in h8_dict[ymd_hm]:
                    h8_dict[ymd_hm][band] = []
                # 保存一个通道所有需要的条带
                h8_dict[ymd_hm][band].append(each)

    # 开始处理每个时次和每个通道
    for ymd_hm in sorted(h8_dict.keys()):
        for band in sorted(h8_dict[ymd_hm].keys()):
            # 接口文件存在跳过
            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s_%s.yaml' % (ymd_hm, band))
            if 'on' in g_rewrite_interface:
                pass
            elif 'off' in g_rewrite_interface:
                if os.path.isfile(out_path_interface):
                    continue
            # 条带文件
            band_file_list = h8_dict[ymd_hm][band]
            res_m = res * 100 * 1000
            file_name = 'HS_H08_%s_%s_%04dM_FLDK.NC' % (
                ymd_hm, band, res_m)
            out_path_file = os.path.join(out_path, file_name)
            dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm},
                     'PROJ': {'left_top_lat': lat_n, 'left_top_lon': lon_w,
                              'row': row, 'col': col, 'lat_res': res, 'lon_res': res},
                     'PATH': {'opath': out_path_file, 'ipath': band_file_list, 'log': str(g_path_log)}}

            if len(band_file_list) == len(segm_list):
                write_yaml_file(dict1, out_path_interface)
                print('%s %s %s create yaml interface success' %
                      (sat_pair, ymd_hm, band))


def job_0310(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        pool.apply_async(create_job_0310, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0310(sat_pair, job_id, ymd):

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 配置信息
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)

    # 输入
    in_path = os.path.join(
        g_path_h8geo, 'H08_GEO_%s_%sM.hdf5' % (sat_dtype, res_m))
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})

    # 输出
    out_path = os.path.join(g_path_h8ang, ymd)
    out_path = str_format(out_path, {'H8_TYPE': sat_dtype})

    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    date_s = datetime.strptime(ymd, '%Y%m%d')
    date_e = date_s + relativedelta(days=1)

    # 计算0000-2350的角度信息
    while date_s < date_e:
        ymd_hm = date_s.strftime('%Y%m%d_%H%M')
        date_s = date_s + relativedelta(minutes=10)

        # 输出角度文件
        out_file = os.path.join(
            out_path, 'AHI8_Angle_%04dM_%s.hdf' % (res_m, ymd_hm))

        # 接口文件存在跳过
        out_path_interface = os.path.join(
            g_path_interface, sat_pair, job_id, ymd, '%s.yaml' % (ymd_hm))
        if 'on' in g_rewrite_interface:
            pass
        elif 'off' in g_rewrite_interface:
            if os.path.isfile(out_path_interface):
                continue

        dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm, 'res': res_m},
                 'PATH': {'opath': out_file, 'ipath': in_path,
                          'log': str(g_path_log)}}
        write_yaml_file(dict1, out_path_interface)
        print('%s %s create yaml interface success' % (sat_pair, ymd_hm))


def job_0410(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        pool.apply_async(create_job_0410, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0410(sat_pair, job_id, ymd):

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 输入
    in_path = os.path.join(g_path_h8nc, ymd)
    in_path_ang = os.path.join(g_path_h8ang, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})
    in_path_ang = str_format(in_path_ang, {'H8_TYPE': sat_dtype})
    print in_path, in_path_ang
    # 输出
    out_path = os.path.join(g_path_h8hdf, ymd)
    out_path = str_format(out_path, {'H8_TYPE': sat_dtype})

    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 配置信息
    area = g_var_cfg['PAIRS'][sat_pair]['area']
    band_list = g_var_cfg['PAIRS'][sat_pair]['band']
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = '%04dM' % (res * 100 * 1000)

    area = [float(each) for each in area]
    lon_w, lon_e, lat_s, lat_n = area

    # 检索NC文件列表
    # HS_H08_20190427_0840_B05_2000M_FLDK.NC
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_%s_FLDK.NC\Z' % res_m
    file_list = find_file(in_path, '.*.NC')

    # 按照时次把每个时次要合成的文件放到字典
    h8_dict = {}
    for each in file_list:
        file_name = os.path.basename(each)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(1)
            hm = m.group(2)
            band = m.group(3)
            ymd_hm = '%s_%s' % (ymd, hm)

            if ymd_hm not in h8_dict:
                h8_dict[ymd_hm] = []

            # 只处理配置中配置的通道
            if band in band_list:
                h8_dict[ymd_hm].append(each)

    # 检索每个时次的文件
    for ymd_hm in sorted(h8_dict.keys()):
        nc_file_list = h8_dict[ymd_hm]
        file_name_angle = 'AHI8_Angle_%s_%s.hdf' % (res_m, ymd_hm)
        in_file_angle = os.path.join(in_path_ang, file_name_angle)
        # 一个时间有几个通道就应该产生几个nc，如果一致则处理
        if len(nc_file_list) == len(band_list):
            if os.path.isfile(in_file_angle):
                out_file = os.path.join(
                    out_path, 'AHI8_OBI_%s_NOM_%s.hdf' % (res_m, ymd_hm))

                # 接口文件存在跳过
                out_path_interface = os.path.join(
                    g_path_interface, sat_pair, job_id, ymd, '%s.yaml' % (ymd_hm))
                if 'on' in g_rewrite_interface:
                    pass
                elif 'off' in g_rewrite_interface:
                    if os.path.isfile(out_path_interface):
                        continue

                dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm, 'res': res_m[:4]},
                         'PROJ': {'lon_w': lon_w, 'lon_e': lon_e, 'lat_s': lat_s, 'lat_n': lat_n, 'res': res},
                         'PATH': {'opath': out_file, 'ipath': nc_file_list,
                                  'ipath_angle': in_file_angle, 'log': str(g_path_log)}}
                write_yaml_file(dict1, out_path_interface)
                print('%s %s create yaml interface success' %
                      (sat_pair, ymd_hm))


def job_0510(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        pool.apply_async(create_job_0510, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0510(sat_pair, job_id, ymd):

    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    area = g_var_cfg['PAIRS'][sat_pair]['area']
    area = [float(each) for each in area]
    lon_w, _, _, lat_n = area
    res_m = int(res * 100 * 1000)
    reg = u'AHI8_OBI_%04dM_NOM_(\d{8})_(\d{4}).hdf$' % res_m

    filename = 'H08_GEO_%s_%sM.hdf5' % (sat_dtype, res_m)

    # 输入
    in_path = os.path.join(g_path_h8hdf, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})

    in_path_geo = os.path.join(g_path_h8geo, filename)
    in_path_geo = str_format(in_path_geo, {'H8_TYPE': sat_dtype})
    # 输出
    out_path = os.path.join(g_path_h8fig, ymd)
    out_path = str_format(out_path, {'H8_TYPE': sat_dtype})

    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 查找当天文件

    file_list = find_file(in_path, reg)

    # 逐个处理
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

            dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm, 'dtype': sat_dtype},
                     'PROJ': {'lt_lon': lon_w, 'lt_lat': lat_n, 'res': res},
                     'PATH': {'ipath': in_file, 'ipath_geo': in_path_geo, 'opath': out_path, 'log': str(g_path_log)}}
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))


def job_0610(job_exe, sat_pair, job_id, date_s, date_e, threads):

    date1 = datetime.strptime(date_s.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_e.strftime('%Y%m%d'), '%Y%m%d')
    pool = Pool(processes=int(threads))
    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        pool.apply_async(create_job_0610, (sat_pair, job_id, ymd))
        date1 = date1 + relativedelta(days=1)
    pool.close()
    pool.join()


def create_job_0610(sat_pair, job_id, ymd):

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)
    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 输入
    in_path = os.path.join(g_path_h8fig, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})

    # 遍历当天数据
    # Himawari08_AHI_L2_Aerosol_20190516_0750.hdf
    reg = u'HIMAWARI_AHI_(%s)_%s_(\d{8})_(\d{4})_(.*).tiff$' % (
        res_m, sat_dtype)
    file_list = find_file(in_path, reg)

    # 输出
    for in_file in file_list:
        file_name = os.path.basename(in_file)
        m = re.match(reg, file_name)
        if m:
            res = m.group(1)
            ymd = m.group(2)
            hm = m.group(3)
            band = m.group(4)
            ptype = band
            ymd_hm = '%s_%s' % (ymd, hm)

            # 接口文件存在跳过
            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s_%s_%s.yaml' % (ymd_hm, res, band))
            if 'on' in g_rewrite_interface:
                pass
            elif 'off' in g_rewrite_interface:
                if os.path.isfile(out_path_interface):
                    continue

            out_path = os.path.join(g_path_h8png, ptype, ymd, hm)
            out_path = str_format(out_path, {'H8_TYPE': sat_dtype})
            if not os.path.isdir(out_path):
                os.makedirs(out_path)

            dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm,
                              'ptype': ptype, 'dtype': sat_dtype},
                     'PATH': {'ipath': in_file, 'opath': out_path,
                              'log': str(g_path_log)}}
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm, band))

if __name__ == '__main__':

    #     with time_block('sssss', g_var_log):
    #         time.sleep(1)

    main()

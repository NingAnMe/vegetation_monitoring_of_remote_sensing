# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re
import shutil
import sys
from multiprocessing import Pool
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from PB.CSC.pb_csc_console import SocketServer, LogServer
from PB.CSC.pb_csc_console import get_args, get_date_list, get_job_id_list, get_pair_list
from PB.CSC.pb_csc_console import run_command, run_command_parallel, get_cmd_list
from PB.pb_io import write_yaml_file, find_file
from lib.com_lib_proj import prj_gll


__description__ = u'dm模块调度'
__author__ = 'wangpeng'
__date__ = '2018-10-30'
__version__ = '1.0.0_beat'


# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/com.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)

# 调度文件中的路径信息
g_path_h8src = g_var_cfg['PATH']['IN']['h8_src']
g_path_h8dat = g_var_cfg['PATH']['IN']['h8_dat']
g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']
g_path_h8nc = g_var_cfg['PATH']['MID']['h8_nc']
g_path_h8ang = g_var_cfg['PATH']['MID']['h8_ang']
g_path_h8hdf = g_var_cfg['PATH']['OUT']['h8_hdf']

g_path_interface = g_var_cfg['PATH']['OUT']['interface']

# 初始化调度日志
g_path_log = g_var_cfg['PATH']['OUT']['log']
g_var_log = LogServer(g_path_log)

# 覆盖接口文件标记
g_var_rewrite = g_var_cfg['CROND']['rewrite_yaml_file']

# 进程数量
threads = g_var_cfg['CROND']['threads']
# 运行模式 单节点或是 集群
run_mode = g_var_cfg['CROND']['parallel_mode']

# 运行模式 单节点或是 集群
port = int(g_var_cfg['CROND']['port'])


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
    }
    return job_id_func.get(job_id)


def main():
    # 启动socket服务,防止多实例运行,此代码片段不能放在全局
    # OSError: [WinError 10048] 通常每个套接字地址(协议/网络地址/端口)只允许使用一次
    # 在Windows中因为子进程会复制主进程中所有代码导致相同的端口在子进程中同样被绑定,解决办法是程序最后做一个判断
    # 问题就解决了,还有在使用进程池的时候切记一定要加这一句否则子进程会递归的添加进程到进程池形成死循环.
    if port > 0:
        sserver = SocketServer()
        if sserver.createSocket(port) == False:
            sserver.closeSocket(port)
            g_var_log.info(u'----已经有一个实例在实行 %d' % os.getpid())
            sys.exit(-1)

    # 获取必要的三个参数(卫星对，作业编号，日期范围)
    args_pair, args_id, args_date = get_args()
    g_var_log.info(u'调度开始: %s %s %s' % (args_pair, args_id, args_date))

    # 1 获取卫星对清单
    args_pair_list = get_pair_list(args_pair, g_var_cfg)
    # 2 获取作业流清单
    args_id_list = get_job_id_list(args_pair_list, args_id, g_var_cfg)
    # 3 获取日期的清单
    args_date_list = get_date_list(args_pair_list, args_date, g_var_cfg)

    #  开始根据卫星对处理作业流
    for sat_pair in args_pair_list:  # 卫星对
        for job_id in args_id_list[sat_pair]:  # 作业编号
            # 1 根据作业编号获取作业进程名（唯一性）
            process_name = g_var_cfg['BAND_JOB_MODE'][job_id]
            date_list = args_date_list[sat_pair]  # 处理时间
            # 2 根据作业编号获取对应的实现函数
            if g_var_rewrite.lower() in 'on':
                print u'覆盖yaml file 模式'
                job_id_func = get_job_id_func(job_id)
                cmd_list = job_id_func(
                    process_name, sat_pair, job_id, date_list)
            elif g_var_rewrite.lower() in 'off':
                print u'非覆盖yaml file 模式'
                cmd_list = get_cmd_list(
                    process_name, sat_pair, job_id, date_list, g_path_interface)
            # 3 运行作业
            if 'onenode' in run_mode:
                pass
                run_command(cmd_list, threads)
            elif 'cluster' in run_mode:
                run_command_parallel(cmd_list)
            else:
                print 'error: parallel_mode args input onenode or cluster'

# 以上部分完全可复用，在不同不摸直接复制即可


def job_0110(job_exe, sat_pair, job_id, date_list):

    g_var_log.info(u'%s %s 葵花数据调bz2解压度开始 ,时间清单长度 %d' %
                   (sat_pair, job_id, len(date_list)))

    pool = Pool(processes=int(threads))
    for date_s in date_list:
        ymd = date_s.strftime('%Y%m%d')
#         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0110(sat_pair, job_id, ymd)
    pool.close()
    pool.join()

    # 获取参数文件
    cmd_list = get_cmd_list(
        job_exe, sat_pair, job_id, date_list, g_path_interface)
    return cmd_list


def create_job_0110(sat_pair, job_id, ymd):

    # HS_H08_20190507_0000_B04_FLDK_R10_S0410.DAT.bz2
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_FLDK_\w{3}_(\w{5}).DAT.bz2\Z'
    # 清理配置接口文件
    out_path_interface = os.path.join(g_path_interface, sat_pair, job_id, ymd)
    if os.path.isdir(out_path_interface):
        shutil.rmtree(out_path_interface)

    # 输入
    in_path = os.path.join(g_path_h8src, ymd[:4], ymd)
    file_list = find_file(in_path, reg)

    # 输出
    out_path = os.path.join(g_path_h8dat, ymd)
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
#         if os.path.isfile(out_path_interface):
#             continue
        dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd},
                 'PATH': {'opath': out_file, 'ipath': in_file, 'log': str(g_path_log)}}
        if not os.path.isfile(out_file):
            write_yaml_file(dict1, out_path_interface)
            print('%s %s %s create yaml interface success' %
                  (sat_pair, ymd, hm))


def job_0210(job_exe, sat_pair, job_id, date_list):

    g_var_log.info(u'%s %s 葵花数据dat转nc调度开始 ,时间清单长度 %d' %
                   (sat_pair, job_id, len(date_list)))

    pool = Pool(processes=int(threads))
    for date_s in date_list:
        ymd = date_s.strftime('%Y%m%d')
#         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0210(sat_pair, job_id, ymd)
    pool.close()
    pool.join()

    # 获取参数文件
    cmd_list = get_cmd_list(
        job_exe, sat_pair, job_id, date_list, g_path_interface)
    return cmd_list


def create_job_0210(sat_pair, job_id, ymd):

    # HS_H08_20190427_1110_B10_FLDK_R20_S0310.DAT
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_FLDK_\w{3}_(\w{5}).DAT\Z'
    band_list = g_var_cfg['PAIRS'][sat_pair]['band']
    segm_list = g_var_cfg['PAIRS'][sat_pair]['segment']
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    area = g_var_cfg['PAIRS'][sat_pair]['area']

    # 计算区域 分辨率 行列等信息
    area = [float(each) for each in area]
    lon_w, lon_e, lat_s, lat_n = area

    band_rowcol_dict = {}

    # 计算行列分辨率
    for band in band_list:
        #         res = float(res_list[idx])
        proj = prj_gll(lat_n, lat_s, lon_w, lon_e, res, res)
#         band_res_dict[band] = [res, res]
        band_rowcol_dict[band] = [proj.rowMax, proj.colMax]

    # 清理配置接口文件
    out_path_interface = os.path.join(g_path_interface, sat_pair, job_id, ymd)
    if os.path.isdir(out_path_interface):
        shutil.rmtree(out_path_interface)

    # 输入
    in_path = os.path.join(g_path_h8dat, ymd)
    file_list = find_file(in_path, '.*.DAT')

    # 输出
    out_path = os.path.join(g_path_h8nc, ymd)
    h8_dict = {}
    for each in file_list:
        file_name = os.path.basename(each)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(1)
            hm = m.group(2)
            band = m.group(3)
            segm = m.group(4)
            t_key = '%s_%s' % (ymd, hm)

            if t_key not in h8_dict:
                h8_dict[t_key] = {}

            if band in band_list and segm in segm_list:
                if band not in h8_dict[t_key]:
                    h8_dict[t_key][band] = []

                h8_dict[t_key][band].append(each)

    for t_key in sorted(h8_dict.keys()):

        for band in sorted(h8_dict[t_key].keys()):

            out_path_interface = os.path.join(
                g_path_interface, sat_pair, job_id, ymd, '%s_%s.yaml' % (t_key, band))
            row = band_rowcol_dict[band][0]
            col = band_rowcol_dict[band][1]
            band_file_list = h8_dict[t_key][band]
            res_m = res * 100 * 1000
            hm = t_key.split('_')[1]
            file_name = 'HS_H08_%s_%s_%s_%04dM_FLDK.NC' % (
                ymd, hm, band, res_m)
            out_path_file = os.path.join(out_path, file_name)
            dict1 = {'INFO': {'pair': sat_pair, 'ymd': t_key.replace('_', '')},
                     'PROJ': {'left_top_lat': lat_n, 'left_top_lon': lon_w,
                              'row': row, 'col': col, 'lat_res': res, 'lon_res': res},
                     'PATH': {'opath': out_path_file, 'ipath': band_file_list, 'log': str(g_path_log)}}

            # 找到的文件和配置要求的数量一致，才生成yaml
            if len(band_file_list) == len(segm_list):
                write_yaml_file(dict1, out_path_interface)
                print('%s %s %s create yaml interface success' %
                      (sat_pair, t_key, band))


def job_0310(job_exe, sat_pair, job_id, date_list):

    g_var_log.info(u'%s %s 葵花角度计算调度开始 ,时间清单长度 %d' %
                   (sat_pair, job_id, len(date_list)))

    pool = Pool(processes=int(threads))
    for date_s in date_list:
        ymd = date_s.strftime('%Y%m%d')
        #         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0310(sat_pair, job_id, ymd)
    pool.close()
    pool.join()

    # 获取参数文件
    cmd_list = get_cmd_list(
        job_exe, sat_pair, job_id, date_list, g_path_interface)
    return cmd_list


def create_job_0310(sat_pair, job_id, ymd):

    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)
    # 输入
    if res_m > 500:
        key_name = '%dkm001' % (res_m / 1000)
    else:
        key_name = '%dm01' % (res_m)
    in_path = os.path.join(
        g_path_h8geo, 'fygatNAV.Himawari08.xxxxxxx.%s.hdf5' % key_name)

    # 输出
    out_path = os.path.join(g_path_h8ang, ymd)

    # 清理配置接口文件
    out_path_interface = os.path.join(g_path_interface, sat_pair, job_id, ymd)
    if os.path.isdir(out_path_interface):
        shutil.rmtree(out_path_interface)

    date_s = datetime.strptime(ymd, '%Y%m%d')
    date_e = date_s + relativedelta(days=1)

    # 计算0000-2350的角度信息
    while date_s < date_e:
        ymd_hm = date_s.strftime('%Y%m%d_%H%M')
        out_file = os.path.join(
            out_path, 'AHI8_Angle_%04dM_%s.hdf' % (res_m, ymd_hm))
        out_path_interface = os.path.join(
            g_path_interface, sat_pair, job_id, ymd, '%s.yaml' % ymd_hm)

        dict1 = {'INFO': {'pair': sat_pair, 'ymd': ymd_hm, 'res': res_m},
                 'PATH': {'opath': out_file, 'ipath': in_path,
                          'log': str(g_path_log)}}

        write_yaml_file(dict1, out_path_interface)
        print('%s %s create yaml interface success' %
              (sat_pair, ymd_hm))
        date_s = date_s + relativedelta(minutes=10)


def job_0410(job_exe, sat_pair, job_id, date_list):

    g_var_log.info(u'%s %s 葵花L1合成调度开始 ,时间清单长度 %d' %
                   (sat_pair, job_id, len(date_list)))

    pool = Pool(processes=int(threads))
    for date_s in date_list:
        ymd = date_s.strftime('%Y%m%d')
        #         pool.apply_async(create_job_0210, (sat_pair, job_id, ymd))
        create_job_0410(sat_pair, job_id, ymd)
    pool.close()
    pool.join()

    # 获取参数文件
    cmd_list = get_cmd_list(
        job_exe, sat_pair, job_id, date_list, g_path_interface)
    return cmd_list


def create_job_0410(sat_pair, job_id, ymd):

    # 清理配置接口文件
    out_path_interface = os.path.join(g_path_interface, sat_pair, job_id, ymd)
    if os.path.isdir(out_path_interface):
        shutil.rmtree(out_path_interface)

    # 输入
    in_path = os.path.join(g_path_h8nc, ymd)
    in_path_ang = os.path.join(g_path_h8ang, ymd)

    # 输出
    out_path = os.path.join(g_path_h8hdf, ymd)

    # 配置信息
    band_list = g_var_cfg['PAIRS'][sat_pair]['band']
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = '%04dM' % (res * 100 * 1000)

    # 检索NC文件列表
    # HS_H08_20190427_0840_B05_2000M_FLDK.NC
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_%s_FLDK.NC\Z' % res_m
    file_list = find_file(in_path, '.*.NC')
#     print file_list
    # 按照时次把每个时次要合成的文件放到字典
    h8_dict = {}
    for each in file_list:
        file_name = os.path.basename(each)
        m = re.match(reg, file_name)
        if m:
            ymd = m.group(1)
            hm = m.group(2)
            band_name = m.group(3)
            key_name = '%s_%s' % (ymd, hm)
            if key_name not in h8_dict:
                h8_dict[key_name] = []

            # 只处理配置中配置的通道
            if band_name in band_list:
                h8_dict[key_name].append(each)

    # 检索每个时次的文件
    for key_name in sorted(h8_dict.keys()):
        in_file_nc = h8_dict[key_name]
        file_name_angle = 'AHI8_Angle_%s_%s.hdf' % (res_m, key_name)
        in_file_angle = os.path.join(in_path_ang, file_name_angle)
        # 一个时间有几个通道就应该产生几个nc，如果一致则处理
        if len(in_file_nc) == len(band_list):
            if os.path.isfile(in_file_angle):
                out_file = os.path.join(
                    out_path, 'AHI8_OBI_%s_NOM_%s.hdf' % (res_m, key_name))

                out_path_interface = os.path.join(
                    g_path_interface, sat_pair, job_id, ymd, '%s.yaml' % key_name)

                dict1 = {'INFO': {'pair': sat_pair, 'ymd': key_name, 'res': res_m[:4]},
                         'PATH': {'opath': out_file, 'ipath': in_file_nc,
                                  'ipath_angle': in_file_angle, 'log': str(g_path_log)}}

                write_yaml_file(dict1, out_path_interface)
                print('%s %s create yaml interface success' %
                      (sat_pair, key_name))


if __name__ == '__main__':
    main()

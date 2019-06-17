#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/9
@Author  : AnNing
"""
from __future__ import print_function

import os
import sys

import numpy as np
from lib.hdf5 import write_hdf5_and_compress
from lib.initialize import load_yaml_file
from lib.load import ReadAhiL1
from lib.ndvi import cal_ndvi
# add by yan on 2019-05-27---create ndvi tiff/png colorbar
# ndvi range from -0.2 to 1.0 (linear stretch gray range is 0-99)
# color lookup table nadvi_symbol.txt
from lib.ndvi_symbol import create_ndvi_figure


def main(yaml_file):
    """
    :param yaml_file: (str) 接口yaml文件
    :return:
    """
    # ######################## 初始化 ###########################
    # 加载接口文件
    print("main: interface file <<< {}".format(yaml_file))

    interface_config = load_yaml_file(yaml_file)

    i_in_file = interface_config['PATH']['ipath']  # 待处理文件绝对路径（str）
    i_out_file = interface_config['PATH']['opath']  # 输出文件绝对路径（str）
    i_geo_file = interface_config['PATH']['ipath_geo']  # 辅助导航文件绝对路径
    i_out_fig = interface_config['PATH']['opath_fig']  # 输出图片文件绝对路径(str)

    # 如果输出文件已经存在，跳过

    if os.path.isfile(i_out_fig):
        print(
            "***Warning***File is already exist, skip it: {}".format(i_out_fig))
        return

    i_out_fig_dirs = os.path.dirname(i_out_fig)
    if not os.path.isdir(i_out_fig_dirs):
        os.makedirs(i_out_fig_dirs)

    if os.path.isfile(i_out_file):
        print(
            "***Warning***File is already exist, skip it: {}".format(i_out_file))
        return
    cal_ndvi_h8(i_in_file, i_out_file, i_geo_file, i_out_fig)


def cal_ndvi_h8(in_file, out_file, geo_file, out_fig):
    """
    计算H8的植被指数
    :return:
    """
    print('<<< {}'.format(in_file))
    loder = ReadAhiL1(in_file, res=2000, geo_file=geo_file)
    r_vis = loder.get_channel_data('VIS0064')
    r_nir = loder.get_channel_data('VIS0086')
    t_tir = loder.get_channel_data('IRX1120')
    m_tir = loder.get_channel_data('IRX0390')
    # 使用天顶角过滤数据
    solar_zenith = loder.get_solar_zenith()
    index_invalid = solar_zenith > 80
    r_vis[index_invalid] = np.nan
    r_nir[index_invalid] = np.nan
    t_tir[index_invalid] = np.nan
    m_tir[index_invalid] = np.nan

    land_mask = loder.get_land_sea_mask()
    land_condition = np.logical_or(land_mask == 1, land_mask == 2)
    sea_condition = np.logical_or(land_mask == 0, np.logical_and(land_mask > 2, land_mask < 8))

    surface_type_mask = np.ones(land_mask.shape, dtype='uint8')
    surface_type_mask[land_condition] = 1
    surface_type_mask[sea_condition] = 2
    ndvi, flag = cal_ndvi(r_vis, r_nir, m_tir, t_tir, surface_type_mask)

    # 写HDF5文件
    result = {'NDVI': ndvi, 'Flag': flag}
    write_hdf5_and_compress(out_file, result)
    # add by yan --- creete ndvi figure(tiff/jpg)
    create_ndvi_figure(result, out_fig)


# if __name__ == '__main__':
#     in_dir = r'D:\KunYu\hangzhou_anning\H8_L1'
#     out_dir = r'D:\KunYu\hangzhou_anning\H8_NDVI'
#     for filename in os.listdir(in_dir):
#         if '2000' not in filename:
#             continue
#         in_file_ = os.path.join(in_dir, filename)
#         out_file_ = os.path.join(out_dir, 'NDVI_' + filename)
#         cal_ndvi_h8(in_file_, out_file_)


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]：yaml_path
        [example]： python app.py arg1
        """
    if "-h" in ARGS:
        print(HELP_INFO)
        sys.exit(-1)

    if len(ARGS) != 1:
        print(HELP_INFO)
        sys.exit(-1)
    else:
        ARG1 = ARGS[0]
        main(ARG1)

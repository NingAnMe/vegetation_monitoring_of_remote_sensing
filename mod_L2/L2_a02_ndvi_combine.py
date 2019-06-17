#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
from __future__ import print_function

import os
import sys

import numpy as np
from lib.hdf5 import write_hdf5_and_compress
from lib.initialize import load_yaml_file
from lib.load import LoadH8Ndvi
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
    i_in_files = interface_config['PATH']['ipath']  # 待处理文件绝对路径（list）
    i_out_file = interface_config['PATH']['opath']  # 输出文件绝对路径（str）
    # i_geo_file = interface_config['PATH']['ipath_geo']  # 辅助导航文件绝对路径（str）
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

    in_files = list()
    for in_file in i_in_files:
        if os.path.isfile(in_file):
            in_files.append(in_file)
        else:
            print('***WARNING***File is not existent: {}'.format(in_file))

    file_count = len(in_files)
    if len(in_files) <= 0:
        print('###ERROR###The count of Valid files is 0')
        return
    else:
        print('---INFO---File count: {}'.format(file_count))

    combine(in_files, i_out_file, i_out_fig)


def combine(in_files, out_file, out_fig):
    ndvi = None
    flag = None
    for in_file in in_files:
        print('<<< {}'.format(in_file))
        loder = LoadH8Ndvi(in_file)
        ndvi_part = loder.get_ndvi()
        flag_part = loder.get_flag()
        if ndvi is None:
            ndvi = ndvi_part
            flag = flag_part
        else:
            # 新旧值进行比较
            index_max = np.logical_and(flag == 0, flag_part == 0)
            ndvi[index_max] = np.maximum(ndvi[index_max], ndvi_part[index_max])

            # 新值赋值
            index_new = np.logical_and(flag != 0, flag_part == 0)
            ndvi[index_new] = ndvi_part[index_new]
            flag[index_new] = 0

            # 将无效值赋值为云或者水体值
            index_nan = np.logical_and(flag == 3, flag_part != 3)
            ndvi[index_nan] = ndvi_part[index_nan]
            flag[index_nan] = flag_part[index_nan]

    # 写HDF5文件
    result = {'NDVI': ndvi, 'Flag': flag}
    write_hdf5_and_compress(out_file, result)

    # add by yan --- creete ndvi figure(tiff/jpg)
    create_ndvi_figure(result, out_fig)


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

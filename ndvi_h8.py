#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/9
@Author  : AnNing
"""
from __future__ import print_function

import sys

import yaml


from load import LoaderH8L1
from hdf5 import write_hdf5_and_compress
from initialize import load_yaml_file
from ndvi import cal_ndvi


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

    cal_ndvi_h8(i_in_file, i_out_file)


def cal_ndvi_h8(in_file, out_file):
    """
    计算H8的植被指数
    :return:
    """
    print('<<< {}'.format(in_file))
    loder = LoaderH8L1(in_file, res=2000)
    r_vis = loder.get_vis()
    r_nir = loder.get_nir()
    t_tir = loder.get_tir()

    ndvi, flag = cal_ndvi(r_vis, r_nir, t_tir)

    # 写HDF5文件
    result = {'NDVI': ndvi, 'Flag': flag}
    write_hdf5_and_compress(out_file, result)


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

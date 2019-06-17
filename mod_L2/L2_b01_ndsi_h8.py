#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/9
@Author  : AnNing

# !!!!   VALUE = 255 : Fill Data--no Data expected For pixel
# !!!!   VALUE = 254 : Saturated MODIS sensor detector
# !!!!   VALUE = 240 : NATIONAL OR PROVINCIAL BOUNDARIES
# !!!!   VALUE = 200 : Snow
# !!!!   VALUE = 100 : Snow-Covered Lake Ice
# !!!!   VALUE =  50 : Cloud Obscured
# !!!!   VALUE =  39 : Ocean
# !!!!   VALUE =  37 : Inland Water
# !!!!   VALUE =  25 : Land--no snow detected
# !!!!   VALUE =  11 : Darkness, terminator or polar
# !!!!   VALUE =   1 : No Decision
# !!!!   VALUE =   0 : Sensor Data Missing

"""
from __future__ import print_function

import os
import sys

from lib.hdf5 import write_hdf5_and_compress
from lib.initialize import load_yaml_file
from lib.ndsi_h8 import ndsi


SOLAR_ZENITH_MAX = 75


def main(yaml_file):
    """
    :param yaml_file: (str) 接口yaml文件
    :return:
    """
    # ######################## 初始化 ###########################
    # 加载接口文件
    print("main: interface file <<< {}".format(yaml_file))

    interface_config = load_yaml_file(yaml_file)
    i_in_files = interface_config['PATH']['ipath']  # 待处理文件绝对路径（str）
    i_out_file = interface_config['PATH']['opath']  # 输出文件绝对路径（str）

    # 如果输出文件已经存在，跳过
    if os.path.isfile(i_out_file):
        print("***Warning***File is already exist, skip it: {}".format(i_out_file))
        return

    in_file_l1, in_file_geo, in_file_cloud = i_in_files

    cal_ndsi_h8(in_file_l1, in_file_geo, in_file_cloud,  i_out_file)


def cal_ndsi_h8(in_file, geo_file, cloud_file, out_file):
    """
    计算H8的植被指数
    :return:
    """
    print('<<< {}'.format(in_file))
    print('<<< {}'.format(geo_file))
    print('<<< {}'.format(cloud_file))

    ndsi_data, ndsi_flag = ndsi(in_file, geo_file, cloud_file)

    # 写HDF5文件
    result = {'NDSI': ndsi_data, 'Flag': ndsi_flag}
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


# # ######################## 程序全局入口 ##############################
# if __name__ == "__main__":
#     # 获取程序参数接口
#     ARGS = sys.argv[1:]
#     HELP_INFO = \
#         u"""
#         [arg1]：yaml_path
#         [example]： python app.py arg1
#         """
#     if "-h" in ARGS:
#         print(HELP_INFO)
#         sys.exit(-1)
#
#     if len(ARGS) != 1:
#         print(HELP_INFO)
#         sys.exit(-1)
#     else:
#         ARG1 = ARGS[0]
#         main(ARG1)

if __name__ == '__main__':
    in_file_l1 = '/DATA3/HZ_HMW8/H08_L1/ORIGINAL/H08_HDF/20190613/AHI8_OBI_2000M_NOM_20190613_1150.hdf'
    in_file_geo = '/DATA3/HZ_HMW8/H08_L1/ORIGINAL/H08_GEO/H08_GEO_ORIGINAL_2000M.hdf5'
    in_file_cloud = None
    print('<<< {}'.format(in_file_l1))
    print('<<< {}'.format(in_file_geo))
    print('<<< {}'.format(in_file_cloud))

    ndsi_data, ndsi_flag = ndsi(in_file_l1, in_file_geo, in_file_cloud)

    # 写HDF5文件
    result = {'NDSI': ndsi_data, 'Flag': ndsi_flag}
    write_hdf5_and_compress('result/result.hdf', result)

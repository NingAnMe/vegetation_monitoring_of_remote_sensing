#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
from __future__ import print_function

import os
import numpy as np

from hdf5 import write_hdf5_and_compress
from load import LoadH8Ndvi


def combine(in_files, out_file):
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


if __name__ == '__main__':
    in_dir = r'D:\KunYu\hangzhou_anning\H8_NDVI'
    out_dir = r'D:\KunYu\hangzhou_anning\H8_NDVI_COMBINE'
    in_files = [os.path.join(in_dir, i) for i in os.listdir(in_dir)]

    out_file = os.path.join(out_dir, 'NDVI_COMBINE_AHI8_OBI_2000M_NOM_20190427.hdf')
    combine(in_files, out_file)

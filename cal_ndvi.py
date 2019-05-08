#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
import os
import numpy as np

from load import LoaderH8L1
from hdf5 import write_hdf5_and_compress


def cal_ndvi(in_file, out_file):
    """
    计算H8的植被指数
    :return:
    """
    loder = LoaderH8L1(in_file, res=2000)
    r_vis = loder.get_vis()
    r_nir = loder.get_nir()
    t_tir = loder.get_tir()

    ndvi = (r_nir - r_vis) / (r_nir + r_vis)  # 计算植被指数

    rd_nv = r_nir - r_vis  # nir和vis的差值
    rr_nv = r_nir / r_vis  # vir和vis的比值

    # 云判别
    r_vis_th = 0.35
    rr_nv_min = 0.9
    rr_nv_max = 1.1
    t_tir_th = 273
    cloud = np.logical_and.reduce((r_vis >= r_vis_th, rr_nv_min <= rr_nv, rr_nv <= rr_nv_max, t_tir <= t_tir_th))

    # 水体判别
    r_vis_th = 0.15
    r_nir_th = 0.1
    rd_nv_th = 0
    water = np.logical_and.reduce((r_vis <= r_vis_th, r_nir <= r_nir_th, rd_nv <= rd_nv_th))

    # 云和水体的判别标识
    flag = np.zeros_like(ndvi, dtype=np.int8)
    flag[cloud] = 1  # 云的判别标识
    flag[water] = 2  # 水体的判别标识

    # 写HDF5文件
    result = {'NDVI': ndvi, 'Flag': flag}
    write_hdf5_and_compress(out_file, result)


if __name__ == '__main__':
    in_dir = r'D:\KunYu\hangzhou_anning\H8_L1'
    out_dir = r'D:\KunYu\hangzhou_anning\H8_NDVI'
    for filename in os.listdir(in_dir):
        if '2000' not in filename:
            continue
        in_file = os.path.join(in_dir, filename)
        out_file = os.path.join(out_dir, filename)
        cal_ndvi(in_file, out_file)

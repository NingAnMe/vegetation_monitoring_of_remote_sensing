#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
from __future__ import print_function

import numpy as np


def cal_ndvi(r_vis, r_nir, t_tir):
    """
    计算植被指数 ndvi

    :param r_vis:  可见光：无效值使用nan填充
    :param r_nir:  近红外：无效值使用nan填充
    :param t_tir:  亮温：无效值使用nan填充

    :return: ndvi, flag
    flag:
    0: 植被指数
    1： 云判别
    2： 水体判别
    3： 无效遥感数据
    """

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
    flag[np.isnan(ndvi)] = 3  # 无效值标识

    return ndvi, flag

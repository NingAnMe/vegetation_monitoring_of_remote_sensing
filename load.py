#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
from __future__ import print_function

import h5py
import numpy as np


class LoaderH8L1:
    def __init__(self, in_file, res=2000):
        self.in_file = in_file
        self.res = res

    def get_vis(self):
        """
        获取可见光波段数据
        :return:
        """
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannelVIS0064_{}'.format(self.res)
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)
            index = np.logical_or(data == 65535, data <= 0)
            data[index] = np.nan

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset
            return data

    def get_nir(self):
        """
        获取近红外波段数据
        :return:
        """
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannelVIS0086_{}'.format(self.res)
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)
            index = np.logical_or(data == 65535, data <= 0)
            data[index] = np.nan

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset
            return data

    def get_tir(self):
        """
        获取热红外波段数据
        :return:
        """
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannelIRX1120_{}'.format(self.res)
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)
            index = np.logical_or(data == 65535, data <= 0)
            data[index] = np.nan

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset
            return data


class LoadH8Ndvi:
    def __init__(self, in_file, res=2000):
        self.in_file = in_file
        self.res = res

    def get_ndvi(self):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NDVI'
            dataset = h5r.get(name)
            data = dataset[:]
            return data

    def get_flag(self):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'Flag'
            dataset = h5r.get(name)
            data = dataset[:]
            return data

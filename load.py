#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""

import h5py


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
            data = dataset[:]
            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            return data * scale_factor + add_offset

    def get_nir(self):
        """
        获取近红外波段数据
        :return:
        """
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannelVIS0086_{}'.format(self.res)
            dataset = h5r.get(name)
            data = dataset[:]
            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            return data * scale_factor + add_offset

    def get_tir(self):
        """
        获取热红外波段数据
        :return:
        """
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannelIRX1120_{}'.format(self.res)
            dataset = h5r.get(name)
            data = dataset[:]
            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            return data * scale_factor + add_offset

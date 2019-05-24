#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/8
@Author  : AnNing
"""
from __future__ import print_function
import os

import h5py
import numpy as np

from pb_drc_base import ReadL1


class ReadAhiL1(ReadL1):
    def __init__(self, in_file, res=2000, geo_file=None):
        sensor = 'AHI'
        super(ReadAhiL1, self).__init__(in_file, sensor)
        self.in_file = in_file
        self.geo_file = geo_file
        print(self.geo_file)
        self.res = res
        self.channel_um = ['VIS0046', 'VIS0051', 'VIS0064', 'VIS0086',
                           'VIS0160', 'VIS0230', 'IRX0390', 'IRX0620',
                           'IRX0700', 'IRX0730', 'IRX0860', 'IRX0960',
                           'IRX1040', 'IRX1120', 'IRX1230', 'IRX1330']

    def set_ymd_hms(self):
        # AHI8_OBI_2000M_NOM_20190426_2200.hdf
        file_name = os.path.basename(self.in_file)
        self.ymd = file_name.split('_')[4]
        self.hms = file_name.split('_')[5] + '00'

    def set_data_shape(self):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMSunZenith'
            dataset = h5r.get(name)
            self.data_shape = dataset.shape

    def get_ref(self):
        data = {}
        for i, name in enumerate(self.channel_um):
            if 'VIS' not in name:
                continue
            channel = 'CH_{:02d}'.format(i + 1)
            data_c = self.get_channel_data(name)
            data[channel] = data_c
        return data

    def get_tbb(self):
        data = {}
        for i, name in enumerate(self.channel_um):
            if 'IRX' not in name:
                continue
            channel = 'CH_{:02d}'.format(i + 1)
            data_c = self.get_channel_data(name)
            data[channel] = data_c
        return data

    def get_data(self):
        data1 = self.get_ref()
        data2 = self.get_tbb()
        return dict(data1, **data2)

    def get_channel_data(self, name):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMChannel{}_{}'.format(name, self.res)
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)
            index = np.logical_or(data == 65535, data <= 0)
            data[index] = np.nan

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset
            return data

    def get_solar_azimuth(self):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMSunAzimuth'
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset

            index = np.logical_or(data < -180, data > 180)
            data[index] = np.nan
            return data

    def get_solar_zenith(self):
        with h5py.File(self.in_file, 'r') as h5r:
            name = 'NOMSunZenith'
            dataset = h5r.get(name)
            data = dataset[:].astype(np.float32)

            scale_factor = dataset.attrs['scale_factor']
            add_offset = dataset.attrs['add_offset']
            data = data * scale_factor + add_offset
            index = np.logical_or(data < 0, data > 180)
            data[index] = np.nan
            return data

    def get_latitude(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_latitude'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)
                index = np.logical_or(data < -90, data > 90)
                data[index] = np.nan

            return data
        else:
            raise ValueError('GEO file is not exist!')

    def get_longitude(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_longitude'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)
                index = np.logical_or(data < -180, data > 180)
                data[index] = np.nan

            return data
        else:
            raise ValueError('GEO file is not exist!')

    def get_land_sea_mask(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_land_mask'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)
                index = np.logical_or(data == -999, data <= 0)
                data[index] = np.nan

            return data
        else:
            raise ValueError('GEO file is not exist!')

    def get_sensor_azimuth(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_satellite_azimuth_angle'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)
                index = np.logical_or(data < -180, data > 180)
                data[index] = np.nan

            return data
        else:
            raise ValueError('GEO file is not exist!')

    def get_sensor_zenith(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_satellite_zenith_angle'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)
                index = np.logical_or(data < 0, data > 180)
                data[index] = np.nan

            return data
        else:
            raise ValueError('GEO file is not exist!')

    def get_height(self):
        if self.geo_file is not None:
            with h5py.File(self.geo_file, 'r') as h5r:
                name = 'pixel_surface_elevation'
                dataset = h5r.get(name)
                data = dataset[:].astype(np.float32)

            return data
        else:
            raise ValueError('GEO file is not exist!')


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

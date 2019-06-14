# -*- coding: utf-8 -*-
import os
import sys
import yaml
import numpy as np
import h5py
import re
from netCDF4 import Dataset
from PB.CSC.pb_csc_console import LogServer

band_name_alias = {'B01': 'NOMChannelVIS0046',
                   'B02': 'NOMChannelVIS0051',
                   'B03': 'NOMChannelVIS0064',
                   'B04': 'NOMChannelVIS0086',
                   'B05': 'NOMChannelVIS0160',
                   'B06': 'NOMChannelVIS0230',
                   'B07': 'NOMChannelIRX0390',
                   'B08': 'NOMChannelIRX0620',
                   'B09': 'NOMChannelIRX0700',
                   'B10': 'NOMChannelIRX0730',
                   'B11': 'NOMChannelIRX0860',
                   'B12': 'NOMChannelIRX0960',
                   'B13': 'NOMChannelIRX1040',
                   'B14': 'NOMChannelIRX1120',
                   'B15': 'NOMChannelIRX1230',
                   'B16': 'NOMChannelIRX1330'}

band_alias = {'B01': None,
              'B02': None,
              'VIS': 'B03',
              'B04': 'B04',
              'B05': 'B05',
              'B06': 'B06',
              'IR4': 'B07',
              'IR3': 'B08',
              'B09': 'B09',
              'B10': 'B10',
              'B11': 'B11',
              'B12': 'B12',
              'IR1': 'B13',
              'B14': 'B14',
              'IR2': 'B15',
              'B16': 'B16'}


class ReadInYaml():

    def __init__(self, inFile):
        """
        读取yaml格式配置文件
        """
        if not os.path.isfile(inFile):
            print 'Not Found %s' % inFile
            sys.exit(-1)

        with open(inFile, 'r') as stream:
            cfg = yaml.load(stream)
        self.ymd = cfg['INFO']['ymd']
        self.pair = cfg['INFO']['pair']
        self.res = cfg['INFO']['res']
        self.ipath = cfg['PATH']['ipath']
        self.ipath_angle = cfg['PATH']['ipath_angle']
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']
        self.lon_w = cfg['PROJ']['lon_w']
        self.lon_e = cfg['PROJ']['lon_e']
        self.lat_s = cfg['PROJ']['lat_s']
        self.lat_n = cfg['PROJ']['lat_n']
        self.pres = cfg['PROJ']['res']


def main(interface_file):

    # 01 ICFG = 输入配置文件类 ##########
    in_cfg = ReadInYaml(interface_file)
    in_log = LogServer(in_cfg.log)
    in_log.info(u'[%s] [%s] 葵花L1合成开始' % (in_cfg.pair, in_cfg.ymd))

    sat_dtype = in_cfg.pair.split('_')[-1]

    # 创建输出
    out_path = os.path.dirname(in_cfg.opath)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    if os.path.isfile(in_cfg.opath):
        in_log.info(u'文件已经存在 %s' % in_cfg.opath)
        return
    reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_(\w{5})_FLDK.NC\Z'

    lon_w = int(in_cfg.lon_w)
    lon_e = int(in_cfg.lon_e)
    lat_n = int(in_cfg.lat_n)
    lat_s = int(in_cfg.lat_s)

    center_lon = lon_w + (lon_e - lon_w) / 2.
    center_lat = lat_s + (lat_n - lat_s) / 2.
    h5w = h5py.File(in_cfg.opath, 'w')
    h5w.attrs.create('CenterLatitude', center_lat, dtype='f8')
    h5w.attrs.create('CenterLongitude', center_lon, dtype='f8')
    h5w.attrs.create('MaxLat', lat_n,  dtype='i8')
    h5w.attrs.create('MaxLon', lon_e,  dtype='i8')
    h5w.attrs.create('MinLat', lat_s, dtype='i8')
    h5w.attrs.create('MinLon', lon_w,  dtype='i8')

    proj_str = '+units=m +lon_0=145 +datum=WGS84 +proj=latlong _%d-%d-%d-%d' % (
        lat_s, lat_n, lon_w, lon_e)
    print len(proj_str)
    h5w.attrs.create('ProjString', proj_str, dtype='S70')
    h5w.attrs.create('Satellite Name', 'Himawari8', dtype='S10')
    h5w.attrs.create('SensorName', 'OBI',  dtype='S5')
    h5w.attrs.create('UResolution', in_cfg.pres,  dtype='f8')
    h5w.attrs.create('VResolution', in_cfg.pres,  dtype='f8')

    for in_file_nc in in_cfg.ipath:

        file_name = os.path.basename(in_file_nc)
        m = re.match(reg, file_name)
        if m:
            band_name = m.group(3)

            if 'BROADCAST' in sat_dtype:
                band_name = band_alias[band_name]
            # 合并nc数据
            if band_name in ['B01', 'B02', 'B03', 'B04', 'B05', 'B06']:
                nc_dset_name = 'albedo'
                rad = read_nc_file(in_file_nc, nc_dset_name)
                if rad is not None:
                    if 'BROADCAST' in sat_dtype and band_name in ['B04', 'B05', 'B06']:
                        rad = np.round(rad * 100)
                    else:
                        rad = np.round(rad * 10000)
                    dsetname = '%s_%s' % (
                        band_name_alias[band_name], in_cfg.res)
                    dset = h5w.create_dataset(dsetname, dtype='u2', data=rad,
                                              compression='gzip', compression_opts=5, shuffle=True)
                    dset.attrs.create(
                        'add_offset', 0.0, shape=(1,), dtype='f4')
                    dset.attrs.create(
                        'scale_factor', 0.0001, shape=(1,), dtype='f4')
            else:
                nc_dset_name = 'tbb'
                rad = read_nc_file(in_file_nc, nc_dset_name)
                if rad is not None:
                    rad = np.round(rad * 100)
                    dsetname = '%s_%s' % (
                        band_name_alias[band_name], in_cfg.res)
                    dset = h5w.create_dataset(dsetname, dtype='u2', data=rad,
                                              compression='gzip', compression_opts=5, shuffle=True)
                    dset.attrs.create(
                        'add_offset', 0.0, shape=(1,), dtype='f4')
                    dset.attrs.create(
                        'scale_factor', 0.01, shape=(1,), dtype='f4')

    # 合并角度信息
    angle_data = read_angle_file(in_cfg.ipath_angle)
    for key in angle_data.keys():
        value = angle_data[key]
        value = np.round(value * 100)

        dset = h5w.create_dataset(key, dtype='u2', data=value,
                                  compression='gzip', compression_opts=5, shuffle=True)
        dset.attrs.create(
            'add_offset', 0.0, shape=(1,), dtype='f4')
        dset.attrs.create(
            'scale_factor', 0.01, shape=(1,), dtype='f4')
    h5w.close()
    return


def read_angle_file(in_file):
    angle_data = {}

    h5r = h5py.File(in_file, 'r')

    for key in h5r.keys():
        h5data = h5r.get(key)[:]
        angle_data[key] = h5data

    h5r.close()

    return angle_data


def read_nc_file(in_file, dset_name):

    rad = None
    if os.path.isfile(in_file):
        try:
            ncr = Dataset(in_file, 'r', format='NETCDF3_CLASSIC')
            rad = ncr.variables[dset_name][:]
            ncr.close()

        except Exception as e:
            print (str(e))

    # rad <class 'numpy.ma.core.MaskedConstant'> --  不转换居然不能插值
#     rad = np.array(rad)
#     fill_points_2d(rads, -1.)
#     fill_points_2d(rads, -1.)
    return rad

if __name__ == '__main__':

    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        interface_file = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)
    main(interface_file)

# -*- coding: utf-8 -*-
import os
import sys
import h5py
import numpy as np
from configobj import ConfigObj
from lib.com_lib_proj import prj_gll
from pykdtree.kdtree import KDTree
from DP.dp_prj import fill_points_2d
from PB.pb_io import str_format
from pyhdf.SD import SD, SDC
# from scipy.spatial.ckdtree import cKDTree
# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/com.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)
g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']


def main():

    job_list = g_var_cfg['PAIRS'].keys()

    for job in job_list:

        res_deg = float(g_var_cfg['PAIRS'][job]['res'])
        area = g_var_cfg['PAIRS'][job]['area']
        lon_w, lon_e, lat_s, lat_n = area
        dtype = job.split('_')[-1]
        res = int(job.split('_')[-2])

        if 'BROADCAST' in dtype:
            if res == 4000:
                filename = 'fygatNAV.Himawari08.xxxxxxx.2000.hdf4'
            else:
                filename = 'fygatNAV.Himawari08.xxxxxxx.%s.hdf5' % res
        else:
            if res == 2000:
                filename = 'fygatNAV.Himawari08.xxxxxxx.%s.hdf4' % res
            else:
                filename = 'fygatNAV.Himawari08.xxxxxxx.%s.hdf5' % res

        geo_file = os.path.join('/HZ_data/HZ_HMW8/H08_COM', 'GLOBAL', filename)

        filename = 'H08_GEO_%s_%sM.hdf5' % (dtype, res)
        out_file = os.path.join(g_path_h8geo, filename)
        out_file = str_format(out_file, {'H8_TYPE': dtype})

        print job, geo_file, out_file
#         continue

        # 读取geo文件
        if res == 2000 or res == 4000:
            geo_data = read_geo_hdf4(geo_file)
        else:
            geo_data = read_geo_hdf5(geo_file)

#         if os.path.isfile(out_file):
#             continue
        geo_lon = geo_data['pixel_longitude']
        geo_lat = geo_data['pixel_latitude']
        proj = prj_gll(
            lat_n, lat_s, lon_w, lon_e, res_deg, res_deg)
        proj_lat, proj_lon = proj.generateLatsLons()
        row = proj.rowMax
        col = proj.colMax

        # 找找最近
        i2, j2, i1, j1 = create_lut(geo_lon, geo_lat, proj_lon, proj_lat)

        proj_dict = {}
        for geo_key in geo_data:
            if 'mask' in geo_key or 'type' in geo_key:
                proj_dict[geo_key] = np.full((row, col), -128, dtype='i1')
                proj_dict[geo_key][i2, j2] = geo_data[geo_key][i1, j1]
            elif 'pixel_longitude' in geo_key:
                proj_dict[geo_key] = proj_lon
            elif 'pixel_latitude' in geo_key:
                proj_dict[geo_key] = proj_lat
            else:
                proj_dict[geo_key] = np.full((row, col), -999., dtype='f4')

                proj_dict[geo_key][i2, j2] = geo_data[geo_key][i1, j1]

        write_geo_file(out_file, proj_dict)


def write_geo_file(out_file, proj_dict):

    h5w = h5py.File(out_file, 'w')
    for key_name in proj_dict:
        data = proj_dict[key_name]
        h5w.create_dataset(
            key_name, data=data, compression='gzip', compression_opts=5, shuffle=True)
    h5w.close()


def read_geo_hdf5(in_file):
    dict_data = {}
    with h5py.File(in_file, 'r') as hdf5:
        for key_name in hdf5:
            data = hdf5[key_name].value
            # 处理
            dict_data[key_name] = data
    return dict_data


def read_geo_hdf4(in_file):

    dict_data = {}
    h4r = SD(in_file, SDC.READ)
    for key_name in h4r.datasets().keys():
        data = h4r.select(key_name).get()
        dict_data[key_name] = data
    h4r.end()
    return dict_data


def create_lut(lons1, lats1, lons2, lats2):

    # 返回lons2, lats2最近点行列
    shape1 = lons1.shape
    shape2 = lons2.shape

#         lons2 = np.ma.masked_where(np.isnan(lons2), lons2)
#         lats2 = np.ma.masked_where(np.isnan(lats2), lats2)

    combined_x_y_arrays = np.dstack([lons1.ravel(), lats1.ravel()])[0]
    points = np.dstack([lons2.ravel(), lats2.ravel()])[0]
    points = points.astype(np.float32)
#     print type(points[0, 0])

    mytree = KDTree(combined_x_y_arrays)
    dist, index = mytree.query(points)

    # 把index中值转成 Lons1.shape的维度
    i1, j1 = np.unravel_index(index, shape1)
#         print i1, j1
    # 把idx的下标转成Lons2.shape的维度
    idx = np.arange(0, index.size)
    i2, j2 = np.unravel_index(idx, shape2)
    dist = dist.reshape(-1)
    return i2, j2, i1, j1

if __name__ == '__main__':
    main()

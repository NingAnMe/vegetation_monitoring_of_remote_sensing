# -*- coding: utf-8 -*-
import os
import sys
import yaml
import h5py
import numpy as np
from configobj import ConfigObj
from lib.com_lib_proj import prj_gll
from DP.dp_prj import fill_points_2d

# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/com.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)


def main():

    # 01 ICFG = 输入配置文件类 ##########
    res_list = [0.01, 0.005, 0.02]
    area = g_var_cfg['PAIRS']['HIMAWARI+AHI_2000']['area']

    g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']

    # 计算区域 分辨率 行列等信息
    area = [float(each) for each in area]
    lon_w, lon_e, lat_s, lat_n = area

#     res_list = list(set(res_list))
#     res_list = [float(each) for each in res_list]

    for res in res_list:
        res_m = int(res * 100 * 1000)
        if res_m > 500:
            key_name = '%dkm001' % (res_m / 1000)
        else:
            key_name = '%dm01' % (res_m)
        filename = 'fygatNAV.Himawari08.xxxxxxx.%s.hdf5' % key_name
        geo_file = os.path.join(g_path_h8geo, 'GLOBAL', filename)
        print geo_file
        out_file = os.path.join(g_path_h8geo, filename)

        if os.path.isfile(out_file):
            continue
        lons, lats, sata, satz, alt = read_geo_file(geo_file)
        if lons is not None:
            proj = prj_gll(lat_n, lat_s, lon_w, lon_e, res, res)
            proj_sata = np.full((proj.rowMax, proj.colMax), -999.)
            proj_satz = np.full((proj.rowMax, proj.colMax), -999.)
            proj_alt = np.full((proj.rowMax, proj.colMax), -999.)
            proj_lats, proj_lons = proj.generateLatsLons()
            print proj.rowMax, proj.colMax, proj_lons.shape
            lut_i, lut_j = proj.create_lut(lons, lats)

            idx = np.where(lut_i >= 0)
            di = lut_i[idx]
            dj = lut_j[idx]
            pi = idx[0]
            pj = idx[1]
            proj_sata[pi, pj] = sata[di, dj]
            proj_satz[pi, pj] = satz[di, dj]
            proj_alt[pi, pj] = alt[di, dj]

            write_geo_file(
                out_file, proj_lons, proj_lats, proj_sata, proj_satz, proj_alt)


def write_geo_file(out_file, lons, lats, sata, satz, alt):

    if not os.path.isfile(out_file):

        # 补点
        fill_points_2d(sata, -999.)
        fill_points_2d(sata, -999.)
        fill_points_2d(sata, -999.)
        fill_points_2d(satz, -999.)
        fill_points_2d(satz, -999.)
        fill_points_2d(satz, -999.)
        fill_points_2d(alt, -999.)
        fill_points_2d(alt, -999.)
        fill_points_2d(alt, -999.)

        h5w = h5py.File(out_file, 'w')
        h5w.create_dataset(
            'latitude', dtype='f4', data=lats, compression='gzip', compression_opts=5, shuffle=True)
        h5w.create_dataset(
            'longitude', dtype='f4', data=lons, compression='gzip', compression_opts=5, shuffle=True)

        h5w.create_dataset(
            'satellite_azimuth', dtype='f4', data=sata, compression='gzip', compression_opts=5, shuffle=True)
        h5w.create_dataset(
            'satellite_zenith', dtype='f4', data=satz, compression='gzip', compression_opts=5, shuffle=True)
        h5w.create_dataset(
            'surface_elevation', dtype='f4', data=alt, compression='gzip', compression_opts=5, shuffle=True)
        h5w.close()


def read_geo_file(in_file):
    lons = lats = sata = satz = alt = None
    if os.path.isfile(in_file):
        try:
            h5r = h5py.File(in_file, 'r')
            lons = h5r.get('pixel_longitude')[:]
            lats = h5r.get('pixel_latitude')[:]
            sata = h5r.get('pixel_satellite_azimuth_angle')[:]
            satz = h5r.get('pixel_satellite_zenith_angle')[:]
            alt = h5r.get('pixel_surface_elevation')[:]
            h5r.close()

        except Exception as e:
            print (str(e))

    return lons, lats, sata, satz, alt


if __name__ == '__main__':
    pass
    main()

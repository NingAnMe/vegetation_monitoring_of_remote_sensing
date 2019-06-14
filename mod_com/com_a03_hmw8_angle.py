# -*- coding: utf-8 -*-
import os
import sys
import yaml
import numpy as np
import h5py
from datetime import datetime
from PB.CSC.pb_csc_console import LogServer


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
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']


def main(interface_file):

    # 01 ICFG = 输入配置文件类 ##########
    in_cfg = ReadInYaml(interface_file)
    in_log = LogServer(in_cfg.log)
    in_log.info(u'[%s] [%s] 葵花角度计算开始' % (in_cfg.pair, in_cfg.ymd))

    # 创建输出
    out_path = os.path.dirname(in_cfg.opath)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 跳过
    if os.path.isfile(in_cfg.opath):
        in_log.info(u'文件已经存在 %s' % in_cfg.opath)
        return

    # 读取定位文件
    lons, lats, alt = read_geo_file(in_cfg.ipath)
    print lons, lats
    ymd = in_cfg.ymd.split('_')[0]
    hms = in_cfg.ymd.split('_')[1] + '00'

    # 计算角度
    sunz = solar_zenith(ymd, hms, lons, lats)
    suna = solar_azimuth(ymd, hms, lons, lats, alt)
    raa = None
    if in_cfg.res == 2000:
        sata = read_geo_file_sata(in_cfg.ipath)
        raa = np.abs(sata - suna)
        idx = np.where(raa > 180.)
        print idx
        raa[idx] = raa[idx] - 180.
    write_angle_file(in_cfg.opath, suna, sunz, raa)


def write_angle_file(out_file, suna, sunz, raa):
    h5w = h5py.File(out_file, 'w')
    h5w.create_dataset('NOMSunAzimuth', dtype='f4', data=suna,
                       compression='gzip', compression_opts=5, shuffle=True)
    h5w.create_dataset('NOMSunZenith', dtype='f4', data=sunz,
                       compression='gzip', compression_opts=5, shuffle=True)
    if raa is not None:
        h5w.create_dataset('NOMAzimuth', dtype='f4', data=raa,
                           compression='gzip', compression_opts=5, shuffle=True)
    h5w.close()


def read_geo_file(in_file):
    lons = lats = alt = None
    if os.path.isfile(in_file):
        print in_file
        try:
            h5r = h5py.File(in_file, 'r')
            lons = h5r.get('longitude')[:]
            lats = h5r.get('latitude')[:]
            alt = h5r.get('surface_elevation')[:]
            h5r.close()

        except Exception as e:
            print (str(e))

    return lons, lats, alt


def read_geo_file_sata(in_file):
    sata = None
    if os.path.isfile(in_file):
        try:
            h5r = h5py.File(in_file, 'r')
            sata = h5r.get('satellite_azimuth')[:]
            h5r.close()

        except Exception as e:
            print (str(e))

    return sata


def solar_zenith(ymd, hms, lons, lats):
    '''
    Function:    getasol6s
    Description: 计算太阳天顶角
    author:      陈林提供C源码( wangpeng转)
    date:        2017-03-22
    Input:       ymd hms : 20180101 030400
    Output:
    Return:      太阳天顶角弧度类型(修改为度类型 2018年4月28日)
    Others:
    '''
    # jays(儒略日,当年的第几天), GMT(世界时 小时浮点计数方式 )
    dtime = datetime.strptime('%s %s' % (ymd, hms), '%Y%m%d %H%M%S')
    jday = int(dtime.strftime('%j'))
#     print jday
    GMT = float(hms[0:2]) + float(hms[2:4]) / 60.
#     print GMT
    lats = lats * np.pi / 180.
#     lons = lons * np.pi / 180.
    b1 = 0.006918
    b2 = 0.399912
    b3 = 0.070257
    b4 = 0.006758
    b5 = 0.000907
    b6 = 0.002697
    b7 = 0.001480

    a1 = 0.000075
    a2 = 0.001868
    a3 = 0.032077
    a4 = 0.014615
    a5 = 0.040849
    A = 2 * np.pi * jday / 365.0
    delta = b1 - b2 * np.cos(A) + b3 * np.sin(A) - b4 * np.cos(2 * A) + \
        b5 * np.sin(2 * A) - b6 * np.cos(3 * A) + b7 * np.sin(3 * A)
    ET = 12 * (a1 + a2 * np.cos(A) - a3 * np.sin(A) - a4 *
               np.cos(2 * A) - a5 * np.sin(2 * A)) / np.pi
    MST = GMT + lons / 15.0
    TST = MST + ET
    t = 15.0 * np.pi / 180.0 * (TST - 12.0)

    asol = np.arccos(
        np.cos(delta) * np.cos(lats) * np.cos(t) + np.sin(delta) * np.sin(lats))
    return np.rad2deg(asol)


def solar_azimuth(ymd, hms, lons, lats, alt):
    '''
    Function:    solar_azimuth
    Description: 计算太阳方位角
    author:      闵敏提供源码( wangpeng转)
    date:        2019-05-03
    Input:       ymd hms : 20180101 030400
    Output:
    Return:      太阳方位角度
    Others:
    '''

    # 从2000年1月1日算起的天数
    time_current = datetime.strptime('%s' % (ymd), '%Y%m%d')
    time_2000 = datetime(2000, 1, 1, 0, 0, 0)
    jday = (time_current - time_2000).days
#     print jday
    # GMT(世界时 小时浮点计数方式 )
    GMT = float(hms[0:2]) + float(hms[2:4]) / 60.
#     print GMT / 24.
    jday = float(jday) + GMT / 24. + 1
#     print jday

    # Keplerian Elements for the Sun (geocentric)
    w = 282.9404 + 4.70935E-5 * jday  # (longitude of perihelion degrees)
    e = 0.016709 - 1.151E-9 * jday   # (eccentricity)
    M = (356.0470 + 0.9856002585 * jday) % 360  # (mean anomaly degrees)
    L = w + M   # (Sun's mean longitude degrees)
    oblecl = 23.4393 - 3.563E-7 * jday  # (Sun's obliquity of the ecliptic)
    print w, e, M, L, oblecl
    # auxiliary angle
    EE = M + (180 / np.pi) * e * np.sin(M * (np.pi / 180)) * \
        (1. + e * np.cos(M * (np.pi / 180)))

    # rectangular coordinates in the plane of the ecliptic (x axis toward
    # perhelion)
    x = np.cos(EE * (np.pi / 180)) - e
    y = np.sin(EE * (np.pi / 180)) * np.sqrt(1 - e**2)

    # find the distance and true anomaly
    r = np.sqrt(x**2 + y**2)
    ttt = np.arctan(y / x)
    if (y < 0.) and (x < 0.):
        ttt = ttt - np.pi
    if (y > 0.) and (x < 0.):
        ttt = ttt + np.pi
    v = ttt * (180 / np.pi)

    # find the longitude of the sun
    lonSun = v + w

    # compute the ecliptic rectangular coordinates
    xeclip = r * np.cos(lonSun * (np.pi / 180))
    yeclip = r * np.sin(lonSun * (np.pi / 180))
    zeclip = 0.

    # rotate these coordinates to equitorial rectangular coordinates
    xequat = xeclip
    yequat = yeclip * np.cos(oblecl * (np.pi / 180)) + \
        zeclip * np.sin(oblecl * (np.pi / 180))
    zequat = yeclip * np.sin(23.4406 * (np.pi / 180)) + \
        zeclip * np.cos(oblecl * (np.pi / 180))

    # convert equatorial rectangular coordinates to RA and Decl:
    r = np.sqrt(xequat**2 + yequat**2 + zequat**2) - \
        (alt / 1.49598E8)  # roll up the altitude correction

    ttt = np.arctan(yequat / xequat)
    if (yequat < 0.) and (xequat < 0.):
        ttt = ttt - np.pi
    if (yequat > 0.) and (xequat < 0.):
        ttt = ttt + np.pi
    RA = ttt * (180 / np.pi)
    delta = np.arcsin(zequat / r) * (180 / np.pi)

    # UTH = real(hour) + real(minute) / 6.d1 + real(second) / 3.6d3

    # Calculate local siderial time
    GMST0 = (L + 180.) % 360. / 15.
    SIDTIME = GMST0 + GMT + lons / 15.
    # Replace RA with hour angle HA
    HA = (SIDTIME * 15. - RA)
    # ha= ((GMST0 + UTH + Lon / 1.5d1)*1.5d1 - RA)
    # convert to rectangular coordinate system
    x = np.cos(HA * (np.pi / 180)) * np.cos(delta * (np.pi / 180))
    y = np.sin(HA * (np.pi / 180)) * np.cos(delta * (np.pi / 180))
    z = np.sin(delta * (np.pi / 180))

    # rotate this along an axis going east-west.
    xhor = x * np.cos((90. - lats) * (np.pi / 180)) - z * \
        np.sin((90. - lats) * (np.pi / 180))
    yhor = y
    zhor = x * np.sin((90. - lats) * (np.pi / 180)) + z * \
        np.cos((90. - lats) * (np.pi / 180))

    # Find the h and AZ
    ttt = np.arctan(yhor / xhor)

    idx = np.logical_and(yhor < 0, xhor < 0)
    ttt[idx] = ttt[idx] - np.pi

    idx = np.logical_and(yhor > 0, xhor < 0)
    ttt[idx] = ttt[idx] + np.pi
#     if (yhor < 0) and (xhor < 0):
#         ttt = ttt - np.pi
#     if (yhor > 0) and (xhor < 0):
#         ttt = ttt + np.pi
    solar_azimuth = ttt * (180 / np.pi) + 180
#     El = np.arcsin(zhor) * (180 / np.pi)   # 太阳高度角

    return solar_azimuth


if __name__ == '__main__':

    #     t1 = datetime(2000, 1, 1, 0, 0, 0)
    #     t2 = datetime(2017, 1, 1, 0, 0, 0)
    #     print (t2 - t1).days
    #

    #     lons = np.array([92.37291])
    #     lats = np.array([37.07469])
    #     alt = np.array([3150.000])
    #     suna = solar_azimuth('20190502', '035000', lons, lats, alt)
    #     print suna
    #     sunz = solar_zenith('20190502', '035000', lons, lats)
    #     print suna, sunz
    #     sys.exit(-1)
    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        interface_file = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)
    main(interface_file)

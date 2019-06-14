# -*- coding: utf-8 -*-

import re
import numpy as np
import os
import time


class prj_gll():
    '''
    等经纬度区域类
    '''

    def __init__(self, nlat=90., slat=-90., wlon=-180., elon=180., resLat=None, resLon=None, rowMax=None, colMax=None):
        '''
        nlat, slat, wlon, elon: 北纬, 南纬, 西经, 东经
        resLat: 纬度分辨率（度）
        resLon: 经度分辨率（度）
        '''
        self.nlat = float(nlat)  # 北纬
        self.slat = float(slat)  # 南纬
        self.wlon = float(wlon)  # 西经
        self.elon = float(elon)  # 东经

        if resLat is None and rowMax is None:
            raise ValueError("resLat and rowMax must set one")

        if resLon is None and colMax is None:
            raise ValueError("resLon and colMax must set one")

        if resLat is None:
            self.rowMax = int(rowMax)
            self.resLat = (self.nlat - self.slat) / (self.rowMax - 1)
        else:
            self.resLat = float(resLat)
            self.rowMax = int(
                round((self.nlat - self.slat) / self.resLat)) + 1  # 最大行数

        if resLon is None:
            self.colMax = int(colMax)
            self.resLon = (self.elon - self.wlon) / (self.colMax - 1)
        else:
            self.resLon = float(resLon)
            self.colMax = int(
                round((self.elon - self.wlon) / self.resLon)) + 1  # 最大列数

    def generateLatsLons(self):
        lats, lons = np.mgrid[
            self.nlat: self.slat - self.resLat * 0.1:-self.resLat,
            self.wlon: self.elon + self.resLon * 0.1: self.resLon]
        return lats, lons

    def lonslats2ij(self, lons, lats):
        j = self.lons2j(lons)
        i = self.lats2i(lats)
        return i, j

    def lons2j(self, lons):
        '''
        lons: 输入经度
        ret: 返回 输入经度在等经纬度网格上的列号，以左上角为起点0,0
        '''
        if isinstance(lons, (list, tuple)):
            lons = np.array(lons)
        if isinstance(lons, np.ndarray):
            idx = np.isclose(lons, 180.)
            lons[idx] = -180.
        return np.floor((lons - self.wlon) / self.resLon).astype(int)  # 列号

    def lats2i(self, lats):
        '''
        lats: 输入纬度
        ret: 返回 输入纬度在等经纬度网格上的行号，以左上角为起点0,0
        '''
        if isinstance(lats, (list, tuple)):
            lats = np.array(lats)

        return np.floor((self.nlat - lats) / self.resLat).astype(int)  # 行号

    def create_lut(self, lons, lats):
        '''
        '创建投影查找表, 
        '即 源数据经纬度位置与投影后位置的对应关系
        '''
        if isinstance(lons, (list, tuple)):
            lons = np.array(lons)
        if isinstance(lats, (list, tuple)):
            lats = np.array(lats)
        assert lons.shape == lats.shape, \
            "Lons and Lats must have same shape."

        # 投影后的行列 proj1_i,proj1_j
        proj1_i, proj1_j = self.lonslats2ij(lons, lats)

        # 根据投影前数据别分获取源数据维度，制作一个和数据维度一致的数组，分别存放行号和列号
        # 原始数据的行列, data1_i, data1_j
        data1_row, data1_col = lons.shape
        data1_i, data1_j = np.mgrid[0:data1_row:1, 0:data1_col:1]

        # 投影方格以外的数据过滤掉

        condition = np.logical_and.reduce((proj1_i >= 0, proj1_i < self.rowMax,
                                           proj1_j >= 0, proj1_j < self.colMax))
        p1_i = proj1_i[condition]
        p1_j = proj1_j[condition]
        d1_i = data1_i[condition]
        d1_j = data1_j[condition]

        fillValue = -999
        ii = np.full((self.rowMax, self.colMax), fillValue, dtype='i4')
        jj = np.full((self.rowMax, self.colMax), fillValue, dtype='i4')
        # 开始根据查找表对第一个文件的投影结果进行赋值
        ii[p1_i, p1_j] = d1_i
        jj[p1_i, p1_j] = d1_j
#         self.lut_i = ii
#         self.lut_j = jj

        return ii, jj


def find_file(path, reg):
    '''
    path: 要遍历的目录
    reg: 符合条件的文件
    '''
    FileLst = []
    try:
        lst = os.walk(path)
        for root, dirs, files in lst:
            for name in files:
                try:
                    m = re.match(reg, name)
                except Exception as e:
                    continue
                if m:
                    FileLst.append(os.path.join(root, name))
    except Exception as e:
        print str(e)

    return sorted(FileLst)


def str_format(string, values):
    """
    格式化字符串
    :param string:(str) "DCC: %sat_sensor_Projection_%ymd（分辨率 %resolution 度）"
    :param values:(dict) {"sat_sensor": sat_sensor, "resolution": str(resolution), "ymd": ymd}
    :return: DCC: FY3D+MERSI_Projection_201712（分辨率 1 度）
    """
    if not isinstance(string, (str, unicode)):
        return

    for k, v in values.iteritems():
        string = string.replace("%" + str(k), str(v))
    return string


def run_command(cmd_list, threads):
    # 开启进程池

    if len(cmd_list) > 0:
        pool = Pool(processes=int(threads))
        for cmd in cmd_list:
            pool.apply_async(command, (cmd,))
        pool.close()
        pool.join()


def command(args_cmd):
    '''
    args_cmd: python a.py 20180101  (完整的执行参数)
    '''

#     print args_cmd
    try:
        os.system(args_cmd)
    except Exception, e:
        print (e)
        return
if __name__ == '__main__':

    t1 = time.clock()
    time.sleep(10)

    t2 = time.clock()

    print t2 - t1

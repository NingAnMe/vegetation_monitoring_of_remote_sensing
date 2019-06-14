# -*- coding: utf-8 -*-

import re
import numpy as np
import os
import json
import time
from datetime import datetime

from contextlib import contextmanager
from multiprocessing import Pool
import fiona
import shapely.geometry as sgeom

import cartopy.crs as ccrs
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

import matplotlib
matplotlib.use('Agg')
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
from matplotlib.pyplot import text
import matplotlib.patheffects as path_effects


def get_file_time(in_file, pat):

    g = re.match(pat, in_file)
    if g:
        ymd = g.group(1)
        hms = g.group(2) + '00'
        return datetime.strptime('%s%s' % (ymd, hms), "%Y%m%d%H%M%S")
    else:
        return


def read_json(filename):
    """ Read data from JSON file.

    Args:
        filename (str): Name of JSON file to be read.

    Returns:
        dict: Data stored in JSON file.

    """
    with open(filename, 'r') as in_file:
        return json.load(in_file)


def get_geometries_from_shp(shpfile):
    """Get shapely geometry features from shapefile.

    Args:
        shpfile(str): The input shapefile.

    Returns:
        list:  Shapely geometry features.

    """
    with fiona.open(shpfile) as records:
        return [sgeom.shape(shp['geometry']) for shp in records]


def lonlat_inbox(lon, lat, box):
    """
    box = lon_w, lon_e, lat_s, lat_n
    """
    lon_w, lon_e, lat_s, lat_n = box
    if (lon_w < lon < lon_e) and (lat_s < lat < lat_n):
        return True
    else:
        return False


def image_plot_title(x, y, area, title, jcfg, out_file):
    # 制图

    height = 4
    width = height * (float(y) / float(x))

    fig, ax = plt.subplots(subplot_kw=dict(projection=ccrs.PlateCarree()),
                           figsize=(width, height))

    ax.set_extent(area, ccrs.PlateCarree())
    ax.outline_patch.set_visible(False)
    ax.background_patch.set_visible(False)
    ax.background_patch.set_alpha(0)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    myfont = FontProperties(fname=jcfg['city_annotation']['base_font'][
        'fname'],
        style=jcfg['city_annotation']['base_font'][
        'style'],
        weight=jcfg['city_annotation']['base_font'][
        'weight'],
        size=8)

    ax.set_title(title.decode('utf-8'),
                 pad=jcfg['title']['pad'],
                 fontproperties=myfont,
                 color=jcfg['title']['color'])

    # 保存出结果

    fig.savefig(out_file,  transparent=True, dpi=300)

    plt.cla()  # 清除axes，即当前 figure 中的活动的axes，但其他axes保持不变。
    plt.clf()  # 清除当前 figure 的所有axes，但是不关闭这个 window，所以能继续复用于其他的 plot。
    plt.close()  # 关闭 window，如果没有指定，则指当前 window。


def image_plot_shp(x, y, area, jcfg, typhoon, out_file):

    # 读取矢量文件,shapefiles格式
    province_shp = jcfg['province_border']['file']
    jiuduanxian_shp = jcfg['jiuduanxian']['file']
    coast_shp = jcfg['coastline']['file']
    river_shp = jcfg['river']['file']

    typhoon_24_shp = jcfg['typhoon']['typhoon_24']['file']
    typhoon_48_shp = jcfg['typhoon']['typhoon_48']['file']
    typhoon_w_shp = jcfg['typhoon']['typhoon_w']['file']

    with time_block(u"读取shp文件", switch=True):
        geo_river = get_geometries_from_shp(river_shp)
        geo_coast = get_geometries_from_shp(coast_shp)
        geo_province = get_geometries_from_shp(province_shp)
        geo_jiuduanxian = get_geometries_from_shp(jiuduanxian_shp)

        geo_typhoon_24 = get_geometries_from_shp(typhoon_24_shp)
        geo_typhoon_48 = get_geometries_from_shp(typhoon_48_shp)
        geo_typhoon_w = get_geometries_from_shp(typhoon_w_shp)

    # 河流、省边界、九段线、海岸线
    layers1 = {'river': geo_river,
               'province_border': geo_province,
               'jiuduanxian': geo_jiuduanxian,
               'coastline': geo_coast,
               }
    # 台风预警线
    if typhoon:
        layers2 = {'typhoon_24': geo_typhoon_24,
                   'typhoon_48': geo_typhoon_48,
                   'typhoon_w': geo_typhoon_w,
                   }
    else:
        layers2 = {}

    height = 4
    width = height * (float(y) / float(x))
    print area
    print width, height  # ,  (diff_lon / diff_lat)

    # 制图
    fig, ax = plt.subplots(subplot_kw=dict(projection=ccrs.PlateCarree()),
                           figsize=(width, height))

#     print diff_lon,  diff_lat
    ax.set_extent(area, ccrs.PlateCarree())

    # tick 长度, 标注与tick距离等宏观参数
#     ax.tick_params('x', length=jcfg['tick']['xtick']['length'],
#                    pad=jcfg['tick']['xtick']['pad'])
#     ax.tick_params('y', length=jcfg['tick']['ytick']['length'],
#                    pad=jcfg['tick']['ytick']['pad'])
#     # 设置经纬度显示格式
#     ax.set_xticks(jcfg['tick']['xtick']['label'],
#                   crs=ccrs.PlateCarree())
#     ax.set_yticks(jcfg['tick']['ytick']['label'],
#                   crs=ccrs.PlateCarree())
#     lon_formatter = LongitudeFormatter(zero_direction_label=True,
#                                        dateline_direction_label=True)
#
#     lat_formatter = LatitudeFormatter()
#
#     ax.xaxis.set_major_formatter(lon_formatter)
#     ax.yaxis.set_major_formatter(lat_formatter)

    for key in layers1:
        ax.add_geometries(layers1[key], ccrs.PlateCarree(),
                          edgecolor=jcfg[key]['style']['edgecolor'],
                          facecolor=jcfg[key]['style']['facecolor'],
                          linewidth=jcfg[key]['style']['linewidth'])

    # 台风预警线
    if layers2:
        for key in layers2:
            ax.add_geometries(layers2[key], ccrs.PlateCarree(),
                              edgecolor=jcfg['typhoon'][
                                  key]['style']['edgecolor'],
                              facecolor=jcfg['typhoon'][
                                  key]['style']['facecolor'],
                              linewidth=jcfg['typhoon'][
                                  key]['style']['linewidth'],
                              alpha=jcfg['typhoon'][key]['style']['alpha'])

    # 注记主要城市
    labels = {u'武 汉': [114.336851, 30.54776],
              u'南 昌': [115.905177, 28.67752],
              u'杭 州': [120.148899, 30.26788],
              u'上 海': [121.469255, 31.232351],
              u'南 京': [118.757745, 32.062768],
              u'广 州': [113.260938, 23.134385],
              u'厦 门': [118.086493, 24.469189],
              u'台 北': [121.519185, 25.040835],
              u'温 州': [120.650676, 28.018775],
              u'三 亚': [109.503526, 18.262541],
              }
    myfont = FontProperties(fname=jcfg['city_annotation']['base_font'][
        'fname'],
        style=jcfg['city_annotation']['base_font'][
            'style'],
        weight=jcfg['city_annotation']['base_font'][
            'weight'],
        size=8)  # jcfg['city_annotation']['base_font']['size']
#     myfont = FontProperties(fname=jcfg['city_annotation']['base_font'][
#         'fname'], style=jcfg['city_annotation']['base_font'][
#         'style'], weight=jcfg['city_annotation']['base_font'][
#         'weight'], size=jcfg['city_annotation']['beijing']['label']['fontsize'])
    for key in labels:
        lab_lon = labels[key][0]
        lab_lat = labels[key][1]
        if lonlat_inbox(lab_lon, lab_lat, area):
            ax.plot(lab_lon, lab_lat,
                    marker=jcfg['city_annotation']['other']['style'][
                        'marker'],
                    markeredgecolor=jcfg['city_annotation']['other'][
                        'style']['markeredgecolor'],
                    markerfacecolor=jcfg['city_annotation']['other'][
                        'style']['markerfacecolor'],
                    markeredgewidth=jcfg['city_annotation']['other'][
                        'style']['markeredgewidth'],
                    markersize=jcfg['city_annotation']['other'][
                        'style']['markersize'])
            text(lab_lon + jcfg['city_annotation']['other'][
                'label']['x_offset'],
                lab_lat + jcfg['city_annotation']['other'][
                'label']['y_offset'],
                key,
                fontproperties=myfont,
                horizontalalignment=jcfg['city_annotation']['other'][
                'label']['horizontalalignment'],
                verticalalignment=jcfg['city_annotation']['other'][
                'label']['verticalalignment'],
                color=jcfg['city_annotation']['other'][
                'label']['color'])

    # 注记北京
    lab_lon = 116.363447
    lab_lat = 39.933707
    if lonlat_inbox(lab_lon, lab_lat, area):
        ax.plot(lab_lon, lab_lat,
                marker=jcfg['city_annotation']['beijing']['style'][
                    'marker'],
                markeredgecolor=jcfg['city_annotation']['beijing'][
                    'style']['markeredgecolor'],
                markerfacecolor=jcfg['city_annotation']['beijing'][
                    'style']['markerfacecolor'],
                markeredgewidth=jcfg['city_annotation']['beijing'][
                    'style']['markeredgewidth'],
                markersize=jcfg['city_annotation']['beijing'][
                    'style']['markersize'])

        text(lab_lon + jcfg['city_annotation']['beijing']['label'][
            'x_offset'],
            lab_lat + jcfg['city_annotation']['beijing']['label'][
            'y_offset'],
            u'北 京',
            fontproperties=myfont,
            horizontalalignment=jcfg['city_annotation']['beijing'][
            'label']['horizontalalignment'],
            verticalalignment=jcfg['city_annotation']['beijing'][
            'label']['verticalalignment'],
            color=jcfg['city_annotation']['beijing'][
            'label']['color'])

    # 标题
#     ax.set_title(title.decode('utf-8'),
#                  pad=jcfg['title']['pad'],
#                  fontproperties=myfont,
#                  color=jcfg['title']['color'])
    # 有些版本不支持字体大小覆盖
    myfont = FontProperties(  # fname=r'C:\Windows\Fonts\simsun.ttc',
        fname=jcfg['title']['fname'],
        style=jcfg['title']['style'],
        weight=jcfg['title']['weight'],
        size=jcfg['logo']['text']['fontsize'])

    # 气象局logo
    fig_text = fig.text(0.92, 0.015, u'杭州市气象局', fontproperties=myfont,
                        # fontsize=param_dict['logo']['text']['fontsize'],
                        horizontalalignment='right',
                        verticalalignment='bottom',
                        color=jcfg['logo']['text']['color'])

#     text1 = text(logo_x,
#                  logo_y,
#                  u'杭州市气象局',
#                  fontproperties=myfont,
#                  # fontsize=param_dict['logo']['text']['fontsize'],
#                  horizontalalignment=jcfg['logo']['text'][
#                      'horizontalalignment'],
#                  verticalalignment=jcfg['logo']['text'][
#                      'verticalalignment'],
#                  color=jcfg['logo']['text']['color'])
    # 文本效果 https://matplotlib.org/users/patheffects_guide.html
    fig_text.set_path_effects([path_effects.Stroke(
        linewidth=jcfg['logo']['text_effect'][
            'stroke_width'],
        foreground=jcfg['logo']['text_effect'][
            'stroke_color']),
        path_effects.Normal()])

    # 图片
#     ax1 = fig.add_axes(jcfg['logo']['img']['loc'],
#                        frameon=False)
    ax1 = fig.add_axes([0.94, 0.01, 0.04, 0.04],
                       frameon=False)

    im = plt.imread(jcfg['logo']['img']['file'])
    ax1.imshow(im)
    ax1.set_axis_off()

    ax.outline_patch.set_visible(False)
    ax.background_patch.set_visible(False)
    ax.background_patch.set_alpha(0)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    # 保存出结果

    fig.savefig(out_file,  transparent=True,
                # pad_inches=jcfg['save']['pad_inches'],
                # bbox_inches=jcfg['save']['bbox_inches'],
                dpi=300)

    plt.cla()  # 清除axes，即当前 figure 中的活动的axes，但其他axes保持不变。
    plt.clf()  # 清除当前 figure 的所有axes，但是不关闭这个 window，所以能继续复用于其他的 plot。
    plt.close()  # 关闭 window，如果没有指定，则指当前 window。


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


class prj_gll2():
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
            self.resLat = (self.nlat - self.slat) / self.rowMax
        else:
            self.resLat = float(resLat)
            self.rowMax = int(
                round((self.nlat - self.slat) / self.resLat))  # 最大行数

        if resLon is None:
            self.colMax = int(colMax)
            self.resLon = (self.elon - self.wlon) / self.colMax
        else:
            self.resLon = float(resLon)
            self.colMax = int(
                round((self.elon - self.wlon) / self.resLon))  # 最大列数

    def generateLatsLons(self):
        lats, lons = np.mgrid[
            self.nlat - self.resLat / 2.: self.slat + self.resLat * 0.1:-self.resLat,
            self.wlon + self.resLon / 2.: self.elon - self.resLon * 0.1: self.resLon]
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


@contextmanager
def time_block(flag, switch=True):
    """
    计算一个代码块的运行时间
    :param flag: 标签
    :param on: 是否开启
    :return:
    """
    time_start = datetime.now()
    try:
        yield
    finally:
        if switch is True:
            time_end = datetime.now()
            all_time = (time_end - time_start).seconds
            print "{} time: {}".format(flag, all_time)


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

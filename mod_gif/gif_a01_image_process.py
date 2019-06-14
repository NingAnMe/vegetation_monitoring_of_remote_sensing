# -*- coding: utf-8 -*-

import sys
import numpy as np
from multiprocessing import Pool
import os
import math
import argparse
from dateutil.relativedelta import relativedelta
from datetime import datetime
from configobj import ConfigObj

from lib.gif_lib_image import prj_gll, get_file_time
from lib.gif_lib_image import find_file, time_block, run_command
from lib.gif_lib_image import read_json, image_plot_title, image_plot_shp
# import matplotlib
# matplotlib.use('Agg')
import matplotlib.pyplot as plt


# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/gif.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)
g_convert = '/bin/convert'
g_mogrify = '/bin/mogrify'

threads = int(g_var_cfg['CROND']['threads'])


def find_time_range_file(ipath, t1, t2, reg):

    date_start = datetime.strptime(t1, '%Y%m%d%H%M%S')
    date_end = datetime.strptime(t2, '%Y%m%d%H%M%S')

    date_s = date_start
    date_e = date_end
    file_list = []
    while date_s <= date_e:
        ymd = date_s.strftime('%Y%m%d')
        full_in_path = os.path.join(ipath, ymd)
        file_list_ymd = find_file(full_in_path, '.*')

        for in_file in file_list_ymd:
            file_name = os.path.basename(in_file)
            file_time = get_file_time(file_name, reg)

            if date_start <= file_time <= date_end:
                file_list.append(in_file)

        date_s = date_s + relativedelta(days=1)

    return file_list


def image_crop(file_list, lon_w, lon_e, lat_s, lat_n, opath):
    """
    抠图
    """
    latlon = g_var_cfg['PAIRS']['CLR_1000M']['latlon']
    rowcol = g_var_cfg['PAIRS']['CLR_1000M']['rowcol']
    row, col = [int(each) for each in rowcol]
    latlon = [float(each) for each in latlon]
    g_lon_w, g_lon_e, g_lat_s, g_lat_n = latlon

    proj = prj_gll(g_lat_n, g_lat_s, g_lon_w, g_lon_e, None, None, row, col)
    start_i, start_j = proj.lonslats2ij(lon_w, lat_n)
    end_i, end_j = proj.lonslats2ij(lon_e, lat_s)

    width = end_j - start_j
    height = end_i - start_i

    print 'width*height', width, height, start_j, start_i
    size = '%dx%d+%d+%d' % (width, height, start_j, start_i)

    cmd_list = []

    for in_file in file_list:
        file_name = os.path.basename(in_file)
        out_file = os.path.join(opath, file_name)
        cmd = '%s -crop %s %s %s' % (g_convert, size, in_file, out_file)
        print cmd
#         if os.path.isfile(out_file):
#             continue
        cmd_list.append(cmd)

    run_command(cmd_list, threads)


def imgage_resize(file_list, percent):
    """
    重置图像大小
    """

    cmd_list = []
    for in_file in file_list:
        cmd = '%s -resize %d%%x%d%% %s %s' % (g_convert,
                                              percent, percent, in_file, in_file)
        cmd_list.append(cmd)

    if len(cmd_list) > 0:
        run_command(cmd_list, threads)


def image_mogrify(file_list, x, y, out_shp_path):
    """
    叠加图片
    """

    # convert  -fill white -pointsize 24 -draw "text 10,15 'lifesinger 2006' "
    # 1.png  2.png
    # convert   -fill white -pointsize 24 -font
    # /home/gsics/Project/OM/hz/font/msyh.ttf  -draw "text 1950,50 '哈哈哈'"
    # H8XX_AHIXX_L2_GLL_20190421_0000_1000M_CLR_GeoColor.jpg 2.jpg

    # 获取字体文件

    # 获取数据所在位置 cd 过去
    os.chdir(out_shp_path)

    # 标题
    cmd_list = []
    for in_file in file_list:
        file_name = os.path.basename(in_file)
        file_name_shpae = file_name.split('.')[0] + '.png'
        # cmd = '''%s -fill white -pointsize 20  -stroke black -font %s
        # -draw "text %d,%d '%s'" -draw 'image SrcOver 0,0 %d,%d %s' %s''' % (
        # g_mogrify, font_file, title_y, title_x, new_title, img_y, img_x,
        # shp_file, file_name)

        cmd = '''%s -draw 'image SrcOver 0,0 %d,%d %s' %s''' % (
            g_mogrify,  y, x, file_name_shpae, in_file)
        cmd_list.append(cmd)

    run_command(cmd_list, threads)

    # shp边界
    cmd_list = []
    for in_file in file_list:
        cmd = '''%s -draw 'image SrcOver 0,0 %d,%d %s' %s''' % (
            g_mogrify,  y, x, 'overlap.png', in_file)
        cmd_list.append(cmd)

    run_command(cmd_list, threads)


def image_gif(file_list, out_gif_file):
    """
    制作动画
    """

    cmd_list = []
    out_file_list = []
    out_path = os.path.dirname(out_gif_file)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    for in_file in file_list:
        file_name = os.path.basename(in_file)

        out_file = os.path.join(out_path, file_name.split('.')[0] + '.gif')
        out_file_list.append(out_file)
#         if os.path.isfile(out_file):
#             continue
        cmd = '%s -delay 25 %s  %s' % (g_convert, in_file, out_file)
        cmd_list.append(cmd)

    if len(cmd_list) > 0:
        run_command(cmd_list, threads)

    os.system('%s -loop 0 %s %s ' %
              (g_convert, ' '.join(out_file_list), out_gif_file))


if __name__ == '__main__':

    ipath = g_var_cfg['PATH']['IN']['h8_jpg']
    mpath = g_var_cfg['PATH']['MID']['h8_crop']
    opath = g_var_cfg['PATH']['OUT']['h8_gif']

    reg = g_var_cfg['PAIRS']['CLR_1000M']['reg']
    json_file = g_var_cfg['PAIRS']['CLR_1000M']['json']
    title = g_var_cfg['PAIRS']['CLR_1000M']['title']
    typhoon = g_var_cfg['PAIRS']['CLR_1000M']['typhoon']

    # 输入
    parser = argparse.ArgumentParser()
    parser.add_argument("-t1", "--time1", help="YYYYMMDDHHMMSS")
    parser.add_argument("-t2", "--time2", help="YYYYMMDDHHMMSS")
    parser.add_argument(
        "-A", "--area", help="lat,lon (Left upper) lat,lon (lower right)")
    parser.add_argument(
        "-N", "--name", help="area name")
    args = parser.parse_args()
    t1 = args.time1
    t2 = args.time2
    latlon = [float(each) for each in args.area.split(',')]
    lon_w, lon_e, lat_s, lat_n = latlon
    name = unicode(args.name)

    print 'args', t1, t2, latlon, name

    # 0.  找到符合条件的文件
    file_list = find_time_range_file(ipath, t1, t2, reg)

    # 1. 开始从原图把需要的区域扣图

    crop_opath = os.path.join(mpath, name, '%s_%s' % (t1, t2))
    if not os.path.isdir(crop_opath):
        os.makedirs(crop_opath)

    with time_block(u'抠图耗时', True):
        image_crop(file_list, lon_w, lon_e, lat_s, lat_n, crop_opath)
        pass

    # 2. 重新定义大小 判断是否缩小 100万像素是分界点

    file_list = find_file(crop_opath, '.*.jpg')
    in_file = file_list[0]
    img = plt.imread(in_file)
    img_x, img_y = img.shape[:2]

    dividing_point = 1000000.
    all_point = img_x * img_y

    if all_point > dividing_point:
        scale = math.ceil((dividing_point / all_point) * 100)
        if scale <= 30.:
            scale = 30
        with time_block(u'等比例缩小 %d' % scale, True):
            imgage_resize(file_list, scale)
            pass

    # 缩放完成重新取大小
    in_file = file_list[0]
    img = plt.imread(in_file)
    img_x, img_y = img.shape[:2]
    plt.close()

    print img_x, img_y

    # 2. 开始绘制边界
    shp_path = os.path.join(crop_opath, 'shp')
    if not os.path.isdir(shp_path):
        os.makedirs(shp_path)
    jcfg = read_json(json_file)
    pool = Pool(processes=int(threads))
    with time_block(u"制作标题png", switch=True):
        for in_file in file_list:
            file_name = os.path.basename(in_file)
            out_file = os.path.join(
                crop_opath, 'shp', file_name.split('.')[0] + '.png')
            tt = get_file_time(file_name, reg)
            tt = tt + relativedelta(hours=8)
            ymd_hm = tt.strftime('%Y-%m-%d %H:%M')
            format_title = title % ymd_hm
#             print img_x, img_y, latlon, format_title, out_file
            pool.apply_async(
                image_plot_title, (img_x, img_y, latlon, format_title, jcfg, out_file))

        pool.close()
        pool.join()

    # 2. 开始绘制边界

    with time_block(u"制作矢量边界", switch=True):
        out_shp_file = os.path.join(crop_opath, 'shp', 'overlap.png')
        image_plot_shp(img_x, img_y, latlon, jcfg, typhoon, out_shp_file)

    # 3. 贴图
    out_shp_path = os.path.join(crop_opath, 'shp')
    with time_block(u'贴图', True):
        image_mogrify(file_list, img_x, img_y, out_shp_path)
        pass

    # 4. 动画
    os.chdir(g_path)
    out_file = os.path.join(
        opath, name, '%s_%s' % (t1, t2), '%s_%s.gif' % (t1, t2))
    with time_block(u'动画', True):
        image_gif(file_list, out_file)
        pass

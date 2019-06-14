# -*- coding: utf-8 -*-
import os
import sys
import yaml
from datetime import datetime
from dateutil.relativedelta import relativedelta
from PB.CSC.pb_csc_console import LogServer
from PB.DBC.pb_dbc_mysql import DBUtil

g_db = DBUtil()
g_db.setDBConnector(g_db.connector_DB_hz)


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
        self.ptype = cfg["INFO"]['ptype']
        self.dtype = cfg["INFO"]['dtype']
        self.pair = cfg['INFO']['pair']
        self.ipath = cfg['PATH']['ipath']
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']


def main(interface_file):

    # 01 ICFG = 输入配置文件类 ##########
    in_cfg = ReadInYaml(interface_file)
    in_log = LogServer(in_cfg.log)
    in_log.info(u'[%s] [%s] TIFF切片程序开始运行' % (in_cfg.pair, in_cfg.ymd))

    # 创建输出
    out_path = os.path.dirname(in_cfg.opath)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # 需要调用第三方exe来进行轨迹计算
    th_exe = os.path.join('bin', 'gdal2tiles.py')
    if not os.path.isfile(th_exe):
        in_log.error(u'Not Found %s' % th_exe)
        return

    cmd = '/HZ_data/HZ_LIB/anaconda3/bin/python %s --processes=4 -s EPSG:4326 -z 3-8 %s %s > /dev/null' % (
        th_exe, in_cfg.ipath, in_cfg.opath)

    lst = os.listdir(in_cfg.opath)

    # 转北京时
    utime = datetime.strptime('%s' % in_cfg.ymd, '%Y%m%d_%H%M')
    utime = utime + relativedelta(hours=8)

    if len(lst) >= 8:
        print '%s is already exists skip' % in_cfg.opath
        db_insert_product(utime, in_cfg.opath, in_cfg.dtype, in_cfg.ptype)
        return
    else:
        if 0 == os.system(cmd):
            print 'sucess %s' % in_cfg.ipath
            db_insert_product(utime, in_cfg.opath, in_cfg.dtype, in_cfg.ptype)


def db_insert_product(utime, opath, dtype, ptype):

    tbl_name = 'm_product_data'
    if not db_select_product(utime, dtype, ptype):
        sql = u"""INSERT INTO %s(cutpic_path,data_type,product_type,datetime) VALUES('%s','%s','%s','%s')""" % (
            tbl_name, opath, dtype, ptype, utime)
        g_db.executeInsert(sql)


def db_select_product(utime, dtype, ptype):

    tbl_name = 'm_product_data'
    sql = "SELECT id FROM %s WHERE datetime='%s' and data_type='%s' and product_type='%s' and cutpic_path is not NULL" % (
        tbl_name, utime, dtype, ptype)
    res = g_db.executeSearch(sql)

    if len(res) > 0:
        print '%s %s is already indb skip' % (utime, ptype)
        return True

if __name__ == '__main__':

    #     utime = '2019-05-20 00:00:00'
    #     opath = '/HZ_data/HZ_HMW8/H08_TILES/Original/GeoColor/20190520/0000'
    #     dtype = 'original'
    #     ptype = 'GeoColor'
    #     db_insert_product(utime, opath, dtype, ptype)
    #     db_select_product(utime, dtype, ptype)
    #     sys.exit()

    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        interface_file = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)
    main(interface_file)

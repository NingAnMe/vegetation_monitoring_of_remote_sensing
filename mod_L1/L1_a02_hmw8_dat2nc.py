# -*- coding: utf-8 -*-
import os
import sys
import yaml

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
        self.row = cfg['PROJ']['row']
        self.col = cfg['PROJ']['col']
        self.lat = cfg['PROJ']['left_top_lat']
        self.lon = cfg['PROJ']['left_top_lon']
        self.lat_res = cfg['PROJ']['lat_res']
        self.lon_res = cfg['PROJ']['lon_res']
        self.row = cfg['PROJ']['row']
        self.ymd = cfg['INFO']['ymd']
        self.pair = cfg['INFO']['pair']
        self.ipath = cfg['PATH']['ipath']
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']


def main(interface_file):

    # 01 ICFG = 输入配置文件类 ##########
    in_cfg = ReadInYaml(interface_file)
    in_log = LogServer(in_cfg.log)
    in_log.info(u'[%s] [%s] 葵花dat转nc计算开始' % (in_cfg.pair, in_cfg.ymd))

    if os.path.isfile(in_cfg.opath):
        in_log.info(u'文件已经存在 %s' % in_cfg.opath)
        return

    if 'BROADCAST' in in_cfg.pair:
        h8_exe = os.path.join('bin', 'hmw8_broadcast_dat2nc.exe')
    else:
        h8_exe = os.path.join('bin', 'hmw8_dat2nc.exe')

    # 需要调用第三方exe来进行轨迹计算

    if not os.path.isfile(h8_exe):
        in_log.error(u'Not Found %s' % h8_exe)
        return

    # 创建输出
    out_path = os.path.dirname(in_cfg.opath)
    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    input_file = '-i ' + ' -i '.join(in_cfg.ipath)
    # 执行轨迹计算可执行程序
    cmd = '%s %s -o %s -width %d -height %d -lat %f -lon %f -dlat %f -dlon %f > /dev/null' % (
        h8_exe, input_file, in_cfg.opath, in_cfg.col, in_cfg.row,
        in_cfg.lat, in_cfg.lon, in_cfg.lat_res, in_cfg.lon_res)
    print cmd
    os.system(cmd)

    return


if __name__ == '__main__':
    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        interface_file = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)
    main(interface_file)

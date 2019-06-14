# -*- coding: utf-8 -*-
import os
import sys
import yaml
import shutil
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
        self.ipath = cfg['PATH']['ipath']
        self.opath = cfg['PATH']['opath']
        self.log = cfg['PATH']['log']


def main(interface_file):

    # 01 ICFG = 输入配置文件类 ##########
    in_cfg = ReadInYaml(interface_file)
    in_log = LogServer(in_cfg.log)
    in_log.info(u'[%s] [%s] 葵花bz2转dat处理开始' % (in_cfg.pair, in_cfg.ymd))

    # 输入数据
    in_file = in_cfg.ipath

    # 输出数据
    out_file = in_cfg.opath

    # 需要调用第三方exe来进行轨迹计算
    h8_exe = os.path.join('bin', 'bunzip2')
    if not os.path.isfile(h8_exe):
        in_log.error(u'Not Found %s' % h8_exe)
        return

    if os.path.isfile(out_file):
        return
#         file_size = os.path.getsize(out_file)
#         if file_size > 0:
#             return
#         else:
#             cmd = '%s -k -c %s > %s' % (h8_exe, in_file, out_file)
#             if 0 == os.system(cmd):
#                 print (out_file, 'ok')

    else:
        cmd = '%s -k -c %s > %s' % (h8_exe, in_file, out_file)
#         print cmd
        if 0 == os.system(cmd):
            print 'sucess %s' % out_file
        else:
            print 'failed %s' % out_file
            print cmd
            os.remove(out_file)
            #os.remove(in_file)

if __name__ == '__main__':
    # 获取python输入参数，进行处理
    args = sys.argv[1:]
    if len(args) == 1:  # 跟参数，则处理输入的时段数据
        interface_file = args[0]
    else:
        print 'input args error exit'
        sys.exit(-1)
    main(interface_file)

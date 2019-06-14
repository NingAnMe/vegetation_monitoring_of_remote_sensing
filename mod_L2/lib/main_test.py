#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/17
@Author  : AnNing
"""
from __future__ import print_function
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import sys

import yaml


def main(period, start_date, end_date):
    """
    :return:
    """

    result_dir = '/HZ_data/HZ_HMW8/H08_COM/ORIGINAL/NDVI'
    h8_l1_dir = '/HZ_data/HZ_HMW8/H08_COM/ORIGINAL/H08_HDF'

    out_dir_yaml = os.path.join(result_dir, 'Yaml')
    if period == 'Orbit':
        in_dir_file = h8_l1_dir
        out_dir_file = os.path.join(result_dir, 'Orbit')
        in_files = get_files_by_ymd(in_dir_file, start_date, end_date)

        print('File count: {}'.format(len(in_files)))

        for in_file in in_files:
            file_name = os.path.basename(in_file)

            out_file = os.path.join(out_dir_file, file_name)
            yaml_data = {
                'PATH': {
                    'ipath': in_file,
                    'opath': out_file,
                }
            }

            file_name_yaml = file_name.replace('hdf', 'yaml')
            out_yaml = os.path.join(out_dir_yaml, file_name_yaml)
            with open(out_yaml, 'w') as stream:
                yaml.dump(yaml_data, stream, default_flow_style=False)
            os.system('python ndvi_h8.py {}'.format(out_yaml))

    elif period == 'Daily':
        in_dir_file = os.path.join(result_dir, 'Orbit')
        out_dir_file = os.path.join(result_dir, 'Daily')
        date_start = datetime.strptime(start_date, '%Y%m%d')
        date_end = datetime.strptime(end_date, '%Y%m%d')

        while date_start <= date_end:
            date_temp = date_start.strftime('%Y%m%d')
            in_files = get_files_by_ymd(in_dir_file, date_temp, date_temp)
            out_file_name = 'AHI8_OBI_2000M_NOM_{}.hdf'.format(date_temp)
            out_file = os.path.join(out_dir_file, out_file_name)
            yaml_data = {
                'PATH': {
                    'ipath': in_files,
                    'opath': out_file,
                }
            }
            file_name_yaml = out_file_name.replace('hdf', 'yaml')
            out_yaml = os.path.join(out_dir_yaml, file_name_yaml)
            with open(out_yaml, 'w') as stream:
                yaml.dump(yaml_data, stream, default_flow_style=False)
            os.system('python ndvi_combine.py {}'.format(out_yaml))
            date_start = date_start + relativedelta(days=1)
    elif period == 'Weekly':
        in_dir_file = os.path.join(result_dir, 'Daily')
        out_dir_file = os.path.join(result_dir, 'Weekly')

        in_files = get_files_by_ymd(in_dir_file, start_date, end_date)
        out_file_name = 'AHI8_OBI_2000M_NOM_{}.hdf'.format(end_date)
        out_file = os.path.join(out_dir_file, out_file_name)
        yaml_data = {
            'PATH': {
                'ipath': in_files,
                'opath': out_file,
            }
        }
        file_name_yaml = out_file_name.replace('hdf', 'yaml')
        out_yaml = os.path.join(out_dir_yaml, file_name_yaml)
        with open(out_yaml, 'w') as stream:
            yaml.dump(yaml_data, stream, default_flow_style=False)
        os.system('python ndvi_combine.py {}'.format(out_yaml))


def get_files_by_ymd(dir_path, time_start, time_end, ext=None, pattern_ymd=None):
    """

    @AnNing anning@kingtansin.com
    :param dir_path: 文件夹
    :param time_start: 开始时间
    :param time_end: 结束时间
    :param ext: 后缀名, '.hdf5'
    :param pattern_ymd: 匹配时间的模式, 可以是 r".*(\d{8})_(\d{4})_"
    :return: list
    """
    files_found = []
    if pattern_ymd is not None:
        pattern = pattern_ymd
    else:
        pattern = r".*(\d{8})"
    print(dir_path)
    for root, dirs, files in os.walk(dir_path):
        for file_name in files:
            if ext is not None:
                if '.' not in ext:
                    ext = '.' + ext
                if os.path.splitext(file_name)[1].lower() != ext.lower():
                    continue
            re_result = re.match(pattern, file_name)
            if re_result is not None:
                time_file = ''.join(re_result.groups())
            else:
                continue
            if int(time_start) <= int(time_file) <= int(time_end):
                files_found.append(os.path.join(root, file_name))
    files_found.sort()
    return files_found


# ######################## 程序全局入口 ##############################
if __name__ == "__main__":
    # 获取程序参数接口
    ARGS = sys.argv[1:]
    HELP_INFO = \
        u"""
        [arg1]： Period
        [arg1]： date start
        [arg1]： date end
        [example]： python main_test.py Daily 20190513 20190513
        """

    print(HELP_INFO)
    main(*ARGS)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/5/17
@Author  : AnNing
"""
from __future__ import print_function
import os


def main():
    """
    :return:
    """
    in_dir = '/DATA2/KTS/CMA_KTS/COM2/H08_L1B/FullDisk'
    in_files = get_files_by_ymd(in_dir, )


def slt_histogram_create_yaml(sat_sensor, job_id, date_now, day_or_month):
    """
    :param job_id:
    :param sat_sensor:
    :param date_now:
    :param day_or_month: Daily or Monthly
    :return:
    """
    sat, sensor = sat_sensor.split('+')
    if day_or_month not in ('Daily', 'Monthly'):
        print("Please check the 'day_or_month' argument")
        return
    if day_or_month == 'Monthly':
        is_monthly = True
    else:
        is_monthly = False

    if is_monthly:
        ymd_now = date_now.strftime("%Y%m")
    else:
        ymd_now = date_now.strftime("%Y%m%d")

    in_path = str_format(g_slt_path, {
        'pair': sat_sensor,
        'sat': sat,
        'sensor': sensor,
        'YYYY': ymd_now[0:4],
        'MM': ymd_now[4:6],
        'DD': ymd_now[6:8],
    })

    if is_monthly:
        date_first = date_now - relativedelta(days=date_now.day + 1)
        date_last = date_first + relativedelta(months=1) - relativedelta(days=1)
        ymd_first = date_first.strftime("%Y%m%d")
        ymd_last = date_last.strftime("%Y%m%d")
        in_files = get_files_by_ymd(in_path, ymd_first, ymd_last)
    else:
        in_files = get_files_by_ymd(in_path, ymd_now, ymd_now)
    if len(in_files) == 0:
        print 'Dont found any in_file.: {}'.format(ymd_now)
        return

    out_path = str_format(g_histogram_path, {
        'pair': sat_sensor,
        'day_or_month': day_or_month,
        'YYYY': ymd_now[0:4],
        'MM': ymd_now[4:6],
        'DD': ymd_now[6:8],
    })

    yaml_path = str_format(g_temp_path, {
        'pair': sat_sensor,
        'job': job_id,
        'YYYY': ymd_now[0:4],
        'MM': ymd_now[4:6],
    })
    yaml_file_name = '{}_{}.yaml'.format(ymd_now, day_or_month)
    yaml_file = os.path.join(yaml_path, yaml_file_name)

    yaml_data = {}
    info = yaml_data['info'] = {}
    path = yaml_data['path'] = {}

    info['pair'] = sat_sensor
    info['day_or_month'] = day_or_month
    info['ymd'] = ymd_now

    path['ipath'] = in_files
    path['opath'] = out_path

    make_sure_path_exists(os.path.dirname(yaml_file))
    with file(yaml_file, 'w') as stream:
        yaml.dump(yaml_data, stream, default_flow_style=False)

    return yaml_file


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


# -*- coding: utf-8 -*-

from datetime import datetime
import os
import re
import shutil
import sys
from multiprocessing import Pool
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from PB.pb_io import write_yaml_file, find_file
import argparse
from PB.DBC.pb_dbc_mysql import DBUtil

g_db = DBUtil()
g_db.setDBConnector(g_db.connector_DB_hz)

band_alias = {'B01': None,
              'B02': None,
              'VIS': 'B03',
              'B04': 'B04',
              'B05': 'B05',
              'B06': 'B06',
              'IR4': 'B07',
              'IR3': 'B08',
              'B09': 'B09',
              'B10': 'B10',
              'B11': 'B11',
              'B12': 'B12',
              'IR1': 'B13',
              'B14': 'B14',
              'IR2': 'B15',
              'B16': 'B16'}


def select_data_desc():

    tbl_name = 'data_dictionary'
    sql = "SELECT id,resolving,address FROM %s" % (tbl_name)
    res = g_db.executeSearch(sql)
    return res


def select_data_info(file_name):

    flag = False
    tbl_name = 'download_data'
    sql = "SELECT name FROM %s where name='%s'" % (tbl_name, file_name)
    res = g_db.executeSearch(sql)
    if len(res) > 0:
        flag = True

    return flag


def insert_data_info(indb_list):
    tbl_name = 'download_data'
    sql = u"""INSERT INTO %s(dataid,name,address,datatime,createtime,downloadtime,
    datasize,passage,block) VALUES(%s)""" % (tbl_name, ','.join(['%s'] * 9))
    g_db.executeInsert(sql, indb_list)


def get_local_file_lst(ymd_s, date_s, date_e):

    indb_list = []
    db_res = select_data_desc()
    for each in db_res:
        data_id = each[0]
        data_res = int(each[1])
        in_path = each[2]

        sat_dtype = data_id.split('_')[-1]
        if 'BC' in sat_dtype:
            if data_res == 1000:
                reg_res = 'VIS'
            elif data_res == 4000:
                reg_res = '\w{2}\d{1}'
            reg = u'IMG_DK01(%s)_(\d{8})(\d{4})_(\w{3})' % reg_res

        else:
            # 数据库分辨率转实际数据标记
            if data_res == 1000:
                reg_res = 'R10'
            elif data_res == 2000:
                reg_res = 'R20'
            elif data_res == 500:
                reg_res = 'R05'

            reg = u'HS_H08_(\d{8})_(\d{4})_(\w{3})_FLDK_%s_(\w{5}).DAT\Z' % reg_res
    #     # 输入
        full_inpath = os.path.join(in_path, ymd_s)
        print data_id, data_res, sat_dtype, full_inpath
        file_list = find_file(full_inpath, reg)
        # 逐个文件处理
        for in_file in file_list:
            file_name = os.path.basename(in_file)
            m = re.match(reg, file_name)
            if m:
                if 'BC' in sat_dtype:
                    ymd = m.group(2)
                    hm = m.group(3)
                    band = m.group(1)
                    segm = m.group(4)
                    if int(segm) > 5:
                        continue

                    # 分时段数据未用，冲突，暂不入库
                    if 'B07' in band:
                        continue

                    band = band_alias[band]
                else:
                    ymd = m.group(1)
                    hm = m.group(2)
                    band = m.group(3)
                    segm = m.group(4)

                # 不在时间段内的数据不处理
                fp_time = datetime.strptime(
                    '%s%s' % (ymd, hm), '%Y%m%d%H%M')
                if date_s > fp_time or fp_time > date_e:
                    continue

                statinfo = os.stat(in_file)
                cc_time = datetime.utcfromtimestamp(statinfo.st_ctime)
                db_time = datetime.utcnow()
                file_size = os.path.getsize(in_file)
                if select_data_info(file_name):
                    continue
                indb_list.append(
                    [data_id, file_name, in_file, fp_time, db_time, cc_time, file_size, band, segm])
    return indb_list

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
#     parser.add_argument("-s", "--sat", help="YYYYMMDD-YYYYMMDD")
#     parser.add_argument("-j", "--job", help="YYYYMMDD-YYYYMMDD")
    parser.add_argument("-t", "--time",  help="history|realtime")
    args = parser.parse_args()
    args_time = args.time
    if 'auto' in args_time:
        date_start = (datetime.utcnow() - relativedelta(minutes=40))
        date_end = datetime.utcnow()
    else:
        date_start, date_end = args_time.split('-')
        date_start = datetime.strptime(date_start, '%Y%m%d%H%M%S')
        date_end = datetime.strptime(date_end, '%Y%m%d%H%M%S')

    date1 = datetime.strptime(date_start.strftime('%Y%m%d'), '%Y%m%d')
    date2 = datetime.strptime(date_end.strftime('%Y%m%d'), '%Y%m%d')

    while date1 <= date2:
        ymd = date1.strftime('%Y%m%d')
        date1 = date1 + relativedelta(days=1)
        indb_list = get_local_file_lst(ymd, date_start, date_end)
        print len(indb_list)
        if len(indb_list) > 0:
            insert_data_info(indb_list)
        else:
            print 'found data nums: %d' % len(indb_list)

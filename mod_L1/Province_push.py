# -*- coding: utf-8 -*-

import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import Pool
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from PB.CSC.pb_csc_console import SocketServer, LogServer
from lib.com_lib_crontrol import get_args, get_date_list, get_job_id_list, get_pair_list
from lib.com_lib_crontrol import run_command, run_command_parallel, get_cmd_list
from lib.com_lib_crontrol import time_block
from PB.pb_io import write_yaml_file, find_file, str_format
import pymysql

class DBUtil:

    def __init__(self):
        '''
        默认连接
        '''
        self.connectDB = self.connector_DB_hz

    def setDBConnector(self, dbConnectorFunc):
        '''
        设定连接DB所用函数
        '''
        self.connectDB = dbConnectorFunc

    def connector_DB_hz(self):
        '''
        建立和数据库系统的连接
        '''
        db = pymysql.connect(host='10.135.30.112', user='hzqx',
                             passwd='PassWord@1234', db='hangZhou', use_unicode=True, charset="utf8")
        return db

    def executeSearch(self, sql):
        '''
        检索DB
        '''
        db = self.connectDB()
        cursor = db.cursor()
        count = 0
        results = ()
        try:
            count = cursor.execute(sql)
            print "count ",count, sql
            if count > 0:
                results = cursor.fetchall()
                print "results", results
        except Exception, e:
            print 'sql:%s, error:%s' % (sql, e)
        finally:
            cursor.close()
            db.close()
        return results

    def executeInsert(self, sql, params=[]):
        '''
        插入DB，一行or多行
        '''
        db = self.connectDB()
        cursor = db.cursor()
        try:
            if len(params) == 0:
                cursor.execute(sql)
            elif len(np.array(params).shape) == 2:
                cursor.executemany(sql, params)
            else:
                return
            db.commit()
        except Exception, e:
            db.rollback()
            print 'sql:%s, error:%s' % (sql, e)
        finally:
            cursor.close()
            db.close()

    def executeUpdate(self, sql):
        '''
        更新DB一条记录
        '''
        db = self.connectDB()
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            db.commit()
        except Exception, e:
            db.rollback()
            print 'sql:%s, error:%s' % (sql, e)
        finally:
            cursor.close()
            db.close()

    def executeDelete(self, sql):
        '''
        删除DB记录
        '''
        self.executeUpdate(sql)

__description__ = u'dm模块调度'
__author__ = 'wangpeng'
__date__ = '2018-10-30'
__version__ = '1.0.0_beat'


# 获取程序所在目录位置
g_path, _ = os.path.split(os.path.realpath(__file__))

os.chdir(g_path)
# 模块的配置文件
g_file_cfg = os.path.join(g_path, 'cfg/L1.cfg')

# 读取模块配置文件内容
g_var_cfg = ConfigObj(g_file_cfg)

# 调度文件中的路径信息
g_path_h8src = g_var_cfg['PATH']['IN']['h8_src']
g_path_h8dat = g_var_cfg['PATH']['IN']['h8_dat']
g_path_h8geo = g_var_cfg['PATH']['IN']['h8_geo']
g_path_h8nc = g_var_cfg['PATH']['MID']['h8_nc']
g_path_h8ang = g_var_cfg['PATH']['MID']['h8_ang']
g_path_h8hdf = g_var_cfg['PATH']['OUT']['h8_hdf']
g_path_h8fig = g_var_cfg['PATH']['OUT']['h8_fig']
g_path_h8png = g_var_cfg['PATH']['OUT']['h8_png']

g_path_interface = g_var_cfg['PATH']['OUT']['interface']

# 初始化调度日志
g_path_log = g_var_cfg['PATH']['OUT']['log']
g_var_log = LogServer(g_path_log)

# 覆盖接口文件标记
g_rewrite_interface = g_var_cfg['CROND']['rewrite_interface'].lower()
g_run_jobs = g_var_cfg['CROND']['run_jobs'].lower()
g_run_mode = g_var_cfg['CROND']['run_mode']

g_db = DBUtil()
g_db.setDBConnector(g_db.connector_DB_hz)

def do_thumb_job(in_file):
    filename = os.path.basename(in_file)
    ymd = filename.split('_')[4]
    hmtime = filename.split('_')[5]
    timestr = ymd+hmtime
    dateTime = datetime.strptime(timestr,'%Y%m%d%H%M')
    dateTime = dateTime + relativedelta(hours=8)
    title_time = datetime.strftime(dateTime, '%Y-%m-%d %H:%M')
    dateTime = datetime.strftime(dateTime, '%Y/%m/%d %H:%M')

    if 'ORIGINAL' in filename:
        if 'Geo_Color' in filename:
            title="卫星原分数据 GeoColor %s(BTC)" % (title_time)
            parms='params_yf_1000m.json'
            data_type='ORIGINAL'
            product_type='Geo_Color'
        elif 'B03' in filename:
            title="卫星原分数据 可见光(3,红光) %s(BTC)" % (title_time)
            parms='params_yf_2000m.json'
            data_type='ORIGINAL'
            product_type='B03'
        elif 'B09' in filename:
            title="卫星原分数据 水汽(9,对流中层) %s(BTC)" % (title_time)
            parms='params_yf_2000m.json'
            data_type='ORIGINAL'
            product_type='B09'
        elif 'B13' in filename:
            title="卫星原分数据 红外(13,长波) %s(BTC)" % (title_time)
            parms='params_yf_2000m.json'
            data_type='ORIGINAL'
            product_type='B13'
    else:
        if 'Geo_Color' in filename:
            title="卫星广播数据 GeoColor %s(BTC)" % (title_time)
            parms='params_gb_1000m.json'
            data_type='BROADCAST'
            product_type='Geo_Color'
        elif 'B03' in filename:
            title="卫星广播数据 可见光(3,红光) %s(BTC)" % (title_time)
            parms='params_gb_4000m.json'
            data_type='BROADCAST'
            product_type='B03'
        elif 'B09' in filename:
            title="卫星广播数据 水汽(9,对流中层) %s(BTC)" % (title_time)
            parms='params_gb_4000m.json'
            data_type='BROADCAST'
            product_type='B09'
        elif 'B13' in filename:
            title="卫星广播数据 红外(13,长波) %s(BTC)" % (title_time)
            parms='params_gb_4000m.json'
            data_type='BROADCAST'
            product_type='B13'
    out_file = in_file.split('.')[0]+'_THUMB.jpg'
    dirname = os.path.dirname(out_file)+"/"
    sqldir = "/home/HZQX" + out_file
    print "!!!!!!!!!!!!!!!!!!!!!!", sqldir
    cmd = "/bin/bash /home/HZQX/Pronvice_Project/image_draw_py27_linux_v2/run_image.sh %s %s \"%s\" %s" % (in_file, out_file, title, parms)
    push_cmd = '/usr/local/bin/wput -q --skip-larger --basename=%s %s ftp://HZQX:HZQX@10.135.30.112:21%s' % (dirname, out_file, dirname)
    thumb_sql_cmd = 'mysql -h"10.135.30.112" -P"3306" -u"hzqx" -p"PassWord@1234" "hangZhou" -e "update m_product_data set thumb_path=\'%s\' where datetime=\'%s\' and data_type=\'%s\' and product_type=\'%s\' "' % (sqldir,dateTime,data_type,product_type) 
    print cmd
    print push_cmd
    print sqldir, thumb_sql_cmd
    os.system(cmd)
    os.system(push_cmd)
    os.system(thumb_sql_cmd)

def get_job_id_func(job_id):
    """
    u 返回jobid对应的函数名称 ，jobid唯一性
    :return:
    """
    job_id_func = {
        "job_0110": job_0110,
        "job_0210": job_0210,
        "job_0310": job_0310,
        "job_0410": job_0410,
        "job_0510": job_0510,
        "job_0610": job_0610,
    }
    return job_id_func.get(job_id)

def main():
    # 获取必要的三个参数(卫星对，作业编号，日期范围 , 端口, 线程)
    sat_pair = sys.argv[1]
    date_s = sys.argv[2]
    push_tifffile_indb(sat_pair, date_s)
    push_hdffile_indb(sat_pair, date_s)
	
def db_insert_product(utime, opath, dtype, ptype):

    tbl_name = 'm_product_data'
    if not db_select_product(utime, dtype, ptype):
        sql = u"""INSERT INTO %s(cutpic_path,data_type,product_type,datetime) VALUES('%s','%s','%s','%s')""" % (
            tbl_name, opath, dtype, ptype, utime)
        g_db.executeInsert(sql)


def db_select_product(sql_cmd):
    res = []
    #sql_cmd = "select id from m_product_data where datetime='%s' and data_type='%s' and product_type='%s'" % (dateTime,data_type,product_type)
    res = g_db.executeSearch(sql_cmd)
    if len(res) > 0:
        return True
	

def push_tifffile_indb(sat_pair, ymd):

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)
    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 输入
    in_path = os.path.join(g_path_h8fig, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})

    # 遍历当天数据
    # Himawari08_AHI_L2_Aerosol_20190516_0750.hdf
    reg = u'HIMAWARI_AHI_(%s)_%s_(\d{8})_(\d{4})_(.*).tiff$' % (
        res_m, sat_dtype)
    file_list = find_file(in_path, reg)

    # 输出
    for in_file in file_list:
        #print in_file
        #HIMAWARI_AHI_1000_ORIGINAL_20190602_0430_Geo_Color.tiff    2019/06/02 05:00
        dirname = os.path.dirname(in_file)+"/"
        filename = os.path.basename(in_file)
        hmtime = filename.split('_')[5]
        timestr = ymd+hmtime
        dateTime = datetime.strptime(timestr,'%Y%m%d%H%M')
        dateTime = dateTime + relativedelta(hours=8)
        dateTime = datetime.strftime(dateTime, '%Y/%m/%d %H:%M')
        tiff_path = dirname
        data_type = filename.split('_')[3]
        if 'Geo_Color' in filename or 'B03' in filename or 'B09' in filename or 'B13' in filename:
            if 'Geo_Color' in filename:
                product_type = 'Geo_Color'
            else:
                product_type = (filename.split('_')[6]).split('.')[0]
            push_cmd = '/usr/local/bin/wput -q --skip-larger --basename=%s %s ftp://HZQX:HZQX@10.135.30.112:21%s' % (dirname, in_file, dirname)
            select_cmd = 'select id from m_product_data where datetime=\'%s\' and data_type=\'%s\' and product_type=\'%s\' and tiff_path is not NULL' % (dateTime,data_type,product_type)
            sql_cmd = 'mysql -h"10.135.30.112" -P"3306" -u"hzqx" -p"PassWord@1234" "hangZhou" -e "insert into m_product_data(datetime,tiff_path,data_type,product_type) values(\'%s\',\'%s\',\'%s\',\'%s\')"' % (dateTime,tiff_path,data_type,product_type) 
            if not db_select_product(select_cmd):
                os.system(push_cmd)
                os.system(sql_cmd)
                infile=in_file.split('.')[0]+".jpg"
                do_thumb_job(infile)
            else:
                print filename, "already indb,so exist"
        else:
            print 'don\'t  push'
			
			
			

def push_hdffile_indb(sat_pair, ymd):

    # 解析配置文件
    res = float(g_var_cfg['PAIRS'][sat_pair]['res'])
    res_m = int(res * 100 * 1000)
    # 从作业名上获取卫星数据类型
    sat_dtype = sat_pair.split('_')[-1]

    # 输入
    in_path = os.path.join(g_path_h8hdf, ymd)
    in_path = str_format(in_path, {'H8_TYPE': sat_dtype})

    # 遍历当天数据
    # Himawari08_AHI_L2_Aerosol_20190516_0750.hdf
    reg = u'AHI8_OBI_%04dM_NOM_(\d{8})_(\d{4}).hdf$' % res_m
    file_list = find_file(in_path, reg)

    # 输出
    for in_file in file_list:
        #print in_file
        #AHI8_OBI_2000M_NOM_20190602_0900.hdf    2019/06/02 05:00
        dirname = os.path.dirname(in_file)+"/"
        filename = os.path.basename(in_file)
        hmtime = (filename.split('_')[5]).split('.')[0]
        timestr = ymd+hmtime
        dateTime = datetime.strptime(timestr,'%Y%m%d%H%M')
        dateTime = dateTime + relativedelta(hours=8)
        dateTime = datetime.strftime(dateTime, '%Y/%m/%d %H:%M')
        tiff_path = dirname
        if '2000M' in filename:
            data_type = 'ORIGINAL'
        else:
            data_type = 'BROADCAST'
        if '2000M' in filename or '4000M' in filename:
            push_cmd = '/usr/local/bin/wput -q --skip-larger --basename=%s %s ftp://HZQX:HZQX@10.135.30.112:21%s' % (dirname, in_file, dirname)
            select_cmd = 'select id from m_hdf_data where hdf_time=\'%s\' and hdf_type=\'%s\'' % (dateTime,data_type)
            sql_cmd = 'mysql -h"10.135.30.112" -P"3306" -u"hzqx" -p"PassWord@1234" "hangZhou" -e "insert into m_hdf_data(hdf_time,hdf_type) values(\'%s\',\'%s\')"' % (dateTime,data_type) 
            if not db_select_product(select_cmd):
                os.system(push_cmd)
                os.system(sql_cmd)
            else:
                print filename, "already indb,so exist"
        else:
            print 'don\'t  push'

if __name__ == '__main__':

    #     with time_block('sssss', g_var_log):
    #         time.sleep(1)

    main()
		
		
		

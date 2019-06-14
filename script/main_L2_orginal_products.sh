#!/bin/bash
#===============================================================
# Name:         Main.sh
# author:       wangpeng
# Type:         Bash Shell Script
# Description:  项目总体调度脚本
#===============================================================
source /HZ_data/HZ_LIB/intel/composer_xe_2015.1.133/bin/compilervars.sh intel64

#配置环境变量
ROOT_DIR=/home/HZQX
export PATH=/HZ_data/HZ_LIB/anaconda2/bin:$PATH
export LD_LIBRARY_PATH=/lib64:/HZ_data/HZ_LIB/lib:$LD_LIBRARY_PATH
export OMP_NUM_THREADS=4
export PYTHONPATH=$ROOT_DIR/Project
export PROJ_LIB=/HZ_data/HZ_LIB/anaconda2/share/proj
export GDAL_DATA=/HZ_data/HZ_LIB/anaconda3/share/gdal
ulimit -s unlimited

# dirname $0 是执行次脚本的位置, cd 后在获取位置才是绝对位置basepath,所以要cd basepath
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`
logName=`echo $bashName |awk -F '.' '{print $1}'`.log
echo `date +"%Y-%m-%d %H:%M:%S"` 'star gsics global crond...' >> $logName

# 模块位置
mod_path=${basepath%/*}/mod_L2
cd $mod_path

if [ $# -eq 0 ];then
    stime=auto
    port=20001
else
    stime=$1
    port=0
fi

# L2产品调度 ASL FOG QPE LST
python2.7 L2_main.py -s HIMAWARI_AHI_L2 -j 0110 -t $stime -p $port --thread 1
# L2 ASL FOG QPE LST 切图
python2.7 L2_main.py -s HIMAWARI_AHI_L2 -j 1010 -t $stime -p $port --thread 1
# L2产品调度 NDIV
python2.7 L2_main.py -s HIMAWARI_AHI_NDVI -j 0210 -t $stime -p $port --thread 1


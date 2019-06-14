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
export GDAL_DATA=/HZ_data/HZ_LIB/anaconda2/share/gdal
ulimit -s unlimited

# dirname $0 是执行次脚本的位置, cd 后在获取位置才是绝对位置basepath,所以要cd basepath
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`
logName=`echo $bashName |awk -F '.' '{print $1}'`.log
echo `date +"%Y-%m-%d %H:%M:%S"` 'star gsics global crond...' >> $logName

# 模块位置
mod_path=${basepath%/*}/mod_L1
cd $mod_path

if [ $# -eq 0 ];then
    stime=auto
    port=10002
else
    stime=$1
    port=0
fi

# 原分500米 调度
python2.7 L1_main.py -s HIMAWARI_AHI_1000_ORIGINAL -j 0210 -t $stime -p $port --thread 4
python2.7 L1_main.py -s HIMAWARI_AHI_1000_ORIGINAL -j 0410 -t $stime -p $port --thread 1
python2.7 L1_main.py -s HIMAWARI_AHI_1000_ORIGINAL -j 0510 -t $stime -p $port --thread 1
python2.7 L1_main.py -s HIMAWARI_AHI_1000_ORIGINAL -j 0610 -t $stime -p $port --thread 4

process_name=`basename $0`
pnums=`ps x |grep -w $process_name |grep -v grep | wc -l`    # grep -v grep 忽略grep本身进程
if [ $pnums -ge 3 ]; then
    echo "too many processes $pnums are running, so exit."
    exit
fi

########### 数据推送 ################
start_time=`date -u -d "1day ago" +%Y%m%d`
end_time=`date -u +%Y%m%d`
while :
do
    if [[ $start_time -gt $end_time ]];then
        break
    fi
    echo python2.7 Province_push.py HIMAWARI_AHI_1000_ORIGINAL $start_time
    python2.7 Province_push.py HIMAWARI_AHI_1000_ORIGINAL $start_time
    wait
    start_time=`date -d "$start_time 1day" +%Y%m%d`
done


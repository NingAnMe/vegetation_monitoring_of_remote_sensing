#!/bin/bash
#===============================================================
# Name:         Main.sh
# author:       wangpeng
# Type:         Bash Shell Script
# Description:  项目总体调度脚本
#===============================================================

#配置环境变量
ROOT_DIR=/home/kts
export PYTHONPATH=$ROOT_DIR/Project
export PATH=/DATA1/KTS_LIB/anaconda2/bin:$PATH
export PROJ_LIB=/DATA1/KTS_LIB/anaconda2/share/proj

# dirname $0 是执行次脚本的位置, cd 后在获取位置才是绝对位置basepath,所以要cd basepath
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`
logName=`echo $bashName |awk -F '.' '{print $1}'`.log
echo `date +"%Y-%m-%d %H:%M:%S"` 'star gsics global crond...' >> $logName

# 项目位置
#proj_path=${basepath%/*}
if [ $# -eq 0 ];then
    stime=`date -u +"%Y%m%d" -d '1 days ago'`
    etime=`date -u +"%Y%m%d"`
else
    stime=$1
    etime=$2
fi
# 1.1 开始下载葵花数据
# 14060A_01_01  从日本获取
# 14060A_01_02  从星地通获取
python $ROOT_DIR/Project/OM/hz/mod_com/com_main.py -s HIMAWARI+AHI -j 0110 -t ${stime}-${etime}

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

order_path=/DATA2/KTS/CMA_KTS/CommonData/order/14060A_01_01
stime=$1
etime=$2

while :
do
    # 把第一个时间格式成yyyymmdd hhmm
    stime=$(date -d "$stime"  +'%Y%m%d %H%M')
    key_name=${stime:0:8}_${stime:9:4} 
    nums=`cat ${order_path}/${stime:0:8}.txt |grep $key_name | wc -l`
    if [[ $nums -lt 160 ]];then
        echo $key_name $nums
    fi

    stime=$(date -d "$stime 10 minute"  +'%Y%m%d %H%M')
    if [[ ${stime:0:8} -gt ${etime:0:8} ]]; then
        break;
    fi
done

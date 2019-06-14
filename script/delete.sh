#!/bin/bash
#===============================================================
# Name:         delete.sh
# author:       wangpeng
# Type:         Bash Shell Script
# Description:  管理数据生命周期
#===============================================================

if [ $# -eq 0 ];then
    stime=`date -u +"%Y%m%d" -d '1 days ago'`
    etime=`date -u +"%Y%m%d"`
else
    stime=$1
    etime=$2
fi

# dirname $0 是执行次脚本的位置, cd 后在获取位置才是绝对位置basepath,所以要cd basepath
basepath=$(cd `dirname $0`; pwd)
cd $basepath
bashName=`basename $0`
logName=`echo $bashName |awk -F '.' '{print $1}'`.log
cfgName=`echo $bashName |awk -F '.' '{print $1}'`.cfg
echo `date +"%Y-%m-%d %H:%M:%S"` 'star crond...' >> $logName

while :
do  
    ymd=$stime
    
    while read line
    do
        # 跳过#开头行和空行
        if echo "$line"|grep -q -E "^$|^#"; then
            continue
        fi
        ipath=`echo ${line} | awk '{print $1}'`
        args1=`echo ${line} | awk '{print $2}'`
        args2=`echo ${line} | awk '{print $3}'`
        # 推送当前目录下文件
        
        in_path=`echo ${ipath} |sed "s#YYYY#${ymd:0:4}#g" |sed "s#MM#${ymd:4:2}#g" |sed "s#DD#${ymd:6:2}#g"`
        
        echo $in_path
        echo find $in_path -type f $args1 $args2
        find $in_path -type f $args1 $args2
        find $in_path -type f $args1 $args2 | xargs rm -rf
        find $in_path -type d $args1 $args2 | xargs rm -rf
    done < $cfgName

    stime=$(date -d "$stime 1day"  +%Y%m%d)
    if [[ $stime -gt $etime ]]; then
        break;
    fi
done

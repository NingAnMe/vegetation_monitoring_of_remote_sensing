#!/bin/bash

pyexe=$1
ps -aef|grep $pyexe | grep -v grep | awk '{print $2}'| while read line
do
    echo $line
    kill -9 $line
done


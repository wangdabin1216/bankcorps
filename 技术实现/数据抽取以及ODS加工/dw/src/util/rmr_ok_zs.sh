#!/bin/bash
#传入日期参数：yyyy-MM-dd
#脚本名称：run_zs_inc.sh
#脚本功能：整体运行脚本
#输入参数: yyyy-MM-dd eg: sh run_zh_inc.sh 2020-02-19
#编写人  ：dk

#判断传参个数
if [ $# -ne 1 ];then
  echo "Usage:$0 date"
  exit -1
fi

etldt=$1

#基础路径
etl_home=/etl/dw

currdt=`$etl_home/src/util/get_date.sh $etldt`

#配置文件路径
conf_path=${etl_home}/conf
#配置文件名称
configfile=${conf_path}/zs.conf
#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#日志文件路径
log_path=${etl_home}/log/load/${sys}/${currdt}

zs_path=$(cat ${configfile} | grep -i '^zs_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
controlpath=${zs_path}/controlpath
xlsxpath=${zs_path}/xlsx
csvpath=${zs_path}/csv


echo echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $xlsxpath/* $csvpath/* ${zs_path}/*.ok" >> $log_path/run_${currdt}.log
rm -rf $xlsxpath/*
rm -rf $csvpath/*
rm -rf ${zs_path}/*.ok

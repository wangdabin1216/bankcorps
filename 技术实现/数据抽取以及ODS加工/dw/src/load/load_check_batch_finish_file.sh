#/bin/bash 
#脚本名称：load_check_batch_finish_file.sh
#脚本功能：检测核心系统批量任务表当日批量是否完成
#输入参数：日期 eg: sh load_check_batch_finish_file.sh sys 20190228
#编写人    ：guxn
#编写日期：20200311
#修改记录：
#by guxn 20200311  1. 创建脚本

#程序入口
if [ $# -ne 2 ]; then 
 echo " Usage : 系统 日期"
 exit 1
fi

#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/load

#系统
typeset -l sys
sys=$1
#日期
currdt=$2
currdt=`$etl_home/dw/src/util/get_date.sh $currdt`

#日志文件的路径创建
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日期为:$currdt"> $log_path/etl_load_check_batch_finish_file_${sys}.log
###################相关参数程序###################
#判断核心系统日终是否完成
if [ -e $log_path/${sys}.ok ];then
	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${sys}系统日终完成">> $log_path/etl_load_check_batch_finish_file_${sys}.log
    exit 0
else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${sys}系统日终未完成">>$log_path/etl_load_check_batch_finish_file_${sys}.log
    exit 200
fi

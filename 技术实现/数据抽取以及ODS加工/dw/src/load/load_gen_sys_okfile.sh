#!/bin/bash
#############################################################
#脚本名称:load_gen_sys_okfile.sh
#脚本功能:检查系统是否满足加载条件
#脚本参数:系统
#脚本运行:sh load_gen_sys_okfile.sh cb
#编写人:gxn
#编写时间:20200319
#功能实现:
###########################################################
if [ $# -ne 1 ];then
  echo "Usage:$0 system"
   exit -1
fi

#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/load

#系统
typeset -l sys
sys=$1

#日期
mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_DISP_CMD)'`
#Oracle的连接字符串，其中包含了Oracle的地址，SID，和端口号
connect_str=`$mysql_conn_str "select distinct disp_dt from disp_job where upper(job_name) like upper('${sys}_LOAD_CHECK_OK_FILE');"`
#oracle数据库
currdt=`echo $connect_str|awk -F" " '{print $1}'`
currdt=`$etl_home/dw/src/util/get_date.sh $currdt`

#日志文件的路径创建
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日期为：$currdt"> $log_path/load_gen_sys_okfile_${sys}.log

#输出日志
echo "$currdt">$log_path/${sys}.ok
if [ $? -eq 0 ];then
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$sys 系统 信号文件生成成功" >> $log_path/load_gen_sys_okfile_${sys}.log
else
	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]$sys 系统 信号文件生成失败">> $log_path/load_gen_sys_okfile_${sys}.log
	exit 1
fi





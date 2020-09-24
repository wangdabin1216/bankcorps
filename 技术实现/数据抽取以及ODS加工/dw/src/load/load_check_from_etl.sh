#!/bin/bash
#############################################################
#脚本名称:load_check_from_etl.sh
#脚本功能:检查系统是否满足加载条件
#脚本参数:系统
#脚本运行:sh load_check_from_etl.sh rm
#编写人:gxn
#编写时间:20200319
#功能实现:
###########################################################
if [ $# -ne 1 ];then
  echo "Usage:$0 system"
   exit -1
fi


#脚本路径
base_path=/data/bigdata
#shell路径
sh_path=${base_path}/script
#日志文件路径
log_path=${base_path}/log

#系统简称
#sys=${v_tgt_table%%_*}
typeset -l sys
sys=$1

#检查系统日终是否完成
while [ 1 == 1 ]
   do
     #判断系统是否传输完成
	 if [ -e ${base_path}/${sys}.ok ];then
       
		#日期
		date=`cat $base_path/${sys}.ok`
		#设置日志文件路径
		log_path=${log_path}/${date}
		if [ ! -e ${log_path} ];then
		 mkdir -p ${log_path}
		fi
	   
		#调用FTP脚本传输文件
		sh $sh_path/dsa_file_ftp_totar.sh $sys $date
		#输出日志
	    echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO] 系统OK文件存在调用dsa_file_ftp_totar.sh进行文件传输">${log_path}/${sys}.log
	    exit 0
	 else
	    sleep 300
	    echo `date "+%Y-%m-%d %H:%M:%S"` "[ERROR] 系统OK文件不存在：${base_path}/${sys}.ok，请等待">>${log_path}/${sys}.log
	fi
done
exit 0

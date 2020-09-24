#!/bin/bash
#############################################################
#脚本名称:load_sqlldr_data.sh
#脚本功能:将数据文件加载到Oracle数据中
#脚本参数:系统 表名 日期
#脚本运行:sh load_sqlldr_data.sh rm cb_kfxp_whmmzh 20190601
#编写人:gxn
#编写时间:20200305
#功能实现:
###########################################################
if [ $# -ne 3 ];then
  echo "Usage:$0 system tablename date"
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

#输入参数,目标表名称
typeset -l v_tgt_table
v_tgt_table=$2

#传入日期
currdt=$3
#日期转换
date=`${sh_path}/get_date.sh ${currdt}`

#设置日志文件路径
log_path=${log_path}/${date}
if [ ! -e ${log_path} ];then
 mkdir -p ${log_path}
fi

#数据文件路径
data_path_tmp=${base_path}/tmp
if [ ! -e ${data_path_tmp} ];then
 mkdir -p ${data_path_tmp}
fi

#配置文件路径
conf_path=${base_path}/conf/ctl
if [ ! -e ${conf_path} ];then
 mkdir -p ${conf_path}
fi

#数据库配置文件
db_conf_file=${base_path}/conf/sqlldr_db.conf
if [ -e $db_conf_file ];then
  #oracle用户名
  db_user=`cat $db_conf_file | grep "neutron" | awk -F"=" '{print $2}' | awk -F"," '{print $1}' `
  #oracle密码
  db_password=`cat $db_conf_file | grep "neutron" | awk -F"=" '{print $2}' | awk -F"," '{print $2}' `
  #tns
  db_name=`cat $db_conf_file | grep "neutron" | awk -F"=" '{print $2}' | awk -F"," '{print $3}' `
  #ip
  db_ip=`cat $db_conf_file | grep "neutron" | awk -F"=" '{print $2}' | awk -F"," '{print $4}' `
else
 echo "$db_conf_file 文件不存在">${log_path}/${v_tgt_table}.log
 exit -1
fi

 #日志输出
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]表名：${v_tgt_table}">${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]系统：${sys}">>${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]数据日期：${date}">>${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]日志文件：${log_path}">>${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]数据文件路径：${data_path_tmp}">>${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]表配置文件：${conf_path}/${v_tgt_table}.ctl">>${log_path}/${v_tgt_table}.log
   echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]数据库连接：${db_user}/${db_password}@${db_ip}/${db_name}">>${log_path}/${v_tgt_table}.log

source /home/oracle/.bash_profile
sqlldr ${db_user}/${db_password}@${db_ip}/${db_name} control=${conf_path}/${v_tgt_table}.ctl log=${log_path}/sqlldr_${v_tgt_table}.log skip=0 load=200000000 errors=100 rows=1000  bindsize=33554432

#判断加载行数
file_num=`wc -l ${data_path_tmp}/${v_tgt_table}.txt |awk -F" " '{print $1}'`
load_num=`cat ${log_path}/sqlldr_${v_tgt_table}.log | grep "successfully" | awk -F" " '{print $1}'`
#判断文件数量和日志中的load数量
   if [ ${file_num}"x" == ${load_num}"x" ];then
     echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]导入表${v_tgt_table}成功,行数为${load_num}">>${log_path}/${v_tgt_table}.log
   else
     echo `date "+%Y-%m-%d %H:%M:%S"` "[ERROR]导入表${v_tgt_table}失败,数据库行数与文件行数不相等:${file_num} <>${load_num}">>${log_path}/${v_tgt_table}.log
	 #将导入报错的表写入sqlldr_error.txt
	 echo "${v_tgt_table}失败,数据库行数与文件行数不相等:${file_num} <>${load_num}" >>${log_path}/sqlldr_error.txt
     exit -1
   fi

exit 0

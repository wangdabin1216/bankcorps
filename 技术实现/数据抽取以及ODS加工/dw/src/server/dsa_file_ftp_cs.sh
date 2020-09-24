#!/usr/bin/env bash
#/bin/bash
################################################################################
#脚本名称：dsa_file_ftp_tar.sh 
#脚本功能：对外供数的FTP传输脚本
#输入参数：供数系统 供数日期 eg: sh dsa_file_ftp_tar.sh kn 20190228
#编写人    ：guxn
#编写日期：2020-03-10
#修改记录：

#检查脚本参数个数
if [ $# -ne 2 ];then
  echo "Usage:$0 system date"
  exit -1
fi

#变量赋值
#ETL路径
etl_home=/Users/wangdabin1216/git/
#基础路径
base_path=$etl_home/dw
#日志文件路径
log_path=$base_path/log/server
#文件目录
file_path=/data/dsa/ext
#脚本所在路径
script_path=$base_path/src/server

#接收传入的系统名
typeset -l system_flag
system_flag=$1

#对传入的日期进行转换
currdt=$2
dsa_date=`$base_path/src/util/get_date.sh $currdt`

#设置日志文件的路径
log_path=$log_path/$dsa_date
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir $log_path
fi

#变量输出到日志文件
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/dsa_ftp_${system_flag}.log
#获取mysql数据库的连接串
 mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_ULD_CMD)'`
#获取FTP服务器连接信息
t_ip=`$mysql_conn_str "select trim(para_val) from etl_meta_args where parameter='dsa.ftp.tar.ip'"`
t_user=`$mysql_conn_str "select trim(para_val) from etl_meta_args where parameter='dsa.ftp.tar.user'"`
t_pwd=`$mysql_conn_str "select trim(para_val) from etl_meta_args where parameter='dsa.ftp.tar.pwd'"`
t_spath=`$mysql_conn_str "select trim(para_val) from etl_meta_args where parameter='dsa.ftp.tar.spath'"`
t_tpath=`$mysql_conn_str "select trim(para_val) from etl_meta_args where parameter='dsa.ftp.tar.tpath'"`
if [ "$t_ip"x != "x" ];then
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 读取数据库配置信息成功" >> $log_path/dsa_ftp_${system_flag}.log
else
 echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 读取数据库配置信息失败 ">> $log_path/dsa_ftp_${system_flag}.log
 exit 1
fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] The file conf is t_ip->$t_ip t_user->$t_user t_pwd->$t_pwd t_tpath->$t_tpath ">> $log_path/dsa_ftp_${system_flag}.log

echo `date +"%Y-%m-%d %H:%M:%S"`"[START] 压缩文件开始!" >> $log_path/dsa_ftp_${system_flag}.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_path/$system_flag/$dsa_date" >> $log_path/dsa_ftp_${system_flag}.log
#tar -zcvf 20200102.tar.gz 20200102
#tar -zxvf 20200102.tar.gz 
cd $file_path/$system_flag
tar -zcvf $dsa_date.tar.gz $dsa_date >> $log_path/dsa_ftp_${system_flag}.log

echo `date +"%Y-%m-%d %H:%M:%S"`"[START] SFTP传输文件开始!" >> $log_path/dsa_ftp_${system_flag}.log
expect $script_path/dsa_file_expect.sh $t_user $t_pwd $t_ip $file_path/$system_flag $system_flag $dsa_date $t_tpath >> $log_path/dsa_ftp_${system_flag}.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[START] SFTP传输文件结束!" >> $log_path/dsa_ftp_${system_flag}.log

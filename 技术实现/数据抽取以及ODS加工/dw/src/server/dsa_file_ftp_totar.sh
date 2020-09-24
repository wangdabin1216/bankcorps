#/bin/bash
################################################################################
#脚本名称：dsa_file_ftp_totar.sh 
#脚本功能：对外供数的FTP传输脚本
#输入参数：供数系统 供数日期 eg: sh dsa_file_ftp_totar.sh rm 20200319
#编写人    ：guxn
#编写日期：20200319
#修改记录：

#检查脚本参数个数
if [ $# -ne 2 ];then
  echo "Usage:$0 system date"
  exit -1
fi

#变量赋值
#ETL路径
#etl_home=/etl
#基础路径
base_path=/data/bigdata
#日志文件路径
log_path=$base_path/log
#脚本所在路径
script_path=$base_path/script

#接收传入的系统名
typeset -l system_flag
system_flag=$1

dsa_date=$2

#设置日志文件的路径
log_path=$log_path/$dsa_date
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir $log_path
fi

#变量输出到日志文件
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/dsa_ftp_${system_flag}.log
#获取FTP服务器连接信息
t_ip=10.4.11.110
t_user=root
t_pwd=root123
t_spath=/data/bigdata
t_tpath=/data/bigdata
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] The file conf is t_ip->$t_ip t_user->$t_user t_pwd->$t_pwd t_spath->$t_spath t_tpath->$t_tpath ">> $log_path/dsa_ftp_${system_flag}.log

echo `date +"%Y-%m-%d %H:%M:%S"`"[START] SFTP传输文件开始!" >> $log_path/dsa_ftp_${system_flag}.log
expect $script_path/dsa_file_expect.sh $t_user $t_pwd $t_ip $t_spath $system_flag $dsa_date $t_tpath >> $log_path/dsa_ftp_${system_flag}.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[START] SFTP传输文件结束!" >> $log_path/dsa_ftp_${system_flag}.log
#删除tar包
if [ -e $t_tpath/*.tar.gz ];then
 rm -rf $t_tpath/*.tar.gz
fi

#删除ok文件
if [ -e $t_tpath/*.ok ];then
 rm -rf $t_tpath/*.ok
fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 删除$t_tpath/tar.gz和OK文件!" >> $log_path/dsa_ftp_${system_flag}.log
exit 0


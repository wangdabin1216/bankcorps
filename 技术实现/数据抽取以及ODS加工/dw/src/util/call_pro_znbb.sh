#/bin/bash 
#脚本名称：call_pro.sh
#脚本功能：执行存储过程
#输入参数：数据库名称 存储过程名称 日期 eg: sh load_unzip.sh omi pro_mid 2019-09-01
#编写人    ：dk
#编写日期：20190918
#修改记录：

#判断传参个数
if [ $# -ne 3 ];then
  echo "Usage:$1 schema $2 pro_name $3 date"
  exit -1
fi

#获取参数
SCHEMA=$1
PRO_NAME=$2
ETLDATE=$3

#基础路径
etl_home=/etl/dw

#将日期转化为yyyyMMdd格式
currdt=`$etl_home/src/util/get_date.sh $ETLDATE`

#配置文件路径
conf_path=${etl_home}/conf
#日志文件路径
log_path=${etl_home}/log/load/${currdt}
#配置文件名称
configfile=${conf_path}/znbb.conf

#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

HIVE_JDBC=$(cat ${configfile} | grep -i '^HIVE_JDBC' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_USER=$(cat ${configfile} | grep -i '^HIVE_USER' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_PASSWORD=$(cat ${configfile} | grep -i '^HIVE_PASSWORD' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')

#调用存储过程
CALL_PROCEDURE="BEGIN
"$SCHEMA".$PRO_NAME('"$currdt"');
END;"

source /etc/profile
#kerberos切换到dw用户
kinit dw -kt ${conf_path}/dw.keytab

#执行存储过程
beeline -u ${HIVE_JDBC}/${SCHEMA} -n ${HIVE_USER} -p ${HIVE_PASSWORD} -e "${CALL_PROCEDURE}" >> ${log_path}/${PRO_NAME}_${currdt}.log

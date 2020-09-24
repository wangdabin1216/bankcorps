#/bin/bash 
#脚本名称：call_pro.sh
#脚本功能：执行存储过程
#输入参数：数据库名称 存储过程名称 日期 eg: sh load_unzip.sh omi pro_mid 2019-09-01
#编写人    ：dk
#编写日期：20190918
#修改记录：

#判断传参个数
if [ $# -ne 4 ];then
  echo "Usage:$1 schema $2 pkg_name $3 pro_name $4 date"
  exit -1
fi

#获取参数
typeset -u SCHEMA
typeset -u PKG_NAME
typeset -u TAB_NAME

SCHEMA=$1
PKG_NAME=$2
TAB_NAME=$3
ETLDATE=$4

PRO_NAME="pro_data_$TAB_NAME"


#基础路径
etl_home=/etl/dw

#将日期转化为yyyyMMdd格式
currdt=`$etl_home/src/util/get_date.sh $ETLDATE`

#配置文件路径
conf_path=${etl_home}/conf
#配置文件名称
configfile=${conf_path}/zs.conf
#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#日志文件路径
log_path=${etl_home}/log/load/${sys}/${currdt}

#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

HIVE_JDBC=$(cat ${configfile} | grep -i '^HIVE_JDBC' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_USER=$(cat ${configfile} | grep -i '^HIVE_USER' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_PASSWORD=$(cat ${configfile} | grep -i '^HIVE_PASSWORD' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
zs_path=$(cat ${configfile} | grep -i '^zs_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')

#调用存储过程
CALL_PROCEDURE="BEGIN
"$SCHEMA"."$PKG_NAME".$PRO_NAME('"$currdt"');
END;"


if [ ! -e ${zs_path}/put_${currdt}.ok ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]没有${zs_path}/put_${currdt}.ok 跳过${SCHEMA}.${PRO_NAME}" >> $log_path/run_${currdt}.log
    echo "[INFO]没有${zs_path}/put_${currdt}.ok 跳过${SCHEMA}.{$PKG_NAME}.${PRO_NAME}"

else
source /etc/profile
#kerberos切换到dw用户
kinit dw -kt ${conf_path}/dw.keytab
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]执行${SCHEMA}.${PKG_NAME}.${PRO_NAME}" >> $log_path/run_${currdt}.log
#执行存储过程
beeline -u ${HIVE_JDBC}/${SCHEMA} -n ${HIVE_USER} -p ${HIVE_PASSWORD} -e "${CALL_PROCEDURE}" >> ${log_path}/${PRO_NAME}_${currdt}.log
fi

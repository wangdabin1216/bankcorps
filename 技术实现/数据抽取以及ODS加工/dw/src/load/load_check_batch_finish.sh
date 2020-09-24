#/bin/bash 
#脚本名称：load_check_batch_finish.sh
#脚本功能：检测核心系统批量任务表当日批量是否完成
#输入参数：日期 eg: sh load_check_batch_finish.sh sys 20190228
#编写人    ：guxn
#编写日期：20200311
#修改记录：
#by guxn 20200311  1. 创建脚本

#****************************************************
#函数名：CheckTable
#功能描述：检查核心批次是否完成
#入口参数：日期(YYYYMMDD)
#返回说明：0 未完成  1 完成
#查询结果为1表示可以抽数。
#****************************************************
CheckTable()
{
	currdt=${1}
	P_DB_CONNECT="${2}"
        schema=${3}
	szResult=0
    szResult=`sqlplus -silent $P_DB_CONNECT<<!
    SET SERVEROUTPUT ON;
    set ECHO OFF;
    set TERM OFF;
    set FEEDBACK OFF;
    set heading off;
    select trim(nvl(count(*), 0)) from $schema.p_ods_etl_flag t where to_char(t.current_day,'yyyymmdd')='$currdt';
    exit;
!`
  echo ${szResult}
  return 0
}

#####################################################
#程序入口
if [ $# -ne 2 ]; then 
 echo " Usage : 系统 日期"
 exit 1
fi

#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/load
#日期
currdt=$2
currdt=`$etl_home/dw/src/util/get_date.sh $currdt`
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日期为：$currdt"> $log_path/etl_load_check_batch_finish.log
#系统
sys=$1

#1.#获取mysql数据库的连接串读取etl_load_system
mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_ULD_CMD)'`
#Oracle的连接字符串，其中包含了Oracle的地址，SID，和端口号
connect_str=`$mysql_conn_str "select trim(db_name),trim(user),trim(password),trim(ip_addr),trim(port),trim(schema_name) from etl_load_system where lower(sys)=lower('$sys');"`
#oracle数据库
oracle_db=`echo $connect_str|awk -F" " '{print $1}'`
#使用的用户名
oracle_name=`echo $connect_str|awk -F" " '{print $2}'`
#oracle 密码
oracle_password=`echo $connect_str|awk -F" " '{print $3}'`
#oracleIP
oracle_ip=`echo $connect_str|awk -F" " '{print $4}'`
#oracle port
oracle_port=`echo $connect_str|awk -F" " '{print $5}'`
schema_name=`echo $connect_str|awk -F" " '{print $6}'`
#oracle连接字符串"$oracle_name/$oracle_name@$oracle_ip:$oracle_port/$oracle_db"
connect_url="$oracle_name/$oracle_password@$oracle_ip:$oracle_port/$oracle_db"
#判断读取参数是否成功
 if [ "$oracle_db"x = "x" -o "$oracle_name"x = "x" -o "$oracle_password"x = "x" -o "$oracle_ip"x = "x" -o "$oracle_port"x = "x" -o "$schema_name"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数connect_str错误">> $log_path/etl_load_check_batch_finish.log
   exit 1
 fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]数据库连接串为：$connect_url">> $log_path/etl_load_check_batch_finish.log


###################相关参数程序###################
#while [ 1 == 1 ] #检查核心系统日终是否完成
#   do
	   #判断核心系统日终是否完成
	   szStatus=`CheckTable ${currdt} ${connect_url} ${schema_name} `	
	   szStatus=`echo $szStatus |awk '{printf "%s",$1}'`
	   #echo "bbbbbb=$szStatus, ${schema_name}"
	   if [ "${szStatus}" == "1" ];then
	 	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${sys}系统日终完成">> $log_path/etl_load_check_batch_finish.log
           exit 0
	   else
	       echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${sys}系统日终未完成,请等待">>$log_path/etl_load_check_batch_finish.log
	       exit 200
	   fi
#done

#exit 0

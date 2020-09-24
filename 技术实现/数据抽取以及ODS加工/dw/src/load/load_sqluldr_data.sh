#!/bin/bash
#脚本名称：load_sqluldr_data.sh
#脚本功能：对源系统文件进行转换并上传到hdfs路径
#输入参数：表名 日期 eg: sh load_sqluldr_data.sh kn_kdpa_zhbcxx 2019-07-24
#编写人    ：guxn
#编写日期：20191225
#修改记录：
#by guxn 20191225       1.新建

if [ $# -ne 2 ];then
  echo "Usage:$0 tablename date"
  exit -1
fi

#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/load
#文件源地址
source_path=/data/src
# kinit dw用户
kinit dw -kt $etl_home/dw/conf/dw.keytab

#传入参数赋值，oracle的表名
typeset -l tablename
tablename=$1
#echo "tablename: $1"
#获取要加载数据的源系统表名
typeset -u srctablename
srctablename=${tablename#*_}

#获取要加载数据的源系统缩写
typeset -l sys
sys=${tablename%%_*}
#echo "sys:$sys"
#日期参数/etl/dw/src/util
date=$2
currdt=`$etl_home/dw/src/util/get_date.sh $date`

#日志文件的路径创建
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir $log_path
fi

#卸载源数据文件存储路径
source_path=/data/src/$sys/$currdt
if [ ! -e $source_path ];then
  mkdir -p $source_path
fi

#变量输出到日志文件
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/unload_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源文件路径为：$source_path">> $log_path/unload_$tablename$currdt.log

#1.#获取mysql数据库的连接串读取etl_load_system
#mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_ULD_CMD)'`
##Oracle的连接字符串，其中包含了Oracle的地址，SID，和端口号
#connect_str=`$mysql_conn_str "select trim(db_name),trim(user),trim(password),trim(ip_addr),trim(port),trim(schema_name) from etl_load_system where lower(sys)=lower('$sys');"`
##oracle数据库
#oracle_db=`echo $connect_str|awk -F" " '{print $1}'`
##使用的用户名
#oracle_name=`echo $connect_str|awk -F" " '{print $2}'`
##oracle 密码
oracle_password=`echo $connect_str|awk -F" " '{print $3}'`
#oracleIP
oracle_ip=`echo $connect_str|awk -F" " '{print $4}'`
#oracle port
oracle_port=`echo $connect_str|awk -F" " '{print $5}'`
schema_name=`echo $connect_str|awk -F" " '{print $6}'`
#oracle连接字符串
connect_url=jdbc:oracle:thin:@$oracle_ip:$oracle_port:$oracle_db:$schema_name
#判断读取参数是否成功
 if [ "$oracle_db"x = "x" -o "$oracle_name"x = "x" -o "$oracle_password"x = "x" -o "$oracle_ip"x = "x" -o "$oracle_port"x = "x" -o "$schema_name"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数connect_str错误">> $log_path/unload_$tablename$currdt.log
   exit 1
 fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]数据库连接串为：$connect_url">> $log_path/unload_$tablename$currdt.log

#2.sqlite读取etl_load_column
#获取查询字段
query_str=`$mysql_conn_str "set session group_concat_max_len=102400000;select group_concat(col_name) from ( select coalesce(trim(trans_statement),trim(col_name)) col_name from etl_load_column where lower(concat(sys,'_',tab_name))=lower('$tablename') order by col_seq) as t;"`
#判断读取参数是否成功
 if [ "$query_str"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数query_str错误">> $log_path/unload_$tablename$currdt.log
   exit 1
 fi

#3.sqlite读取etl_load_table
#获取查询条件及增全量抽取标识
tab_str=`$mysql_conn_str "select trim(cond),trim(if_flag),trim(tab_type),trim(file_name),trim(col_separator) from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
chain_field=`echo $tab_str|awk -F" " '{print $1}'`
if_flag=`echo $tab_str|awk -F" " '{print $2}'`
tab_type=`echo $tab_str|awk -F" " '{print $3}'`
file_name=`echo $tab_str|awk -F" " '{print $4}'`
col_sep=`echo $tab_str|awk -F" " '{print $5}'`
load_data_file=`echo ${file_name//'$currdt'/$currdt}`
#判断读取参数是否成功
 if [ "$if_flag"x = "x" -o "$tab_type"x = "x"  -o "$file_name"x = "x" -o  "$col_sep"x = "x" -o  "$load_data_file"x = "x"  ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数tab_str错误">> $log_path/unload_$tablename$currdt.log
   exit 1
 fi

echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件名为：$load_data_file">> $log_path/unload_$tablename$currdt.log


#抽取核心表增量条件为>=，其他为=
 if [ "$sys" = "kn" ] && [ "$tab_type" = "ST" ];then
   cond="$chain_field>='$currdt'"
 else
    cond="$chain_field='$currdt'"
 fi

 #重新赋值分隔符
 if  [ "$col_sep"x = "\002"x ] || [ "$col_sep"x = ""x   ] ;then
 	col_sep=""
 fi

#开始进行sqluldr操作
 if [[ ${if_flag}x = "F"x ]];then
    sqluldr2.bin  user=${oracle_name}/${oracle_password}@$oracle_ip:$oracle_port/$oracle_db query="select ${query_str} from $schema_name.${srctablename} " file=${source_path}/${load_data_file}  field="$col_sep"  record=0x0a safe=yes rows=100000 charset=utf8  log=$log_path/sqluldr_$tablename$currdt.log
    echo "sqluldr2.bin  user=${oracle_name}/${oracle_password}@$oracle_ip:$oracle_port/$oracle_db query=\"select ${query_str} from $schema_name.${srctablename} \" file=${source_path}/${load_data_file}  field=\"$col_sep\"  record=0x0a safe=yes rows=100000 charset=utf8" >> $log_path/unload_$tablename$currdt.log
else
    sqluldr2.bin  user=${oracle_name}/${oracle_password}@$oracle_ip:$oracle_port/$oracle_db query="select ${query_str} from $schema_name.${srctablename} where ${cond}" file=${source_path}/${load_data_file}  field="$col_sep"  record=0x0a safe=yes rows=100000 charset=utf8  log=$log_path/sqluldr_$tablename$currdt.log
    echo "sqluldr2.bin  user=${oracle_name}/${oracle_password}@$oracle_ip:$oracle_port/$oracle_db query=\"select ${query_str} from $schema_name.${srctablename} where ${cond}\" file=${source_path}/${load_data_file}  field=\"$col_sep\"  record=0x0a safe=yes rows=100000 charset=utf8" >> $log_path/unload_$tablename$currdt.log
fi

#sqoop脚本执行是否成功
db_num=`cat $log_path/sqluldr_$tablename$currdt.log | grep -i "output" | awk -F"," '{print $1}' | awk -F" " '{print $6}'`
file_num=`wc -l ${source_path}/${load_data_file} |awk -F" " '{print $1}'`
if [ ${db_num}"x" == ${file_num}"x" ];then
  echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO]导出表$schema_name.${srctablename}成功,行数为${db_num}">>$log_path/unload_$tablename$currdt.log
else
  echo `date "+%Y-%m-%d %H:%M:%S"` "[ERROR]导出表$schema_name.${srctablename}失败,数据库行数与文件行数不相等:${db_num} <>${file_num}">>$log_path/unload_$tablename$currdt.log
  exit -1
fi

#调度put脚本进行数据转换和上传hdfs
sh $etl_home/dw/src/load/load_put_file.sh $tablename $date
   if [ $? -eq 0 ];then
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]load_put_file执行成功" >> $log_path/unload_$tablename$currdt.log
    else
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]load_put_file执行失败">> $log_path/unload_$tablename$currdt.log
  	  exit -1
    fi
exit 0

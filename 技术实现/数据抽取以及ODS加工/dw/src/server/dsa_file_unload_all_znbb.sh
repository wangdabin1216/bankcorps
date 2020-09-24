#/bin/bash

#判断传参个数
if [ $# -ne 2 ];then
  echo "Usage:$0 table_name date"
  exit -1
fi

table_name=$1
currdt=$2

#基础路径
etl_home=/etl/dw

currdt=`$etl_home/src/util/get_date.sh $currdt`

#配置文件路径
conf_path=${etl_home}/conf
#日志文件路径
log_path=${etl_home}/log/server/${currdt}
#配置文件名称
configfile=${conf_path}/znbb.conf

#ODS报表文件路径(使用:$dsa_path/$currdt)
dsa_path=$(cat ${configfile} | grep -i '^dsa_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#本地报表文件路径(使用:$file_path/$currdt)
file_path=$(cat ${configfile} | grep -i '^file_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#hdfs报表文件路径(使用:$unld_path/$tablename)
hdfs_path=$(cat ${configfile} | grep -i '^unld_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_JDBC=$(cat ${configfile} | grep -i '^HIVE_JDBC' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_USER=$(cat ${configfile} | grep -i '^HIVE_USER' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
HIVE_PASSWORD=$(cat ${configfile} | grep -i '^HIVE_PASSWORD' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')


dsa_path=$dsa_path/$currdt
file_path=$file_path/$currdt
#创建日志目录
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi
#创建目录
if [ ! -e $dsa_path ];then
  mkdir -p $dsa_path
fi
#创建目录
if [ ! -e $file_path ];then
  mkdir -p $file_path
fi
#创建目录并删除文件
  if [ -e $file_path/${table_name} ];then
     rm -rf $file_path/${table_name}/
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_path/${table_name} 路径存在，删除并创建" >> $log_path/unload_${table_name}.log 
     mkdir -p $file_path/${table_name}/
  else 
     mkdir -p $file_path/${table_name}/
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_path/${table_name} 路径不存在,创建" >> $log_path/unload_${table_name}.log
  fi

source /etc/profile
#kerberos切换到dw用户
kinit dw -kt ${conf_path}/dw.keytab
#判断HDFS路径下是否存在文件
hadoop fs -ls $hdfs_path/${table_name}/ >/dev/null 2>&1
if [ $? -eq 0 ];then
   hadoop fs -rm -r $hdfs_path/${table_name}/ >/dev/null 2>&1
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $hdfs_path/${table_name} 路径下有该表的目录，已经清空，可以卸载数据!" >> $log_path/unload_${table_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $hdfs_path/${table_name} 路径下没有该表的目录，可以卸载数据！" >> $log_path/unload_${table_name}.log
fi

#导出数据
beeline -u ${HIVE_JDBC} -n ${HIVE_USER} -p ${HIVE_PASSWORD} -e "begin set_env('transaction.type','inceptor'); set_env('mapred.reduce.tasks','5');insert overwrite directory '$hdfs_path/$table_name' row format delimited fields terminated by '|' select * from omi.$table_name distribute by rand() end"
if [ $? -eq 0 ];then
     hadoop fs -get $hdfs_path/${table_name}/000000_0 $file_path/${table_name}/000000_0
     hadoop fs -get $hdfs_path/${table_name}/000001_0 $file_path/${table_name}/000001_0
     hadoop fs -get $hdfs_path/${table_name}/000002_0 $file_path/${table_name}/000002_0
     hadoop fs -get $hdfs_path/${table_name}/000003_0 $file_path/${table_name}/000003_0
     hadoop fs -get $hdfs_path/${table_name}/000004_0 $file_path/${table_name}/000004_0
     cat $file_path/${table_name}/000001_0 >> $file_path/${table_name}/000000_0
     cat $file_path/${table_name}/000002_0 >> $file_path/${table_name}/000000_0
     cat $file_path/${table_name}/000003_0 >> $file_path/${table_name}/000000_0
     cat $file_path/${table_name}/000004_0 >> $file_path/${table_name}/000000_0
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 已经将数据都导出到本地!" >> $log_path/unload_${table_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 连接不上数据库或配置表信息有误" >>$log_path/unload_${table_name}.log
  exit 1
fi
#替换空值和分隔符
perl -p -i -e 's/\\N//g;s/\|/\|\+\|/g' $file_path/${table_name}/000000_0
if [ $? -eq 0 ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 已经对文本进行处理!" >> $log_path/unload_${table_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[Error] 对文本处理失败，请检查!" >>$log_path/unload_${table_name}.log
  exit 1
fi
#copy文件到目标路径
cp $file_path/${table_name}/000000_0 $dsa_path/i_bd_${table_name}_${currdt}_000_000.dat
#echo "">$dsa_path/dsa_$currdt.ok


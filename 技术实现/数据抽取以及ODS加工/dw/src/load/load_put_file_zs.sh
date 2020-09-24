#/bin/bash 
#脚本名称：load_put_file.sh
#脚本功能：对源系统文件进行转换并上传到hdfs路径
#输入参数：表名 日期 eg: sh load_put_file.sh mb_bankdept 2019-02-28
#编写人    ：guxn
#编写日期：20190912
#修改记录：
#by guxn 20190912 1.
#by dk 20190926 修改conf中文件名称获取处理
#by dk 20191111 参数获取

#判断传参个数
if [ $# -ne 2 ];then
  echo "Usage:$0 table_name date"
  exit -1
fi

#输入参数获取
#要加载数据文件的表名
typeset -l tablename
tablename=$1

#日期
currdt=$2

#基础路径
etl_home=/etl/dw

currdt=`$etl_home/src/util/get_date.sh $currdt`

#配置文件路径
conf_path=${etl_home}/conf
#配置文件名称
configfile=${conf_path}/zs.conf

#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#卸数目录(使用:$target_path/$sys/$currdt)
target_path=$(cat ${configfile} | grep -i '^target_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
target_path=${target_path}/${sys}
#上传文件路径(处理完成的文件目录,使用:$put_path/$sys/$currdt)
put_path=$(cat ${configfile} | grep -i '^put_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
put_path=${put_path}/${sys}/${currdt}
#ods上传到hdfs路径(使用:$hdfs_path/$tablename)
hdfs_path=$(cat ${configfile} | grep -i '^hdfs_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#日志文件路径
log_path=${etl_home}/log/load/${sys}/${currdt}

#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

#变量输出到日志文件
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]批处理日期:$currdt" >> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]PUT文件名称:$tablename"  >> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]脚本存储基础路径:$etl_home"  >> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]运行系统名:$sys" >> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件路径:$target_path">> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]HDFS存储路径:$hdfs_path"  >> $log_path/etl_$tablename$currdt.log
#如果没有目录就创建
if [ ! -e $target_path ];then
  mkdir -p $target_path
fi

echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]目标文件路径:$put_path" >> $log_path/etl_$tablename$currdt.log
#如果没有目录就创建
if [ ! -e $put_path ];then
  mkdir -p $put_path
fi

#删除目标目录下的文件
if [ -e $put_path/$tablename.txt ];then
  rm -rf $put_path/$tablename.txt
  echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $put_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
fi

#获取源系统表数据文件
file_name_tmp=`cat $conf_path/table_zs.conf | grep "\<${tablename}\>" | awk -F ',' '{print $1}' `
load_data_file=`echo ${file_name_tmp//'${currdt}'/$currdt}`
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件名为：$load_data_file">> $log_path/etl_$tablename$currdt.log
mv $target_path/$load_data_file  $put_path/$tablename.txt
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]mv $target_path/$load_data_file  $put_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log


#如果文本文件存在则执行文件行数检查、大小校验、MD5校验、字符集转换
ls $put_path | grep $tablename.txt >> $log_path/etl_$tablename$currdt.log  2>&1 
if [ $? -eq 0 ];then

#空值类型转换\N,字段分隔符为|+|时
 #perl -p -i -e 's/\|\+\|\|\+\|/\|\+\|\\N\|\+\|/g;s/\|\+\|\|\+\|/\|\+\|\\N\|\+\|/g'  $put_path/$tablename.txt >/dev/null 2>&1
 perl -p -i -e 's/\|\+\|\|\+\|/\|\+\|\\N\|\+\|/g;s/\|\+\|\|\+\|/\|\+\|\\N\|\+\|/g;s/^\|\+\|/\\N\|\+\|/g;s/\|\+\|$/\|\+\|\\N/g;' $put_path/$tablename.txt >/dev/null 2>&1
 if [ $? -eq 0 ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
 else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  exit -1
 fi
  echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/\|\|/\|\\N\|/g;s/\|\|/\|\\N\|/g' $put_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log


source /etc/profile
#kerberos切换到dw用户
kinit dw -kt ${conf_path}/dw.keytab

#检查模块
hdfs_path=$hdfs_path/$tablename
#检查表文件路径是否存在
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]检查HDFS路径是否存在" >> $log_path/etl_$tablename$currdt.log  
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]hdfs dfs -ls $hdfs_path" >> $log_path/etl_$tablename$currdt.log
hdfs dfs -ls $hdfs_path   >> $log_path/etl_$tablename$currdt.log 2>&1
if [ $? -eq 0 ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$hdfs_path路径存在" >> $log_path/etl_$tablename$currdt.log
else
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]$hdfs_path路径不存在"  >> $log_path/etl_$tablename$currdt.log
   hdfs dfs -mkdir -p $hdfs_path
   echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]hdfs dfs -mkdir -p $hdfs_path" >> $log_path/etl_$tablename$currdt.log
fi

#加载模块 
#检查表文件是否存在

#判断是否包含特殊字符,将替换成空
  num=`cat $put_path/$tablename.txt | grep '' | wc -l `
  if [ "$num" -ne 0 ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]sed -i 's/^M//g' $put_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
   sed -i 's///g' $put_path/$tablename.txt
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]数据文件替换^M成功" >> $log_path/etl_$tablename$currdt.log
  fi
  
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]检查$hdfs_path路径下文件是否存在">> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]hdfs dfs -ls $hdfs_path/*"  >> $log_path/etl_$tablename$currdt.log
hdfs dfs -ls $hdfs_path/* >> $log_path/etl_$tablename$currdt.log 2>&1

if [ $? -eq 0 ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$hdfs_path路径下存在文件" >> $log_path/etl_$tablename$currdt.log
#删除原有数据文件
   echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]hdfs dfs -rm hdfs_path/*" >> $log_path/etl_$tablename$currdt.log   
   hdfs dfs -rm $hdfs_path/* >> $log_path/etl_$tablename$currdt.log 2>&1
   if [ $? -eq 0 ];then 
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$hdfs_path路径下旧文件删除成功" >> $log_path/etl_$tablename$currdt.log
   else
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]$hdfs_path路径下旧文件删除错误">> $log_path/etl_$tablename$currdt.log
   exit -1
  fi 
else
   echo `date +"%Y-%m-%d %H:%M:%S"`"[WARN]$hdfs_path路径下不存在文件"  >> $log_path/etl_$tablename$currdt.log
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]使用Put上传文件" >> $log_path/etl_$tablename$currdt.log
fi 
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$hdfs_path路径检查文件完成"  >> $log_path/etl_$tablename$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]开始使用PUT上传文件"  >> $log_path/etl_$tablename$currdt.log

#加载当日数据文件
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE] hdfs dfs -put $put_path/$tablename.txt $hdfs_path/$tablename.txt" >>$log_path/etl_$tablename$currdt.log
hdfs dfs -put $put_path/$tablename.txt $hdfs_path/$tablename.txt >> $log_path/etl_$tablename$currdt.log 2>&1
if [ $? -eq 0 ];then
    hdfs dfs -chmod 644 $hdfs_path/$tablename.txt >> $log_path/etl_$tablename$currdt.log 2>&1
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]PUT上传文件成功" >> $log_path/etl_$tablename$currdt.log
else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]PUT上传文件失败" >> $log_path/etl_$tablename$currdt.log
    exit -1
fi
fi
exit 0

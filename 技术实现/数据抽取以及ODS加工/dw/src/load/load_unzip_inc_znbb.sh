#/bin/bash 
#脚本名称：load_unzip.sh
#脚本功能：对源系统文件进行转换并上传到hdfs路径
#输入参数：表名 日期 eg: sh load_unzip.sh 2019-02-28
#编写人    ：guxn
#编写日期：20190912
#修改记录：
#by guxn 20190912 1.
#by dk 20190926

#判断传参个数
if [ $# -ne 1 ];then
  echo "Usage:$0 date"
  exit -1
fi

source /etc/profile

#日期
currdt=$1

#基础路径
etl_home=/etl/dw

currdt=`$etl_home/src/util/get_date.sh ${currdt}`

#配置文件路径
conf_path=${etl_home}/conf
#日志文件路径
log_path=${etl_home}/log/load/${currdt}
#配置文件名称
configfile=${conf_path}/znbb.conf

#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#ods源文件目录(使用:$source_path/$sys/$currdt)
source_path=$(cat ${configfile} | grep -i '^source_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
source_path=${source_path}/${sys}/${currdt}
#卸数目录(使用:$target_path/$sys/$currdt)
target_path=$(cat ${configfile} | grep -i '^target_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
target_path=${target_path}/${sys}/${currdt}
#如果没有日志目录就创建
if [ ! -e ${log_path} ];then
  mkdir -p ${log_path}
fi
#删除卸数目录
rm -rf ${target_path}
#重新创建目录
if [ ! -e ${target_path} ];then
  mkdir -p ${target_path}
fi

#while [ 1 == 1 ]
#do
#从ods拷贝当天的文件
if [ ! -e ${source_path}/${sys}_${currdt}.ok ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]ODS没有此目录或OK文件,${source_path}/${sys}_${currdt}.ok" >> ${log_path}/etl_${currdt}.log
  #sleep 300
  exit 200
else 
 cp ${source_path}/${sys}_${currdt}.tar ${target_path}
 source /etc/profile
 #kerberos切换到dw用户
 kinit dw -kt ${conf_path}/dw.keytab
 hdfs dfs -put ${target_path}/${sys}_${currdt}.tar /dw/backup/${sys}_${currdt}.tar
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]OK文件存在,${source_path}/${sys}_${currdt}.ok" >> ${log_path}/etl_${currdt}.log
 cd ${target_path}
 tar -xvf ${target_path}/${sys}_${currdt}.tar
 cat ${conf_path}/table.conf | while read line
 do
 	file_name_tmp=`echo ${line} | awk -F, '{print $1}'`
        file_name=`echo ${file_name_tmp//'${currdt}'/$currdt}.z`
        unzip ${file_name}
	if [ $? -eq 0 ];then
		echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]解压${file_name}完成" >> ${log_path}/etl_${currdt}.log
	else
		echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]解压${file_name}失败" >> ${log_path}/etl_${currdt}.log
	fi
 done
 #wait
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]解压所有文件完成" >> ${log_path}/etl_${currdt}.log
 exit 0
fi
#done
#exit 0


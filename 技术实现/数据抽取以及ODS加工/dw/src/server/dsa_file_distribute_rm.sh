#/bin/bash 
################################################################################
#脚本名称：dsa_file.sh 
#脚本功能：对外供数的数据分发脚本，按照系统级别进行调用
#输入参数：供数系统 供数日期 eg: sh dsa_file.sh KN 2019-02-28
#编写人    ：guxn
#编写日期：2019-06-04
#修改记录：

#检查脚本参数个数
if [ $# -ne 2 ];then
  echo "Usage:$0 system_flag etl_date"
  exit -1
fi

#变量赋值
#ETL路径
etl_home=/etl
#基础路径
base_path=$etl_home/dw
#sqlite数据库路径
sqlite_path=$base_path/conf
#日志文件路径
log_path=$base_path/log/server
#文件下载目录
file_path=/data/dsa/unld
#脚本所在路径
script_path=$base_path/src/server
#供数路径
dsa_dis_path=/data/dsa
#设置最小文本序号的大小
file_seq=1
#源文件存放目录
source_root=/data/src


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
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/dsa_${system_flag}.log

 #获取mysql数据库的连接串
mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_ULD_CMD)'`

#获取本分发系统的信号文件名称
single_file_name=`$mysql_conn_str "select para_val from etl_meta_args where parameter='dsa.single.file$system_flag';"`

#判断此系统文件是否需要压缩-sqlite参数表配置
#zip_flag=`$mysql_conn_str "select para_val from etl_meta_args where parameter='dsa.zip.$system_flag';"`

echo `date +"%Y-%m-%d %H:%M:%S"`"[START] 文件转移开始!" >> $log_path/dsa_${system_flag}.log

#判断供数路径是否存在
dsa_path=$dsa_dis_path/ext/$system_flag/$dsa_date
if [ -e  $dsa_path ];then
  rm -rf $dsa_path/*
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 供数路径存在，清空目录进行供数!" >> $log_path/dsa_${system_flag}.log
else
  mkdir -p $dsa_path
  chmod 750 $dsa_path
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 供数路径不存在，但已经创建！" >> $log_path/dsa_${system_flag}.log
fi

#供数OK文件路径
dsa_ok_path=$dsa_dis_path/ext/$system_flag

#获取传入系统名的最大的文件序号
max_file_seq=`$mysql_conn_str "select max(cast(trim(tar_file_seq) as unsigned integer)) from etl_serve_distribute where lower(system_flag)=lower('$system_flag')";`
if [ "$max_file_seq"x != "x" ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 最大文件序号为:$max_file_seq" >> $log_path/dsa_${system_flag}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 最大文件序号获取失败 ">> $log_path/dsa_${system_flag}.log
  exit 1
fi

#通过循环处理每个file_seq的内容
while [ $file_seq -le $max_file_seq ]
do
  temp=`$mysql_conn_str "select trim(tar_file_name),trim(tar_file_code),trim(source_file_path),trim(tar_file_source_type),trim(lower(source_name)),trim(source_file_name),trim(source_file_code),trim(tar_file_check_name) from etl_serve_distribute where tar_file_seq='$file_seq' and lower(system_flag)=lower('$system_flag') and status = 1"`
  if [ "$temp"x != "x" ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 读取数据库配置信息成功" >> $log_path/dsa_${system_flag}.log
  else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 读取数据库配置信息失败 ">> $log_path/dsa_${system_flag}.log
    exit 1
  fi
  #状态为1的进行分发`echo $connect_str|awk -F"|" '{print $1}'`
  if [ -n "$temp" ];then
    file_name=`echo $temp | awk -F" " '{print $1}'`
    file_code=`echo $temp | awk  -F" " '{print $2}'`
    source_file_path=`echo $temp | awk -F" " '{print $3}'`
    source_type=`echo $temp | awk -F" " '{print $4}'`
    source_name=`echo $temp | awk -F" " '{print $5}'`
    source_file=`echo $temp | awk -F" " '{print $6}'`
    source_code=`echo $temp | awk -F" " '{print $7}'`
    tar_file_check_name=`echo $temp | awk -F" " '{print $8}'`
    #echo "aa:$file_name--$file_code--$source_file_path--$source_type--$source_name--$source_file--$source_code--$tar_file_check_name"
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] The file $file_seq conf is source_file_path->$source_file_path file_name->$file_name file_code->$file_code ">> $log_path/dsa_${system_flag}.log
    #源文件为数仓卸载后的文件
    if [ "$source_type" == "unload" ];then
    	#转码，重命名文件
    	if [ ! "$file_code"x = "UTF-8"x ];then
      	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] iconv -c -f UTF-8 -t $file_code $file_path/$source_file_path/000000_0 > $dsa_path/$file_name" >> $log_path/dsa_${system_flag}.log
      	iconv -c -f UTF-8 -t $file_code $file_path/$source_file_path/000000_0 > $dsa_path/$file_name
      	if [ $? -eq 0 ];then
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_name 文件转移成功！" >> $log_path/dsa_${system_flag}.log
      	else
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $file_name 文件转移失败! ">> $log_path/dsa_${system_flag}.log
        	exit 1
      	fi
    	else
      	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] cp $file_path/$source_file_path/000000_0  $dsa_path/$file_name" >> $log_path/dsa_${system_flag}.log
      	cp $file_path/$source_file_path/000000_0  $dsa_path/$file_name
      	if [ $? -eq 0 ];then
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_name 文件转移成功！" >> $log_path/dsa_${system_flag}.log
      	else
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $file_name 文件转移失败! ">> $log_path/dsa_${system_flag}.log
        	exit 1
      	fi
    	fi
    fi
    #源文件为数源系统的文件
    if [ "$source_type" == "origin" ];then
  		#获取源系统表数据文件
   		typeset -l data_file
   		#data_file="load."$source_name".data.file"

   		load_data_file_tmp=`$mysql_conn_str "select lower(trim(para_val)) from etl_meta_args where parameter='load.sys.data.file';"`
   		#判断读取参数是否成功
    	if [ "$load_data_file_tmp"x = "x" ];then
      	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数data_file错误">> $log_path/dsa_${system_flag}.log
      	exit 1
    	fi
		typeset -u source_file
		source_file=$source_file
		load_data_file=`echo ${load_data_file_tmp//'$currdt'/$dsa_date}`
   		load_data_file=`echo ${load_data_file//'$srctablename'/$source_file}`
		
   		echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件名为：$load_data_file">> $log_path/dsa_${system_flag}.log    	
     	#转移文件：
    	if [ ! "$source_code"x = "$file_code"x ];then
      	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]EXECUTE:iconv -c -f $source_code -t $file_code $source_root/$source_name/$dsa_date/$load_data_file > $dsa_path/$file_name">> $log_path/dsa_${system_flag}.log
      	iconv  -c -f $source_code -t $file_code  $source_root/$source_name/$dsa_date/$load_data_file > $dsa_path/$file_name
      	if [ $? -eq 0 ];then
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_name 文件字符集转换,copy成功" >> $log_path/dsa_${system_flag}.log
      	else
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $file_name 文件字符集转换,copy失败">> $log_path/dsa_${system_flag}.log
        	exit 1
      	fi
   		else
     		echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]EXECUTE:cp $source_root/$source_name/$dsa_date/$load_data_file  $dsa_path/$file_name">> $log_path/dsa_${system_flag}.log
     		cp $source_root/$source_name/$dsa_date/$load_data_file  $dsa_path/$file_name
     		if [ $? -eq 0 ];then
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_name 文件copy成功" >> $log_path/dsa_${system_flag}.log
     		else
        	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $file_name 文件copy失败">> $log_path/dsa_${system_flag}.log
       		exit 1
     		fi
   		fi   	
  	fi
    if [ "$tar_file_check_name" != "" ];then
  		file_record_count=`awk 'END{print NR}' $dsa_path/$file_name`
  		file_size=`ls -l $dsa_path/$file_name | awk '{print $5}'`
  	  #生成校验文件
  	  echo "$file_name" >> $dsa_path/$tar_file_check_name
      echo "$file_record_count" >> $dsa_path/$tar_file_check_name
      echo "$file_size" >> $dsa_path/$tar_file_check_name
     	if [ $? -eq 0 ];then
      	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_name 校验文件生成成功" >> $log_path/dsa_${system_flag}.log
     	else
        echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $file_name 校验文件生成失败">> $log_path/dsa_${system_flag}.log
       	exit 1
     	fi  	
  	fi
    #状态为1的进行分发-结束
  fi
  let file_seq=$file_seq+1
done
#生成信号文件
#echo "分发路径：======"$dsa_path/$file_name
echo "${dsa_date}" > $dsa_ok_path/${system_flag}.ok
if [ $? -eq 0 ];then
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $system_flag 系统 信号文件生成成功" >> $log_path/dsa_${system_flag}.log
else
	echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] $system_flag 系统 信号文件生成失败">> $log_path/dsa_${system_flag}.log
	exit 1
fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[END] $system_flag 系统 文件转移结束" >> $log_path/dsa_${system_flag}.log
exit 0

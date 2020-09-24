#/bin/bash
#脚本名称：load_put_file.sh
#脚本功能：对源系统文件进行转换并上传到hdfs路径
#输入参数：表名 日期 eg: sh load_put_file.sh mb_bankdept 20190228
#编写人    ：guxn
#编写日期：20191225
#修改记录：
#by guxn 20191225  1.新建    


#判断传参个数
if [ $# -ne 2 ];then
  echo "Usage:$0 table_name date"
  exit -1
fi

#变量赋值：
#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/load
#文件源地址
source_path=/data/src
#文件路径(处理完成的文件目录)
target_path=/data/put


#输入参数获取
#要加载数据文件的表名
typeset -l tablename
tablename=$1

#获取要加载数据的源系统表名
#typeset -u srctablename
#srctablename=${tablename#*_}

#获取要加载数据的源系统缩写
sys=${tablename%%_*}

#日期
currdt=$2
currdt=`$etl_home/dw/src/util/get_date.sh $currdt`

#日志文件的路径创建
log_path=$log_path/$currdt
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
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件路径:$source_path">> $log_path/etl_$tablename$currdt.log
#切换路径
cd $etl_home/dw
#获取mysql数据库的连接串
mysql_conn_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.MYSQL_ULD_CMD)'`
#获取要加载数据文件的目录
hdfs_path=` $mysql_conn_str "select lower(trans_path) from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
#判断读取参数是否成功
 if [ "$hdfs_path"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数hdfs path错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]HDFS存储路径:$hdfs_path"  >> $log_path/etl_$tablename$currdt.log

#kerberos切换到dw用户
kinit dw -kt /etl/dw/conf/dw.keytab

load_src_path=$source_path/$sys
source_path=$source_path/$sys/$currdt
#如果没有目录就创建
if [ ! -e $source_path ];then
  mkdir -p $source_path
fi

#文件路径(处理完成的文件目录)
target_path=$target_path/$sys/$currdt
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]目标文件路径:$target_path" >> $log_path/etl_$tablename$currdt.log
#如果没有目录就创建
if [ ! -e $target_path ];then
  mkdir -p $target_path
fi

#删除目标系统目录下的数据文件
if [ -e $target_path/$tablename.txt ];then
  rm -rf $target_path/$tablename.txt
  echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
fi

#获取源系统表数据文件
 file_name_tmp=`$mysql_conn_str "select file_name from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
#判断读取参数是否成功
 if [ "$file_name_tmp"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数file_name错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi
load_data_file=`echo ${file_name_tmp//'$currdt'/$currdt}`
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统文件名为：$load_data_file">> $log_path/etl_$tablename$currdt.log

#获取检核文件
 is_check=`$mysql_conn_str "select is_exist_check_file from etl_load_system where upper(sys)=upper('$sys');"`
 #判断读取参数是否成功,读取为空为异常
if [ "$is_check"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数check_file错误">> $log_path/etl_$tablename$currdt.log
   exit -1
elif [ "$is_check" = "1" ];then
   #ck_file="load."$sys".ck.file"
   load_ck_file_tmp=`$mysql_conn_str "select check_file from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
   #判断读取参数是否成功
   if [ "$load_ck_file_tmp"x = "x" ];then
     echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数check_file错误">> $log_path/etl_$tablename$currdt.log
     exit -1
   fi
   #判断check方式，获取对应的check文件
   if [ "${load_ck_file_tmp:0:4}" = "DIR." -o "${load_ck_file_tmp:0:4}" = "dir." ];then
     load_ck_file=`echo ${load_ck_file_tmp//'$currdt'/$currdt}`
     #load_ck_file=`echo ${load_ck_file//'$srctablename'/$srctablename}`
   else
     load_ck_file=$load_ck_file_tmp
   fi
else
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]本系统不存在检核文件">> $log_path/etl_$tablename$currdt.log
fi

#如果文本文件存在则执行文件行数检查、大小校验、MD5校验、字符集转换
ls $source_path | grep $load_data_file >/dev/null 2>&1   
if [ $? -eq 0 ];then

  #文件行数检查
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]检核文件名:$source_path/$load_ck_file" >> $log_path/etl_$tablename$currdt.log
  
  #判断check文件是否包含特殊字符,将替换成空
  if [ "$load_ck_file"x != "x" ];then
  	num=`cat $source_path/$load_ck_file | grep '
' | wc -l `
  	if [ "$num" -ne 0 ];then
   	  echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]sed -i 's/^M//g' $source_path/$load_ck_file" >> $log_path/etl_$tablename$currdt.log
      sed -i 's/
//g' $source_path/$load_ck_file
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]检核文件替换^M成功" >> $log_path/etl_$tablename$currdt.log
     fi
  fi
  #读取check文件行数校验
  if [ "$load_ck_file"x != "x" ];then
    #根据check文件类型不同进行不同处理
    if [ "${load_ck_file:0:4}" = "DIR."  -o "${load_ck_file_tmp:0:4}" = "dir." ];then
      row_orig_value=`sed -n '3p' $source_path/$load_ck_file`
      row_value=`sed -n '$=' $source_path/$load_data_file`
    else
      row_orig_value=`cat $source_path/$load_ck_file |grep -w $load_data_file | awk -F":" '{print $2}'`
      row_value=`sed -n '$=' $source_path/$load_data_file`
    fi
    #数据文件为空row_value赋值为0
    if [ "$row_value"x = "x" ];then
      row_value=0
    fi
    #验证数据行数
    if [ "$row_orig_value" != "$row_value" ];then
      echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]文件行数校验错误:$row_orig_value != $row_value">> $log_path/etl_$tablename$currdt.log
      exit -1
    else
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]文件行数校验成功">> $log_path/etl_$tablename$currdt.log
    fi
  fi
  
  #文件大小检查
  if [ "$load_ck_file"x != "x" ] && [ "${load_ck_file:0:4}" = "DIR." -o "${load_ck_file_tmp:0:4}" = "dir." ];then
    byte_orig_value=`sed -n "2,1p" $source_path/$load_ck_file`
    byte_value=` ls -l  $source_path/$load_data_file |awk -F" " '{print $5}' `
    if [ "$byte_value"x = "x" ];then
      byte_value=0
    fi
    if [ "$byte_orig_value" != "$byte_value" ];then
      echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]文件大小校验错误">> $log_path/etl_$tablename$currdt.log
      exit -1
    else
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]文件大小校验成功">> $log_path/etl_$tablename$currdt.log
    fi
  fi
  
  #获取系统是否使用MD5校验
  if [ "$load_ck_file"x != "x" ] && [ "${load_ck_file:0:4}" != "DIR." -o "${load_ck_file_tmp:0:4}" = "dir." ];then
    md5_orig_value=`cat $source_path/$load_ck_file | grep -w $load_data_file | awk -F ":" '{print $3}'`
    md5_value=`md5sum $source_path/$load_data_file | awk '{print $1}'`
    if [ "$md5_orig_value" != "$md5_value" ];then
      echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]文件md5校验失败:$md5_orig_value != $md5_value">> $log_path/etl_$tablename$currdt.log
        exit -1
    else
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]文件md5校验成功">> $log_path/etl_$tablename$currdt.log
    fi
  fi

#获取codec配置，从表级获取每张表的字符集编码
codec=`$mysql_conn_str "select encode_format from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
 #判断读取参数是否成功
 if [ "$codec"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数codec错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源系统表字符集编码为:$codec" >> $log_path/etl_$tablename$currdt.log

  if [[ "$codec"x = "UTF-8"x ]];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE] cp $source_path/$load_data_file  $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
    cp $source_path/$load_data_file  $target_path/$tablename.txt
    if [ $? -eq 0 ];then
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]文件字符集为UTF-8，无需转换，Copy成功" >> $log_path/etl_$tablename$currdt.log
    else
      echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]文件字符集为UTF-8，无需转换，Copy错误">> $log_path/etl_$tablename$currdt.log
      exit -1
    fi
  else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]iconv -c -f $codec -t UTF-8 $source_path/$load_data_file > $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
    iconv -c -f $codec -t UTF-8 $source_path/$load_data_file > $target_path/$tablename.txt
    if [ $? -eq 0 ];then
      echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]文件字符集转换成功" >> $log_path/etl_$tablename$currdt.log
    else
      echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]文件字符集转换错误">> $log_path/etl_$tablename$currdt.log
      exit -1
    fi
  fi
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]$source_path/$load_data_file文件不存在." >>$log_path/etl_$tablename$currdt.log
  exit -1
fi

#获取系统是否存在定长字段
is_fix_length=`$mysql_conn_str "select is_fix_length from etl_load_system where lower(sys)=lower('$sys');"`
 #判断读取参数是否成功
 if [ "$is_fix_length"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数is_fix_length错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi
 
#空值类型转换\N,直接在分隔符之间加了\N，还未判断特殊情况，后续遇到再增加，目前遇到的分隔符只有^B和|
col_sep=`$mysql_conn_str "select col_separator from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
 #判断读取参数是否成功
 if [ "$col_sep"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数col_sep错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi
#字段分隔符为^B时
if  [ "$col_sep"x = "\002"x ] || [ "$col_sep"x = ""x   ] ;then
  #系统存在定长处理时
  if [ "$is_fix_length" = "1" ];then
    perl -p -i -e 's/[ ]*\002/\002/g;s/\002\002/\002\\N\002/g;s/\002\002/\002\\N\002/g'  $target_path/$tablename.txt >/dev/null 2>&1
    if [ $? -eq 0 ];then
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
    else
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	  exit -1
    fi
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/[ ]*\002/\002/g;s/\002\002/\002\\N\002/g;s/\002\002/\002\\N\002/g'   $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  #系统不存在定长处理时
  else
    perl -p -i -e 's/\002\002/\002\\N\002/g;s/\002\002/\002\\N\002/g'  $target_path/$tablename.txt >/dev/null 2>&1
    if [ $? -eq 0 ];then
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
    else
  	  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	  exit -1
    fi
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/\002\002/\002\\N\002/g;s/\002\002/\002\\N\002/g'   $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  fi
#字段分隔符为|时
elif  [ "$col_sep"x = "|"x ];then
  #系统存在定长处理时
  if [ "$is_fix_length" = "1" ];then
     perl -p -i -e 's/[ ]*\|/\|/g;s/\|\|/\|\\N\|/g;s/\|\|/\|\\N\|/g'  $target_path/$tablename.txt >/dev/null 2>&1
     if [ $? -eq 0 ];then
  	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
     else
  	    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	     exit -1
     fi
     echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/[ ]*\|/\|/g;s/\|\|/\|\\N\|/g;s/\|\|/\|\\N\|/g' $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  #系统不存在定长处理时
  else
    perl -p -i -e 's/\|\|/\|\\N\|/g;s/\|\|/\|\\N\|/g'  $target_path/$tablename.txt >/dev/null 2>&1
     if [ $? -eq 0 ];then
  	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
     else
  	    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	     exit -1
     fi
     echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/\|\|/\|\\N\|/g;s/\|\|/\|\\N\|/g' $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  fi
#字段分隔符为~@~时
elif  [ "$col_sep"x = "~@~"x ];then
  #系统存在定长处理时
  if [ "$is_fix_length" = "1" ];then
     perl -p -i -e 's/[ ]*\~@~/\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g'  $target_path/$tablename.txt >/dev/null 2>&1
     if [ $? -eq 0 ];then
  	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
     else
  	    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	     exit -1
     fi
     echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/[ ]*\~@~/\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g' $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  #系统不存在定长处理时
  else
    perl -p -i -e 's/\~@~\~@~/\~@~\\N\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g'  $target_path/$tablename.txt >/dev/null 2>&1
     if [ $? -eq 0 ];then
  	   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]空值转换\N成功" >> $log_path/etl_$tablename$currdt.log
     else
  	    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]空值转换\N错误">> $log_path/etl_$tablename$currdt.log
  	     exit -1
     fi
     echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]perl -p -i -e 's/\~@~\~@~/\~@~\\N\~@~/g;s/\~@~\~@~/\~@~\\N\~@~/g' $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
  fi
fi

#判断转换前后的文件行数是否一致
snum=`wc -l  $source_path/$load_data_file | awk -F" " '{print $1}' `
tnum=`wc -l  $target_path/$tablename.txt | awk -F" " '{print $1}' `
if [ "$snum" -ne "$tnum" ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]源文件与目标文件数据行数校验错误，源：$snum ，目标： $tnum" >> $log_path/etl_$tablename$currdt.log
  exit -1
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]源文件与目标文件数据行数校验成功，源：$snum ，目标： $tnum" >> $log_path/etl_$tablename$currdt.log
fi

#检查模块
#检查表文件路径是否存在
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]检查HDFS路径是否存在" >> $log_path/etl_$tablename$currdt.log  
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]hdfs dfs -ls $hdfs_path" >> $log_path/etl_$tablename$currdt.log
hdfs dfs -ls $hdfs_path   >> $log_path/etl_$tablename$currdt.log 2>&1

if [ $? -eq 0 ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]$hdfs_path路径存在" >> $log_path/etl_$tablename$currdt.log
else
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]$hdfs_path路径不存在"  >> $log_path/etl_$tablename$currdt.log
   exit -1
fi

#获取字段的个数的校验值
 is_col=`$mysql_conn_str "select is_col_separator from etl_load_table where lower(concat(sys,'_',tab_name))=lower('$tablename');"`
   #判断读取参数是否成功
 if [ "$is_col"x = "x" ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]读取参数is_col错误">> $log_path/etl_$tablename$currdt.log
   exit -1
 fi

  #获取表最大字段个数，当行末尾存在分隔符的时候，最大字段个数+1
  #max_col_seq=`$mysql_conn_str "select count(1) from etl_load_column where lower(concat(sys,'_',tab_name))=lower('$tablename') group by tab_name ;"`
  #if [ "$is_col" = "1" ];then
  #  value=$(($max_col_seq+1))
  #else
  #  value=$max_col_seq
  #fi
  ##检查文件合法性
  #echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]开始调用Python进行字段个数校验"  >> $log_path/etl_$tablename$currdt.log  
  #echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]python $etl_home/dw/src/util/check_replace.py $target_path/$tablename.txt  $value  $col_sep" >> $log_path/etl_$tablename$currdt.log
  #if [ "$col_sep" == "\002" ];then
  #  clm_sep=""
  #else
  #  clm_sep=$col_sep
  #fi
  #python $etl_home/dw/src/util/check_replace.py $target_path/$tablename.txt $value $clm_sep >> $log_path/etl_$tablename$currdt.log
  #if [ $? -eq 0 ];then
  #  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]字段个数校验成功" >> $log_path/etl_$tablename$currdt.log
  #else
  #  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]字段个数校验错误">> $log_path/etl_$tablename$currdt.log
  #  exit -1
  #fi
  #echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]结束调用Python进行字段个数校验">> $log_path/etl_$tablename$currdt.log  

#加载模块 
#检查表文件是否存在

#判断是否包含特殊字符,将替换成空
  num=`cat $target_path/$tablename.txt | grep '
' | wc -l `
  if [ "$num" -ne 0 ];then
   echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]sed -i 's/^M//g' $target_path/$tablename.txt" >> $log_path/etl_$tablename$currdt.log
   sed -i 's/
//g' $target_path/$tablename.txt
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
echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE] hdfs dfs -put $target_path/$tablename.txt $hdfs_path/$tablename.txt" >>$log_path/etl_$tablename$currdt.log
hdfs dfs -put $target_path/$tablename.txt $hdfs_path/$tablename.txt >> $log_path/etl_$tablename$currdt.log 2>&1
if [ $? -eq 0 ];then
    hdfs dfs -chmod 644 $hdfs_path/$tablename.txt >> $log_path/etl_$tablename$currdt.log 2>&1
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]PUT上传文件成功" >> $log_path/etl_$tablename$currdt.log
else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]PUT上传文件失败" >> $log_path/etl_$tablename$currdt.log
    exit -1
fi

exit 0


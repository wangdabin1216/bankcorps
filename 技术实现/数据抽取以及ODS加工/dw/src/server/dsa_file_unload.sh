#/bin/bash 
#脚本名称：dsa_file_unload_base.sh
#脚本功能：对外供数的单表操作的脚本，从数据库中导出数据
#输入参数：需要传入三个参数，第一个为表名，第二个为文件序号，第三个个为供数日期
#编写人    ：guxn
#编写日期：2019-04-05
#修改记录：
#by wjx 201904011         1.修改从mysql数据库读取参数       
#                        2.因存储过程调整导出为HDFS，因此在脚本中增加get步骤
#by guxn 20190423        1.调整参数名称和脚本名称

#ETL路径
etl_home=/etl
#基础路径
base_path=$etl_home/dw
#日志文件路径
log_path=$base_path/log/server
#文件下载目录
file_path=/data/dsa/unld
#脚本所在路径
script_path=$base_path/src/server
#设置卸载文件的HDFS目录
hdfs_path=/dw/dsa
#设置最小文本序号的大小
#file_seq=1

#检查脚本参数个数
if [ $# -ne 3 ];then
  echo "Usage:$0 table_name file_seq data_date"
  exit -1
fi

# kinit dw用户
kinit dw -kt $base_path/conf/dw.keytab

#接收传入的表名
table_name=$1

#文本序号
file_seq=$2

#接收供数的日期
dsa_date=$3

#接收供数系统名称
#sys_name=$4

#带序号的表名
sche_tbl_name=$table_name"~@~"$file_seq

#日期
currdt=$3
currdt=`$base_path/src/util/get_date.sh $currdt`


#设置日志文件的路径
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

#变量输出到日志文件
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]base脚本日志路径为：$log_path">> $log_path/unload_${sche_tbl_name}.log
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $hdfs_path/${table_name}_${file_seq}" >> $log_path/unload_${sche_tbl_name}.log

#获取beeline连接串
beeline_cmd_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.BEELINE_CMD)'`

#判断在HDFS上是否存在表名的目录
hadoop fs -ls $hdfs_path/${table_name}_${file_seq}/ >/dev/null 2>&1
if [ $? -eq 0 ];then
   hadoop fs -rm -r $hdfs_path/${table_name}_${file_seq}/ >/dev/null 2>&1
   echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $hdfs_path/${table_name}_${file_seq} 路径下有该表的目录，已经清空，可以卸载数据!" >> $log_path/unload_${sche_tbl_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $hdfs_path/${table_name}_${file_seq} 路径下没有该表的目录，可以卸载数据！" >> $log_path/unload_${sche_tbl_name}.log
fi

#判断本地目录是否存在，若存在，删除后创建，否则就创建
 if [ -e $file_path/${table_name}_${file_seq} ];then
     rm -rf $file_path/${table_name}_${file_seq}/
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_path/${table_name}_${file_seq} 路径存在，删除并创建" >> $log_path/unload_${sche_tbl_name}.log 
     mkdir -p $file_path/${table_name}_${file_seq}/
  else 
     mkdir -p $file_path/${table_name}_${file_seq}/
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] $file_path/${table_name}_${file_seq} 路径不存在,创建" >> $log_path/unload_${sche_tbl_name}.log
  fi

#echo "本地路径：======"$file_path/${table_name}_${file_seq}

#调用存储过程，将数据导出到inceptor服务器本地
beeline_cmd=`$beeline_cmd_str -e "begin cc.pkg_dw_util.pro_serv_dsa_unload($file_seq,'$table_name','$dsa_date') end"`
if [ $? -eq 0 ];then
     hadoop fs -get $hdfs_path/${table_name}_${file_seq}/000000_0 $file_path/${table_name}_${file_seq}/000000_0
     hadoop fs -get $hdfs_path/${table_name}_${file_seq}/000001_0 $file_path/${table_name}_${file_seq}/000001_0
     hadoop fs -get $hdfs_path/${table_name}_${file_seq}/000002_0 $file_path/${table_name}_${file_seq}/000002_0
     hadoop fs -get $hdfs_path/${table_name}_${file_seq}/000003_0 $file_path/${table_name}_${file_seq}/000003_0
     hadoop fs -get $hdfs_path/${table_name}_${file_seq}/000004_0 $file_path/${table_name}_${file_seq}/000004_0
     cat $file_path/${table_name}_${file_seq}/000001_0 >> $file_path/${table_name}_${file_seq}/000000_0
     cat $file_path/${table_name}_${file_seq}/000002_0 >> $file_path/${table_name}_${file_seq}/000000_0
     cat $file_path/${table_name}_${file_seq}/000003_0 >> $file_path/${table_name}_${file_seq}/000000_0
     cat $file_path/${table_name}_${file_seq}/000004_0 >> $file_path/${table_name}_${file_seq}/000000_0  
     echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 已经将数据都导出到本地!" >> $log_path/unload_${sche_tbl_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 连接不上数据库或配置表信息有误" >>$log_path/unload_${sche_tbl_name}.log
  exit 1
fi
#:<<!
#检查文件记录数和数据库中记录数
if [ "$table_name" != "fdm_td_trad_rt" ] && [ "$table_name" != "crew_a_all_sign_result" ];then
  count=`$beeline_cmd_str -e "begin cc.pkg_dw_util.pro_check_unload_num($file_seq,'$table_name','$dsa_date') end"`

#echo "数据的数量======="$count

  if [ $? -eq 0 ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 已获取记录数!" >> $log_path/unload_${sche_tbl_name}.log
  else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 连接数据库失败!" >>$log_path/unload_${sche_tbl_name}.log
    exit 1
  fi
  count=`echo $count | awk '{print $NF}'`
  count=`echo ${count//'"'/''}`
#echo "数据的数量======="$count

  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 数据库数量 $count " >> $log_path/unload_${sche_tbl_name}.log
  file_count=`wc -l $file_path/${table_name}_${file_seq}/000000_0 | awk '{print $1}'`
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 文件数据  $file_count " >> $log_path/unload_${sche_tbl_name}.log
  if [ "$count" != "$file_count" ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 数据库数量和文件数量不一致!" >> $log_path/unload_${sche_tbl_name}.log
   exit 1
  fi
fi
#!
#使用sed工具修改导出的文本的内容，将\N去掉，将分隔符替代为~@~
#sed -i 's/\x01/~@~/g' /mdp/dsa/share/${table_name}_${file_seq}/000000_0 && sed -i 's/\\N//g' /mdp/dsa/share/${table_name}_${file_seq}/000000_0
perl -p -i -e 's/\\N//g' $file_path/${table_name}_${file_seq}/000000_0
if [ $? -eq 0 ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 已经对文本进行处理!" >> $log_path/unload_${sche_tbl_name}.log
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[Error] 对文本处理失败，请检查!" >>$log_path/unload_${sche_tbl_name}.log
  exit 1
fi
exit 0

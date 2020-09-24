#!/bin/bash
#############################################################
#脚本名称:load_check_from_mid.sh
#脚本功能:检查系统是否满足加载条件
#脚本参数:系统
#脚本运行:sh load_check_from_mid.sh rm
#编写人:gxn
#编写时间:20200319
#功能实现:
###########################################################
if [ $# -ne 1 ];then
  echo "Usage:$0 system"
   exit -1
fi


#脚本路径
base_path=/data/bigdata
#shell路径
sh_path=${base_path}/script
#日志文件路径
log_path=${base_path}/log

#系统简称
#sys=${v_tgt_table%%_*}
typeset -l sys
sys=$1

#检查系统日终是否完成
while [ 1 == 1 ]
   do
	   #判断系统是否传输完成
	   if [ -e ${base_path}/${sys}.ok ];then
	    
		#日期
	    date=`cat $base_path/${sys}.ok`

	    #设置日志文件路径
	    log_path=${log_path}/${date}
	    if [ ! -e ${log_path} ];then
	     mkdir -p ${log_path}
	    fi

	    #休息会儿,防止文件正在传输中
	    sleep 100
		
		#解压文件
		echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO] 解压文件">${log_path}/${sys}.log
        cd $base_path
		tar -zxvf ${date}.tar.gz >>${log_path}/${sys}.log
		
		#清空sqlldr_error.txt文件
		echo ""> ${log_path}/sqlldr_error.txt
		
		#将数据文件拷贝到固定目录,拷贝之前先清空目录
		rm -rf /data/bigdata/tmp/*
		cp /data/bigdata/${date}/* /data/bigdata/tmp/
		echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO] 拷贝文件成功">>${log_path}/${sys}.log
		
		source /home/oracle/.bash_profile
		#循环读取load的表进行数据加载
		while read table
		do
		 sh $sh_path/load_sqlldr_data.sh $sys $table $date
		done </data/bigdata/conf/rm_tables.txt
        
		#输出日志
	    echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO] 系统OK文件存在调用load_sqlldr_data.sh进行数据加载">>${log_path}/${sys}.log
	    #删除tar包
        if [ -e $base_path/*.tar.gz ];then
          rm -rf $base_path/*.tar.gz
         fi
        #删除ok文件
        if [ -e $base_path/*.ok ];then
          rm -rf $base_path/*.ok
        fi
        exit 0
	   else
	    sleep 300
	    echo `date "+%Y-%m-%d %H:%M:%S"` "[INFO] 系统OK文件不存在：${base_path}/${sys}.ok ，请等待">>${log_path}/${sys}.log
	   fi
done
exit 0

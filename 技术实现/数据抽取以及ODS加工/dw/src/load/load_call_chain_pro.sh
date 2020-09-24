#!/usr/bin/env bash
#/bin/bash
#脚本名称：load_call_chain_pro.sh
#脚本功能：执行存储过程
#输入参数：数据库名称 存储过程名称 日期 eg: sh load_call_chain_pro.sh tbo.ib_ln_credit_apply_mst 20200101
#编写人    ：guxn
#编写日期：20200305
#修改记录：

#判断传参个数
if [ $# -ne 2 ];then
  echo "Usage:table_name date"
  exit -1
fi

#ETL路径
etl_home=/etl
#基础路径
base_path=$etl_home/dw
#日志文件路径
log_path=$base_path/log/load
#脚本所在路径
script_path=$base_path/src/load
# kinit dw用户
kinit dw -kt $base_path/conf/dw.keytab
#表名
table_name=$1
#日期
currdt=$2
currdt=`$base_path/src/util/get_date.sh $currdt`


#设置日志文件的路径
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

#变量输出到日志文件
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]base脚本日志路径为：$log_path"> $log_path/load_chain_${table_name}.log
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${table_name}:${currdt}" >> $log_path/load_chain_${table_name}.log

#获取beeline连接串
beeline_cmd_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.BEELINE_CMD)'`

#调用存储过程，将数据导出到inceptor服务器本地
beeline_cmd=`$beeline_cmd_str -e "begin cc.pkg_dw_util.pro_data_his_main('$currdt','$table_name') end"`
if [ $? -eq 0 ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 执行存储过程 cc.pkg_dw_util.pro_data_his_main('$currdt','$table_name') 成功" >>$log_path/load_chain_${table_name}.log  
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 执行存储过程 cc.pkg_dw_util.pro_data_his_main('$currdt','$table_name') 失败" >>$log_path/load_chain_${table_name}.log
  exit 1
fi


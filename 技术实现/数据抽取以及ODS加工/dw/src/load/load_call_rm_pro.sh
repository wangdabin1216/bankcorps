#/bin/bash 
#脚本名称：load_call_rm_pro.sh
#脚本功能：执行存储过程
#输入参数：存储过程名称 日期 eg: sh load_call_rm_pro.sh dmm.pro_rm_jinjzb_hs 20200101
#编写人    ：guxn
#编写日期：20200322
#修改记录：

#判断传参个数
if [ $# -ne 2 ];then
  echo "Usage:pro_name date"
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
pro_name=$1
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
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]base脚本日志路径为：$log_path"> $log_path/load_${pro_name}.log
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]${pro_name}:${currdt}" >> $log_path/load_${pro_name}.log

#获取beeline连接串
beeline_cmd_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.BEELINE_CMD)'`

#调用存储过程，将数据导出到inceptor服务器本地
beeline_cmd=`$beeline_cmd_str -e "begin ${pro_name}('$currdt') end"`
if [ $? -eq 0 ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO] 执行存储过程 begin ${pro_name}('$currdt') end 成功" >>$log_path/load_${pro_name}.log  
else
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR] 执行存储过程 begin ${pro_name}('$currdt') end 失败" >>$log_path/load_${pro_name}.log
  exit 1
fi
exit 0



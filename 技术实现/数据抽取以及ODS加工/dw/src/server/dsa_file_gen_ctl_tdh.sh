#!/bin/bash
#脚本名称：dsa_file_gen_ctl_tdh.sh
#脚本功能：对源系统文件进行转换并上传到hdfs路径
#输入参数：用户 表名 eg: sh dsa_file_gen_ctl_tdh.sh omi ib_ln_credit_apply_mst
#编写人    ：guxn
#编写日期：20191225
#修改记录：
#by guxn 20191225       1.新建

if [ $# -ne 2 ];then
  echo "Usage:$0 user tablename"
  exit -1
fi

#基础路径
etl_home=/etl
#日志文件路径
log_path=$etl_home/dw/log/dsa
#传入参数赋值，oracle的用户名
username=$1
#传入参数赋值，oracle的表名
typeset -l tablename
tablename=$2
#echo "tablename: $1"
#获取要加载数据的源系统表名
typeset -u srctablename
srctablename=${tablename#*_}
#获取要加载数据的源系统缩写
typeset -l sys
sys=${tablename%%_*}
#echo "sys:$sys"
#日志文件的路径创建
log_path=$log_path/$currdt
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir $log_path
fi

#变量输出到日志文件
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志路径为：$log_path"> $log_path/dsa_file_gen_ctl.log
#获取beeline连接串
beeline_cmd_str=`python -c 'import sys;sys.path.append(r"/etl/dw/src/init");import setting;print(setting.BEELINE_CMD)'`

#P_DB_CONNECT="neutron/neutron@10.4.11.110:1521/zgcuatdb"
szResult=`select  \
'LOAD DATA \
CHARACTERSET UTF8 \
INFILE \'/data/bigdata/tmp/ib_ln_credit_apply_mst_hs.txt\' \
truncate INTO TABLE dds.ib_ln_credit_apply_mst_hs FIELDS TERMINATED BY \"~\"  \
TRAILING NULLCOLS  \
\('  from cc.dual \
union all \
select  \
concat(COLUMN_NAME,' ', \
DECODE(COLUMN_TYPE,   \
'timestamp','TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF"',   \
'string','',   \
'date','DATE "YYYY-MM-DD"'  \
),',') \
from (  \
select * from  system.columns_v \
where database_name=lower('omi') AND table_name = lower('ib_ln_credit_apply_mst_hs')  \
order by column_id)  \
union all  \
select \')\' from cc.dual;`
#执行SQL
#a=`select  "LOAD DATA CHARACTERSET UTF8 INFILE '/data/bigdata/tmp/ib_ln_credit_apply_mst_hs.txt' truncate  INTO TABLE dds.ib_ln_credit_apply_mst_hs FIELDS TERMINATED BY \"~\"  TRAILING NULLCOLS  ("  from cc.dual union all select  concat(COLUMN_NAME,' ', DECODE(COLUMN_TYPE,   'timestamp','TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF"',   'string','',   'date','DATE "YYYY-MM-DD"'  ),',') from (  select * from  system.columns_v where database_name=lower('omi') AND table_name = lower('ib_ln_credit_apply_mst_hs')  order by column_id)  union all  select ")" from cc.dual;`
szResult=`$beeline_cmd_str -e "$szResult" > /etl/dw/conf/ctl/${tablename}_hs.ctl`
echo "database_name=lower('$username') AND table_name = lower('${tablename}_hs')  "
echo "$szResult" #> /etl/dw/conf/ctl/${tablename}_hs.ctl

  
  
  
  
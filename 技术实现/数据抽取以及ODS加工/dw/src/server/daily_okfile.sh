#!/bin/bash
#传入日期参数：yyyy-MM-dd
#脚本名称：run.sh
#脚本功能：非月底生成ok文件
#输入参数: yyyy-MM-dd eg: sh daily_okfile.sh 2019-02-28
#编写人  ：dk
#编写日期：20191118
#修改记录：

#判断传参个数
if [ $# -ne 1 ];then
  echo "Usage:$0 date"
  exit -1
fi

etldt=$1

#基础路径
etl_home=/etl/dw

currdt=`$etl_home/src/util/get_date.sh $etldt`

#配置文件路径
conf_path=${etl_home}/conf
#日志文件路径
log_path=${etl_home}/log/load/${currdt}
#配置文件名称
configfile=${conf_path}/znbb.conf

#ODS报表文件路径(使用:$dsa_path/$currdt)
dsa_path=$(cat ${configfile} | grep -i '^dsa_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
dsa_path=${dsa_path}/${currdt}
#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
mthdt=`date -d "${currdt}" +%m%d`
#当前日期下一天的天数
tomnum=`date -d "${currdt} tomorrow" +\%e`


#创建目录
if [ ! -e ${dsa_path} ];then
  mkdir -p ${dsa_path}
fi
#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]运行数据日期:$currdt" > $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]运行10位数据日期:$etldt" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]月底日期:$mthdt" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]当前日期下一天的天数:$tomnum" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]配置文件路径:$conf_path" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志文件路径:$log_path" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]卸载文件路径:$dsa_path" >> $log_path/run_$currdt.log

#运行非月底数据时，只给下游系统生成空文件
if [ ${tomnum} != 1 ] && [ ${tomnum} != 2 ];then
	echo "">${dsa_path}/i_bd_rpt_partner_fee_month_${currdt}_000_000.dat
	echo "">${dsa_path}/i_bd_rpt_partner_fee_quart_${currdt}_000_000.dat
	echo "">${dsa_path}/i_bd_rpt_partner_fee_hyear_${currdt}_000_000.dat
	echo "">${dsa_path}/etl_${currdt}.ok
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]非月初或月底数据，生成空文件完成" >> $log_path/run_$currdt.log
fi



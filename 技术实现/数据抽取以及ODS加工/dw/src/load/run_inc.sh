#!/bin/bash
#传入日期参数：yyyy-MM-dd
#脚本名称：run.sh
#脚本功能：整体运行脚本
#输入参数: yyyy-MM-dd eg: sh run.sh 2019-02-28
#编写人  ：dk
#编写日期：20190920
#修改记录：
#by dk 20190926
#by dk 20191010 增加以文件方式读取日期配置文件

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
#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#卸数目录(使用:$target_path/$sys/$currdt)
target_path=$(cat ${configfile} | grep -i '^target_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#上传文件路径(处理完成的文件目录,使用:$put_path/$sys/$currdt)
put_path=$(cat ${configfile} | grep -i '^put_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')

mthdt=`date -d "${currdt}" +%m%d`
#当前日期下一天的天数
tomnum=`date -d "${currdt} tomorrow" +\%e`

dsa_path=${dsa_path}/${currdt}
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
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]非月底数据，生成空文件完成" >> $log_path/run_$currdt.log
fi

#每天运行
#解压文件
sh ${etl_home}/src/load/load_unzip_inc_znbb.sh ${etldt}
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每天运行解压文件脚本运行完成" >> $log_path/run_$currdt.log
#put文件
cat ${conf_path}/table.conf | while read line
do
table_name=`echo ${line} | awk -F, '{print $2}'`
sh ${etl_home}/src/load/load_put_file_znbb.sh ${table_name} ${etldt}
done
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每天运行put脚本运行完成" >> $log_path/run_$currdt.log
#调用存储过程
sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_mb_acct_balance ${etldt} >> $log_path/pro_mid_mb_acct_balance_$currdt.log
sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_orc_tpay_main ${etldt} >> $log_path/pro_orc_tpay_main_$currdt.log
sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_orc_mb_acc_int_det_spl ${etldt} >> $log_path/pro_orc_mb_acc_int_det_spl_$currdt.log

echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每天运行调用存储过程脚本运行完成" >> $log_path/run_$currdt.log

#月末运行
if [ ${tomnum} = 1 ];then
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_plan ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_plan_$currdt.log
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_actual ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_actual_$currdt.log
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_month ${etldt} >> $log_path/pro_rpt_partner_fee_month_$currdt.log
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每月末运行脚本运行完成" >> $log_path/run_$currdt.log
elif [ ${tomnum} = 2 ];then
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_plan ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_plan_$currdt.log
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_actual ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_actual_$currdt.log
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_month2 ${etldt} >> $log_path/pro_rpt_partner_fee_month2_$currdt.log
        echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每月初运行脚本运行完成" >> $log_path/run_$currdt.log
else
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]不是月初，也不是月末" >> $log_path/run_$currdt.log
fi

#季末运行
if [ "${mthdt}" = "0331" ] || [ "${mthdt}" = "0630" ] || [ "${mthdt}" = "0930" ] || [ "${mthdt}" = "1231" ];then
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_quart ${etldt} >> $log_path/pro_rpt_partner_fee_quart_$currdt.log
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每季运行脚本运行完成" >> $log_path/run_$currdt.log
else
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]不是季末" >> $log_path/run_$currdt.log
fi

#半年末运行
if [ "${mthdt}" = "0630" ] || [ "${mthdt}" = "1231" ];then
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_hyear ${etldt} >> $log_path/pro_rpt_partner_fee_hyear_$currdt.log
        echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]半年末运行脚本运行完成" >> $log_path/run_$currdt.log
else
        echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]不是半年" >> $log_path/run_$currdt.log
fi

if [ ${tomnum} = 1 ] || [ ${tomnum} = 2 ];then
 #卸数与生成OK文件
 sh ${etl_home}/src/server/dsa_file_unload_znbb.sh rpt_partner_fee_month ${etldt} >> $log_path/rpt_partner_fee_month_$currdt.log
 sh ${etl_home}/src/server/dsa_file_unload_znbb.sh rpt_partner_fee_quart ${etldt} >> $log_path/rpt_partner_fee_quart_$currdt.log
 sh ${etl_home}/src/server/dsa_file_unload_znbb.sh rpt_partner_fee_hyear ${etldt} >> $log_path/rpt_partner_fee_hyear_$currdt.log
 echo "">${dsa_path}/etl_${currdt}.ok
 echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]卸数与生成OK文件完成" >> $log_path/run_$currdt.log
fi

#清除处理完成的数据，释放磁盘空间
#文件路径(处理完成的文件目录)
rm -rf ${target_path}/${sys}/${currdt}
rm -rf ${put_path}/${sys}/${currdt}

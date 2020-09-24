#!/bin/bash
#传入日期参数：yyyy-MM-dd
#脚本名称：run.sh
#脚本功能：非月底生成ok文件
#输入参数: yyyy-MM-dd eg: sh final_okfile.sh 2019-02-28
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
#卸数目录(使用:$target_path/$sys/$currdt)
target_path=$(cat ${configfile} | grep -i '^target_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
target_path=${target_path}/${sys}/${currdt}
#上传文件路径(处理完成的文件目录,使用:$put_path/$sys/$currdt)
put_path=$(cat ${configfile} | grep -i '^put_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
put_path=${put_path}/${sys}/${currdt}

mthdt=`date -d "${currdt}" +%m%d`
#当前日期下一天的天数
tomnum=`date -d "${currdt} tomorrow" +\%e`

#月末运行
if [ ${tomnum} = 1 ];then
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_plan ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_plan_$currdt.log
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_actual ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_actual_$currdt.log
	sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_month ${etldt} >> $log_path/pro_rpt_partner_fee_month_$currdt.log
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每月月末运行脚本运行完成" >> $log_path/run_$currdt.log
elif [ ${tomnum} = 2 ];then
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_plan ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_plan_$currdt.log
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_mid_ln_prdt_base_info_actual ${etldt} >> $log_path/pro_mid_ln_prdt_base_info_actual_$currdt.log
        sh ${etl_home}/src/util/call_pro_znbb.sh omi pro_rpt_partner_fee_month2 ${etldt} >> $log_path/pro_rpt_partner_fee_month2_$currdt.log
        echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]每月月初运行脚本运行完成" >> $log_path/run_$currdt.log
else
	echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]不是月末" >> $log_path/run_$currdt.log
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
rm -rf ${target_path}
rm -rf ${put_path}

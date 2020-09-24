#!/bin/bash
#传入日期参数：yyyyMMdd
#脚本名称：get_history_date.sh
#脚本功能：循环传入日期的上个月日期作为参数循环执行脚本
#输入参数: yyyyMMdd eg: sh run_last_mth.sh 20190201
#编写人  ：dk
#编写日期：20190928
#修改记录：
nowdate=`date -d"$1" +%Y%m01`
echo "nowdate:"$nowdate
#上个月的第一天
startdate=`date -d"$nowdate last month" +%Y%m%d`
echo "startdate:"$startdate
#上个月的最后一天
enddate=`date -d"$nowdate last day" +%Y%m%d`
echo "enddate:"$enddate

while (( $startdate <= $enddate ))
do
    re_date=`date -d"$startdate" +%Y-%m-%d`
    echo "re_date:"$re_date
    sh /etl/dw/src/load/run_inc.sh $re_date
    startdate=`date -d "+1 day $startdate" +%Y%m%d`
done 

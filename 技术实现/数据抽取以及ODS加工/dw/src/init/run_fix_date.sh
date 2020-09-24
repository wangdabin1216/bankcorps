#!/bin/bash
#脚本名称：run_fix_date.sh
#脚本功能：参数初始化脚本
#编写人  ：dk
#编写日期：20190928
#修改记录：
for date in 20200101 20200201 20200301
do
    echo $date
    sh /etl/dw/src/init/run_last_mth.sh $date
done

#for ti in 2019-12-01 2019-12-02 2019-12-03 2019-12-04 2019-12-05 2019-12-06 2019-12-07 2019-12-08 2019-12-09 2019-12-10 2019-12-11 2019-12-12 2019-12-13 2019-12-14
#do
#    echo $ti
#    sh /etl/dw/src/load/run_inc.sh $ti
#done

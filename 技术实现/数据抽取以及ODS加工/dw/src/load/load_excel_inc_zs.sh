#!/bin/bash
#传入日期参数：yyyy-MM-dd
#脚本名称：run_zs_inc.sh
#脚本功能：整体运行脚本
#输入参数: yyyy-MM-dd eg: sh run_zh_inc.sh 2020-02-19
#编写人  ：dk

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
#配置文件名称
configfile=${conf_path}/zs.conf
#源系统名称
sys=$(cat ${configfile} | grep -i '^sys' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
#卸数目录(使用:$target_path/$sys/$currdt)
target_path=$(cat ${configfile} | grep -i '^target_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
target_path=${target_path}/${sys}
#去重路径
distinct_path=${target_path}/distinct/
generate_path=${target_path}/generate_path/
#日志文件路径
log_path=${etl_home}/log/load/${sys}/${currdt}

zs_path=$(cat ${configfile} | grep -i '^zs_path' | cut -d = -f 2 | tr -d "[ ]" | sed 's/\r//')
controlpath=${zs_path}/controlpath
xlsxpath=${zs_path}/xlsx
csvpath=${zs_path}/csv
pytools=$etl_home/src/util

#如果没有日志目录就创建
if [ ! -e $log_path ];then
  mkdir -p $log_path
fi

echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]运行数据日期:$currdt" > $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]运行10位数据日期:$etldt" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]配置文件路径:$conf_path" >> $log_path/run_$currdt.log
echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]日志文件路径:$log_path" >> $log_path/run_$currdt.log


if [ ! -e ${zs_path}/${sys}.ok ];then
  echo `date +"%Y-%m-%d %H:%M:%S"`"[ERROR]ZS没有${zs_path}/${sys}.ok wating" >> ${log_path}/run_${currdt}.log
  #sleep 300
  exit 200
else
  if [ ! -e ${zs_path}/put.ok ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]没有${zs_path}/put.ok finish" >> ${log_path}/run_${currdt}.log
  else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]start python commond" >> $log_path/run_$currdt.log
    python $pytools/toTxtTools.py -p $controlpath -t 2 -s zs >> $log_path/run_$currdt.log
    python $pytools/toTxtTools.py -p $xlsxpath -t 1 -s zs >> $log_path/run_$currdt.log
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]end python commond" >> $log_path/run_$currdt.log

    rm -rf $target_path
    echo echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $target_path" >> $log_path/run_$currdt.log

    if [ ! -e $target_path ];then
      mkdir -p $target_path
    fi

    rm -rf $distinct_path
    echo echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $distinct_path" >> $log_path/run_$currdt.log

    if [ ! -e $distinct_path ];then
      mkdir -p $distinct_path
    fi

    rm -rf $generate_path
    echo echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]rm -rf $generate_path" >> $log_path/run_$currdt.log

    if [ ! -e $generate_path ];then
      mkdir -p $generate_path
    fi

    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]move *.txt and *.csv to $distinct_path" >> $log_path/run_$currdt.log
    mv $controlpath/*.txt $distinct_path
    mv $xlsxpath/*.txt $distinct_path
    mv $csvpath/*.csv $distinct_path

    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]start distinct python commond" >> $log_path/run_$currdt.log
    python $pytools/distinctData.py -inp ${distinct_path} -gp ${generate_path} -outp ${target_path} >> $log_path/run_$currdt.log
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]end python commond" >> $log_path/run_$currdt.log
    fi
fi

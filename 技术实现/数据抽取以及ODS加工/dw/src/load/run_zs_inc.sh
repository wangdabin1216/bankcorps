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
util_path=$etl_home/src/util


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
  #exit 200
else
  if [ ! -e ${zs_path}/put_${currdt}.ok ];then
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]没有${zs_path}/put_${currdt}.ok finish" >> ${log_path}/run_${currdt}.log
  else
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]start python commond" >> $log_path/run_$currdt.log
    python $util_path/toTxtTools.py -p $controlpath -t 2 -s zs >> $log_path/run_$currdt.log
    python $util_path/toTxtTools.py -p $xlsxpath -t 1 -s zs >> $log_path/run_$currdt.log
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
    cp $controlpath/*.txt $distinct_path
    cp $xlsxpath/*.txt $distinct_path
    cp $csvpath/*.csv $distinct_path

    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]start distinct python commond" >> $log_path/run_$currdt.log
    python $util_path/distinctData.py -inp ${distinct_path} -gp ${generate_path} -outp ${target_path} >> $log_path/run_$currdt.log
    echo `date +"%Y-%m-%d %H:%M:%S"`"[EXECUTE]end python commond" >> $log_path/run_$currdt.log
    
    #put文件
    cat ${conf_path}/table_zs.conf | while read line
    do
    table_name=`echo ${line} | awk -F, '{print $2}'`
    echo $table_name
    sh ${etl_home}/src/load/load_put_file_zs.sh ${table_name} ${etldt}
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]加载${table_name}完成" >> $log_path/run_$currdt.log
    done
    echo `date +"%Y-%m-%d %H:%M:%S"`"[INFO]put_zs脚本运行完成" >> $log_path/run_$currdt.log
    #调用存储过程
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYKZLJ_GD ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYKZLJ_DWTZ ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYKZLJ_SJKZR ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYZPZWXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_CPWS ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_BGXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_BGXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_YZWF ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_XZCFJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_QYDWTZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_QYDWTZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_QYYCML ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_FZJG ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_QYFDDBRDWTZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_FDDBRQTGSRZ ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_CCJC ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_SFXZJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_SFXZXQ ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_QSXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_SSGPJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_JBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_ZXXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_BDBZZQXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_DYWXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_DYQRXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_ZYGLRY ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_SXBZXRXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_BZXRXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_JYZX_JBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_GLSXBZXRXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_GLBZXRXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_GDJCZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_GQCZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_GQCZXX_ZXXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_XGXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_GQBGXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_QYNBJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_SHBXXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_RJCZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_WZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_NB_QYSJCZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_QYJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_QYCXSJ_DCDY_DJXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_QYZPJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_SSWF ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_TRZXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_ZLFLXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_ZLJBXX ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_ZPZZQ ${etldt}
    sh ${util_path}/call_pro_zs.sh omi pkg_zs_util ZS_ZTMKSJ_FYGG ${etldt}
    #清理源数据
    #rm -rf $controlpath/*
    #rm -rf $xlsxpath/*
    #rm -rf $csvpath/*
    #rm -rf ${zs_path}/${sys}.ok
    fi
fi

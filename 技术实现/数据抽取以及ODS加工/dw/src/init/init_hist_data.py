#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
脚本名称：init_hist_data.py
脚本功能：读取mysql数据表meta元数据写入存储过程所依赖的cc.dw_sm_hist表数据
输入参数：schema schema 表名 eg: python init_hist_data.py tbo omi mb_bank 0
编写人：  guxn
编写日期：20191113
修改记录：
by guxn 20191113   1.创建init_insert_hist.py
'''

import os
import sys
import setting
import operate_os
import operate_log

class Init_hist_data():
  def __init__(self,logger,opos):
    self.logger =logger
    self.opos=opos

  def gen_load_insert(self,src_schema,tar_schema,system,table,file_name):
    #从sqlite数据库导出数据到文件中
    try:
    	#判断tab输入参数是否为1
    	if table=='1':
    		tab_txt=" and 1=1"
    	else:
    		tab_txt=" and lower(t.tab_name)='"+table+"'"
    	#查询sqlite数据库sql语句
    	sql="set session group_concat_max_len=102400000;select concat('"+src_schema+".',lower(t.sys),'_',lower(t.tab_name)),lower(t.sys),concat('"+tar_schema+".',lower(t.sys),'_',lower(t.tab_name),'_hs'),lower(t1.FIELDS),lower(coalesce(t2.pk,'')),lower(coalesce(t.partition_set,'yyyymm')),lower(coalesce(t.chain_type,'chain_all')),coalesce(t.cond,'none') ,'2' \
    	from etl_load_table t \
    	left join (select sys, tab_name,group_concat(col_name) AS fields from etl_load_column  group by sys,tab_name order by col_seq) t1  \
    	on t.tab_name = t1.tab_name AND t.sys=t1.sys \
    	left join (select sys, tab_name,group_concat(col_name) AS pk from etl_load_column WHERE is_pk = 1 group by sys,tab_name order by col_seq) t2 \
    	on t.sys=t2.sys AND t.tab_name=t2.tab_name \
    	where lower(t.sys)='"+system+"'"+tab_txt+"; "
    	#print("aaaa",sql)
    	sqlite_cmd=setting.MYSQL_ULD_CMD +" \"  "+sql+" \">>"+file_name
    	replace_null_cmd="perl -p -i -e 's/\t/\|/g;s/[ ]*\|/\|/g;s/\|\|/\|\\\\N\|/g;s/\|\|/\|\\\\N\|/g;s/NULL/\\\\N/g' "+file_name+" >/dev/null 2>&1"
        #执行命令
    	self.opos.exec_cmd(sqlite_cmd)
    	self.opos.exec_cmd(replace_null_cmd)
        
    	self.logger.info("写入dw_sm_hist_ext.txt文件成功")
    except BaseException as err:
    	self.logger.error("写入dw_sm_hist_ext.txt文件错误 "+str(table)+","+str(err.args))


  def gen_trans_insert(self,src_schema,tar_schema,system,table,file_name):
    #从sqlite数据库导出数据到文件中
    try:
    	#判断tab输入参数是否为1
    	if table=='1':
    		tab_txt=" and 1=1"
    	else:
    		tab_txt=" and lower(t.tab_name)='"+table+"'"
    	#查询sqlite数据库sql语句
    	sql="set session group_concat_max_len=102400000;select concat('"+src_schema+".',lower(t.tab_name)),lower(t.sys),concat('"+tar_schema+".',lower(t.tab_name),'_hs'),lower(t1.FIELDS),lower(coalesce(t2.pk,'')),lower(coalesce(t.partition_set,'yyyymm')),lower(coalesce(t.chain_type,'chain_all')),'none','2' \
    	from etl_trans_table t \
    	left join (select sys, tab_name,group_concat(col_name) AS fields from etl_trans_column where upper(col_name)<>'ETL_DATE'  group by sys,tab_name order by col_seq) t1  \
    	on t.tab_name = t1.tab_name AND t.sys=t1.sys \
    	left join (select sys, tab_name,group_concat(col_name) AS pk from etl_trans_column WHERE is_pk = 1 group by sys,tab_name order by col_seq) t2 \
    	on t.sys=t2.sys AND t.tab_name=t2.tab_name \
    	where lower(t.sys)='"+system+"'"+tab_txt+"; "
    	
    	sqlite_cmd=setting.MYSQL_ULD_CMD +" \"  "+sql+" \">>"+file_name
    	replace_null_cmd="perl -p -i -e 's/\t/\|/g;s/[ ]*\|/\|/g;s/\|\|/\|\\\\N\|/g;s/\|\|/\|\\\\N\|/g;s/NULL/\\\\N/g' "+file_name+" >/dev/null 2>&1"
        #执行命令
    	self.opos.exec_cmd(sqlite_cmd)
    	self.opos.exec_cmd(replace_null_cmd)
        
    	self.logger.info("写入dw_sm_hist_ext.txt文件成功")
    except BaseException as err:
    	self.logger.error("写入dw_sm_hist_ext.txt文件错误 "+str(table)+","+str(err.args))


  def init_hist_data(self,src_schema,tar_schema,tab_name):
    '''
    根据传入源系统schema调度不同方法执行数据导出
    导出数据方法和执行方法需要分开，有多表循环
    src_schema --源表schema
    tar_schema --目标表schema
    tab_name  --同步的表名
    '''
    self.logger.debug("调用方法-init_hist_data")
    system=tab_name[:tab_name.find('_')]
    table=tab_name[tab_name.find('_')+1:]
    file_name=setting.DATA_FILE+'dw_sm_hist_ext.txt'
    if src_schema =='tbo':
        self.gen_load_insert(src_schema,tar_schema,system,table,file_name)
        print(tab_name,"写入历史存储配置信息txt文件成功")
    else:
        self.gen_trans_insert(src_schema,tar_schema,tar_schema,tab_name,file_name)
        print(tab_name,"写入历史存储配置信息txt文件成功")
    self.logger.info("执行Main方法成功")


  def init_exec_pro(self):
    '''调用call_pro_sync_cc_data同步外表数据到hbase表'''
    self.logger.debug("调用方法-init_exec_pro")
    #外表名
    ext_tab="dw_sm_hist_ext"
    #hbase表名
    hbase_tab="dw_sm_hist"
    #文件名
    file_name=setting.DATA_FILE+'dw_sm_hist_ext.txt'
    #执行命令
    rm_file_cmd="hdfs dfs -rm /dw/cc/"+ext_tab+"/*"
    put_file_cmd="hdfs dfs -put "+file_name+" /dw/cc/"+ext_tab+"/"+ext_tab+".txt"
    call_pro_cmd = setting.BEELINE_CMD + " -e \" begin cc.pkg_dw_util.pro_sync_cc_data('"+ext_tab+"','"+hbase_tab+ "') end \" "
    #执行kinit命令
    self.opos.exec_cmd(setting.DW_KINIT_CMD)
    #执行删除hdfs文件命令
    self.opos.exec_cmd(rm_file_cmd)
    #执行put命令
    self.opos.exec_cmd(put_file_cmd)
    #调用存储过程
    self.opos.exec_cmd(call_pro_cmd)


  def main(self):
    #检查输入参数个数
    if len(sys.argv)<5:
        infostr = "Usage: %s src_schema tar_schema tab_name is_exec " %sys.argv[0]
        print("输入参数个数有误，请检查.\n"+infostr)
        sys.exit(-1)
    print( "参数个数正确，开始元表结构初始化.详细日志查询init.log ")
    #输入脚本参数  脚本多一个参数
    src_schema,tar_schema,tar_tab,is_exec =  sys.argv[1:]
    file_name=setting.DATA_FILE+'dw_sm_hist_ext.txt'
    #删除已存在文件
    self.opos.delete_file(file_name)
    #导出数据
    self.init_hist_data(src_schema,tar_schema,tar_tab)
    ext_tab="dw_sm_hist_ext"
    hbase_tab="dw_sm_hist"
    self.logger.info("执行Main方法成功")
    #选择是否执行
    if int(is_exec) == 1:
        #执行命令
        rm_file_cmd="hdfs dfs -rm /dw/cc/"+ext_tab+"/*"
        put_file_cmd="hdfs dfs -put "+file_name+" /dw/cc/"+ext_tab+"/"+ext_tab+".txt"
        call_pro_cmd = setting.BEELINE_CMD + " -e \" begin cc.pkg_dw_util.pro_sync_cc_data('"+ext_tab+"','"+hbase_tab+ "') end \" "
        #执行kinit命令
        self.opos.exec_cmd(setting.DW_KINIT_CMD)
        #执行删除hdfs文件命令
        self.opos.exec_cmd(rm_file_cmd)
        #执行put命令
        self.opos.exec_cmd(put_file_cmd)
        #调用存储过程
        self.opos.exec_cmd(call_pro_cmd)
    else:
        pass
    sys.exit(0)

if __name__ == '__main__':
    logger=operate_log.getLogger("/Users/wangdabin1216/git/dw/log/init/init_hist_data.log")
    ihd=Init_hist_data(logger,operate_os.Operate_os(logger))
    ihd.main()


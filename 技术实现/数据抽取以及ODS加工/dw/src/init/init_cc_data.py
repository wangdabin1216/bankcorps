#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
 脚本名称：init_cc_data.py
 脚本功能：读取mysql数据表meta元数据写入oracle数据库表
 输入参数： 表名 eg: python init_cc_data.py etl_serve_unload dw_sm_unload_ext 1
 编写人：  guxn
 编写日期：20191113
 修改记录：
 by guxn 20191113   1.创建init_cc_data.py
'''

import os
import sys
import setting
import operate_mysql
import operate_os
import operate_log

class Init_cc_data:
  def __init__(self,logger,opos,db):
    self.logger = logger
    self.opos=opos
    self.db = db

  def get_cols_info(self,src_tab):
    '''从etl_meta_tables 获取表的字段信息'''
    self.logger.debug("调用方法-get_cols_info")
    try:
        #字段子句序列
        cols_info=""
        #获取字段信息
        sql="select group_concat(col_name) from (select col_name from etl_meta_tables where lower(tab_name)='"+src_tab.lower()+"' order by col_seq) a;"
        cu = self.db.search(sql)
        if cu == None :
            raise  Exception("未获取到表"+src_tab+"的字段配置信息，请检查参数")
        else:
            pass
        #逻辑判断统一处理，有异常直接报错
        for row in cu:
            #字段不能为空;字段类型不能为空
            if len(str(row[0]))>0 and str(row[0]) !="None":
                cols_info=str(row[0]).strip()
            else:
                raise Exception("表"+src_tab+"的字段信息配置部分有误，请检查修改")
        self.logger.debug("获取表的字段信息,字段子句成功:"+str(cols_info))
    except Exception as err:
        self.logger.error("获取表的字段信息,字段子句时错误"+str(src_tab)+","+str(err.args))
        sys.exit(-1)
    #finally:
        #self.db.close_mysql()
    return cols_info

  def get_tab_data(self,src_tab,file_name):
    '''导出sqlite表数据导出到文件''' 
    self.logger.debug("调用方法-gen_insert_sql")
    try:
        sql_list=[]
        column=self.get_cols_info(src_tab)
        sqlite_cmd=setting.MYSQL_ULD_CMD +" \" select "+column+" from "+src_tab+" \">"+file_name
        replace_null_cmd="perl -p -i -e 's/\t/\|/g;s/[ ]*\|/\|/g;s/\|\|/\|\\\\N\|/g;s/\|\|/\|\\\\N\|/g;s/NULL/\\\\N/g' "+file_name+" >/dev/null 2>&1"
        #执行命令
        self.opos.exec_cmd(sqlite_cmd)
        self.opos.exec_cmd(replace_null_cmd)
        self.logger.info("导出数据文件成功:"+file_name)
    except Exception as err:
        self.logger.error("导出数据文件失败"+str(err.args))
        sys.exit(-1)


  def init_cc_data(self,src_tab,ext_tab):
    '''
    init入口程序调用主方法,导出数据，sqlite表导出数据生成inceptor外表txt文件,调用pro_sync_cc_data存储过程同步外表数据到hbase表
    输入参数为
    src_tab --sqlite表
    ext_tab --inceptor外表
    '''
    self.logger.debug("调用方法-init_cc_data")
    try:
        #data文件名称
        file_name = setting.DATA_FILE+ext_tab.lower()+".txt"
        #删除已存在文件
        self.opos.delete_file(file_name)
        #将数据写入文件
        self.get_tab_data(src_tab,file_name)
        #生成hbase表名
        hbase_tab=ext_tab[:-4]
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
    except Exception as err:
        self.logger.error("put或调用cc.pkg_dw_util.pro_sync_cc_data错误："+str(err.args))
        sys.exit(-1)

  def main(self):
    #检查输入参数个数
    if len(sys.argv)<4:
        infostr = "Usage: %s source_table ext_table is_exec " %sys.argv[0]
        print("输入参数个数有误，请检查.\n"+infostr)
        sys.exit(-1)
    print( "参数个数正确，开始元表结构初始化.详细日志查询init.log ")
    #输入脚本参数  脚本多一个参数  
    src_tab,ext_tab,is_exec =  sys.argv[1:]
    file_name = setting.DATA_FILE+ext_tab.lower()+".txt"
    self.opos.delete_file(file_name)
    self.get_tab_data(src_tab,file_name)
    self.logger.info("执行Main方法成功")
    #选择是否执行
    if int(is_exec) == 1:
        self.init_cc_data(src_tab,ext_tab)
        self.logger.info("调用init_cc_data方法成功")
    else:
        pass
    sys.exit(0)

if __name__ == '__main__':
    logger=operate_log.getLogger("/Users/wangdabin1216/git/dw/log/init/init_cc_data.log")
    icd=Init_cc_data(logger,operate_os.Operate_os(logger),operate_mysql.Operate_mysql(logger))
    icd.main()
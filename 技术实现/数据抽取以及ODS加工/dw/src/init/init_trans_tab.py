#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
 脚本名称：init_trans_tab.py
 脚本功能：读取mysql数据表meta元数据生成DDL语句
 输入参数：层级 类型 表名 eg: python init_trans_tab.py fmi orchs f_cs_pr_elec_cntct 0
 编写人：  guxn
 编写日期：20191114
 修改记录：
 by guxn 20191114   1.创建init_gen_trans_ddl.py
'''

import os
import sys
import setting
import importlib
import operate_mysql
import operate_os
import operate_log

class Init_trans_tab:
  def __init__(self,logger,opos,db):
    self.logger =logger
    self.opos=opos
    self.db = db

  def get_cols_info(self,schema_name,tab_name):
    '''
    从etl_trans_column获取表的字段配置信息，包括：
    字段及字段注释、映射后的字段类型,组成字段子句；
    主键字段，组成主键子句；
    分桶字段，组成分桶子句；
    '''
    self.logger.debug("调用方法-get_cols_info")
    try:
        #字段子句序列
        cols_list=[]
        #主键字段序列
        pk_list=[]
        #分桶字段名称
        dk_col_name=""
        #字段信息组
        col_info=dict()
        #获取字段信息，包括 0 字段名称、1 字段类型（映射后）、2 字段描述、3 主键、4 是否为空、5 默认值、6 分桶键
        sql="select col_name,mapping_col_type,col_cn_name,is_pk,is_not_null,default_val,is_dk from etl_trans_column where lower(sys)='"+schema_name.lower()+"' and lower(tab_name)='"+tab_name.lower()+"'  and upper(col_name)<>'ETL_DATE'  order by cast(col_seq as signed);"
        cu = self.db.search(sql)
        if cu == None :
            raise  Exception("未获取到表"+tab+"的字段配置信息，请检查参数")
        else:
            pass
        #逻辑判断统一处理，有异常直接报错
        for row in cu:
            #字段不能为空;字段类型不能为空
            if len(str(row[0]))>0 and str(row[0]) !="None" and len(str(row[1]))>0 and str(row[1]) !="None":
                col_info['col_name']=str(row[0]).strip()
                col_info['mapping_col_type']=str(row[1]).strip()
                col_info['col_cn_name']=str(row[2]).strip()
                col_info['is_pk']=str(row[3]).strip()
                col_info['is_not_null']=str(row[4]).strip()
                col_info['default_val']=str(row[5]).strip()
                col_info['is_dk']=str(row[6]).strip()
            else:
                raise Exception("表"+tab+"的字段信息配置部分有误，请检查修改")
            #字段子句
            if col_info['is_not_null'] == '1':
                cols_list.append(col_info['col_name']+" "+col_info['mapping_col_type']+" not null comment \'"+col_info['col_cn_name']+"\'")
            else:
                cols_list.append(col_info['col_name']+" "+col_info['mapping_col_type']+" comment \'"+col_info['col_cn_name']+"\'")
            #主键序列获取（为支持hbase表创建做准备）
            if col_info['is_pk'] == '1':
                pk_list.append(col_info['col_name'])
            else:
                pass
            #分桶字段获取
            if col_info['is_dk'] == '1':
                dk_col_name=col_info['col_name']
            else:
                pass 
        #字段语句拼接
        cols_txt = ",\n".join(cols_list)
        #主键序列拼接
        pk_txt = ",".join(pk_list)
        #分桶子句拼接
        dk_txt = "CLUSTERED BY ("+dk_col_name+")"
        #组合字典
        cols_info={"cols_txt":cols_txt,"pk_txt":pk_txt,"dk_txt":dk_txt}
        self.logger.debug("获取表的字段信息,字段子句成功:"+str(cols_info))
    except Exception as err:
        self.logger.error("获取表的字段信息,字段子句时错误"+str(tab_name)+","+str(err.args))
        sys.exit(-1)
    return cols_info

  def get_tab_info(self,schema_name,tab_name):
    '''
    从etl_trans_table 获取表的信息，包括：
    表中文名称、表类型等信息
    返回值为一个字典类型
    '''
    self.logger.debug("调用方法-get_tab_info")
    try:
        tab_info=dict()
        tab_info['tab_name']=tab_name
        sql="select tab_cn_name,tab_type,11 as dk_num from etl_trans_table where lower(sys)='"+schema_name.lower()+"' and lower(tab_name)='"+tab_name.lower()+"';"
        cu = self.db.search(sql)
        if cu == None:
            raise  Exception("未获取到表"+tab+"的配置信息，请检查参数")
        else:
            pass
        #不合规的判断整合在一起判断
        for row in cu:
            if len(str(row[1]))>0 and str(row[1]) !="None":
                tab_info['tab_cn_name']=str(row[0]).strip()
                tab_info['dk_num']=(str(row[2]).strip() if (int(row[2])>0) else 11)
            else:
                raise Exception("表"+tab+"的表信息配置部分有误（表描述字段)，请检查修改")
        self.logger.debug("表"+str(tab_name)+"信息获取完成："+str(tab_info))
    except Exception as err:
        self.logger.error("表"+str(tab_name)+"信息获取异常,"+str(err.args))
        sys.exit(-1)
    return tab_info


  def assemble_sql(self,schema_name,tab_type,tab_name):
    '''
    根据配置生成相应的表结构编译sql，
    输入参数：模型层级；表名；表类型
    返回建表语句sql
    '''
    self.logger.debug("调用方法-assemble_sql")
    #获取表信息
    tab_info = self.get_tab_info(schema_name,tab_name)
    #获取字段信息
    cols_info = self.get_cols_info(schema_name,tab_name)
    #带层级表名赋值
    tab_name = schema_name+"."+tab_name
    #删除操作
    drop_sql = "DROP TABLE IF EXISTS "+tab_name+";\n"
    #中间字段子句均一致
    sql_middle = cols_info['cols_txt']
    #print("sql_middle1",sql_middle)
    #根据不同类型处理
    if tab_type == 'EXT':
        self.logger.debug("执行EXT类型拼装.")
        #分段组装
        sql_head = "CREATE EXTERNAL TABLE "+tab_name+"( \n"
        sql_tail = ") \nCOMMENT \'"+tab_info['tab_cn_name']+"\' \n" \
        +"ROW FORMAT DELIMITED FIELDS TERMINATED BY \'"+tab_info['col_separator'] +"\' \n"  \
        +"STORED AS TEXTFILE LOCATION \'"+tab_info['trans_path']+"\';"
        self.logger.debug("外表"+tab_name+"的语句拼接完成.")
    elif tab_type == 'ORC':
        self.logger.debug("执行ORC类型拼装.")
        sql_head = "CREATE TABLE "+tab_name+"( \n ETL_DATE DATE comment \'数据日期\' , \n"
        sql_tail = ") \nCOMMENT \'"+tab_info['tab_cn_name'] +"\' \n" \
        +cols_info['dk_txt']+" INTO "+tab_info['dk_num']+" BUCKETS \n" \
        +"stored as orc \nTBLPROPERTIES (\'transactional\'=\'true\');"
        self.logger.debug("orc表"+tab_name+"的结构语句拼接完成.")
    elif tab_type == 'ORCHS':
        self.logger.debug("执行ORCHS类型拼装.")
        #历史表，表名增加_hs后缀
        tab_name = tab_name+"_HS"
        #删除操作
        drop_sql = "DROP TABLE IF EXISTS "+tab_name+";\n"
        self.logger.debug(tab_name)
        sql_head="CREATE TABLE "+tab_name+" ( \n BEGNDT DATE comment \'生效日期\',\nOVERDT DATE comment \'失效日期\' , \n"
        self.logger.debug(sql_head)
        sql_tail=") \nCOMMENT \'"+tab_info['tab_cn_name'] +"\' \n" \
        +"partitioned by (partid string) \n" \
        +cols_info['dk_txt']+" INTO "+tab_info['dk_num']+" BUCKETS \n" \
        +"stored as orc \nTBLPROPERTIES (\'transactional\'=\'true\');"
        self.logger.debug(sql_tail)
        self.logger.debug("orc历史表"+tab_name+"的结构语句拼接完成.")
    #有需要再开发
    elif tab_type == 'HBASE':
        sql_head= "none"
        sql_tail= "none"
    else:
        self.logger.error("tab_type类型值输入错误,tab_type="+tab_type)
        sys.exit(-1)
    sql=drop_sql+sql_head+sql_middle+sql_tail 
    self.logger.info("表"+tab_name+"的结构语句组装完成.")
    return sql  


  def init_trans_tab(self,schema_name,tab_type,tab_name=""):
    '''
    init入口程序调用主方法
    输入参数为 schema名称 类型
    生成sql文件，以及实现在inceptor建表
    '''
    self.logger.debug("调用方法-init_load_tab")
    schema_name=schema_name.upper()
    tab_type =tab_type.upper()
    #load_ddl文件名称
    if tab_name == "":
        file_name = setting.SQL_PATH+"load_"+schema_name.lower()+"_"+tab_type.lower()+"_tab.sql"
    else:
        file_name = setting.SQL_PATH+"load_"+schema_name.lower()+"_"+tab_type.lower()+"_"+tab_name.lower()+"_tab.sql"
    #删除已存在文件
    self.opos.delete_file(file_name)

    try:
        #获取本次需要创建的表，表级调用直接将表名输入到列表中
        tab_list=[]
        if tab_name == "":
            sql = "select tab_name from etl_trans_table where is_create=1 and is_valid=1;"
            result = self.db.search(sql)
            for row in result:
                tab_list.append(str(row[0]).upper())
        else:
            tab_list.append(tab_name.upper())
        if len(tab_list) <1:
            raise Exception("获取到的本次待创建表个数为0")
        else:
            pass
        print("本次共创建"+str(len(tab_list))+"张表.")
        self.logger.info("获取本次待创建的表列表如下："+str(tab_list)+",共"+str(len(tab_list))+"张表.")
    except Exception as err:
        self.logger.error("获取本次待创建的表列表时出现错误,错误信息："+str(err.args))
        sys.exit(-1)
    try:
        #获取建表语句sql序列
        sql_list=[]
        for tab_name in tab_list:
            #获取建表语句sql,添加到sql序列
            sql = self.assemble_sql(schema_name,tab_type,tab_name)
            sql ="----TABLE:"+tab_name.upper()+"---------\n"+sql+'\n\n'
            sql_list.append(sql)
        self.logger.debug(str(sql_list))
    except Exception as err:
        self.logger.error("获取建表语句sql序列时出现错误,错误信息："+str(err.args))
        sys.exit(-1)
    #finally:
        #self.db.close_mysql()
    try:
        #将所有sql写入文件
        with open(file_name,'a+') as f:
            for sql in sql_list:
                f.write(sql)
        self.logger.info("写入完成,所有sql已写入文件:"+file_name)
    except Exception as err:
        self.logger.error("将所有sql写入文件时出现错误,错误信息："+str(err.args))
        sys.exit(-1)
    return file_name

  def main(self):
    #检查输入参数个数
    if len(sys.argv)<4:
        infostr = "Usage: %s schema_name tab_type tab_name is_exec " %sys.argv[0]
        print("输入参数个数有误，请检查.\n"+infostr)
        sys.exit(-1)
    print( "参数个数正确，开始元表结构初始化.详细日志查询init.log ")
    #输入脚本参数  脚本多一个参数  
    schema_name,tab_type,tab_name,is_exec =  sys.argv[1:]
    #创建生成文本
    sql_file = self.init_trans_tab(schema_name,tab_type,tab_name)
    #选择是否执行
    if int(is_exec) == 1:
        re = self.opos.compile_inceptor_file(sql_file)
        if re == 1 :
            print("库表创建编译已完成，请检查执行结果.")
        else:
            print("文件编译出现错误.")
    else:
        pass
    sys.exit(0)

if __name__ == '__main__':
    logger=operate_log.getLogger("/Users/wangdabin1216/git/dw/log/init/init_trans_tab.log")
    itt=Init_trans_tab(logger,operate_os.Operate_os(logger),operate_mysql.Operate_mysql(logger))
    itt.main()

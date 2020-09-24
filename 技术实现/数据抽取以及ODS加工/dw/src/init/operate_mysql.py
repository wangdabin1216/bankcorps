# -*- coding: UTF-8 -*-
'''
脚本名称：operate_mysql.py
脚本功能：mysql数据库操作类及常用方法
输入参数：无
编写人：  guxn  
编写日期：20191107
修改记录：
by guxn  20191107  新建
'''

import os
import sys
import pymysql
import importlib
import operate_getcfg

class Operate_mysql:
  def __init__(self,logger):
    self.logger = logger
    self.conn_mysql()

  def conn_mysql(self):
    """连接数据库"""
    getcfgInfo = operate_getcfg.Getcfginfo()
    self.get_db = getcfgInfo.getEtldb()
    try:
      self.conn = pymysql.connect(host=self.get_db[0], user=self.get_db[2], password=self.get_db[3], db=self.get_db[4])
      self.cur = self.conn.cursor()
      self.logger.info("数据库初始化连接创建成功.")
    except Exception as err:
      self.conn = None 
      self.logger.error("数据库初始化连接创建失败"+str(err.args))
      sys.exit(-1)
      print(str(err.args))

  def execute_sql(self,sql):
    """执行事务类SQL"""
    if self.cur is not None:
      try:
        self.cur.execute(sql)
        self.logger.info("执行事务类SQL成功,sql："+sql+".")
      except Exception as err:
        self.conn.rollback()
        self.logger.error("执行事务类SQL失败,sql："+sql+".\n"+str(err.args))
        print(str(err.args))
        sys.exit(-1)

  def search(self,sql):
    """执行查询sql"""
    if self.cur is not None:
      try:
        self.cur.execute(sql)
        self.logger.debug("查询sql成功,sql："+sql+".")
      except Exception as err:
        self.logger.error("查询sql失败,sql："+sql+".\n"+str(err.args))
        print(str(err.args))
        sys.exit(-1)
    return self.cur.fetchall()

  def commit(self):
    '''提交事务'''    
    if self.conn is not None:
      try:
        self.conn.commit()
        self.logger.debug("提交事务类操作成功.")
      except Exception as err:
        print(str(err.args)) 
        self.logger.error("提交事务类操作失败"+str(err.args))
        sys.exit(-1)
    return 0

  def close_mysql(self):
    """关闭数据库连接"""
    self.cur.close()
    self.conn.close()
    self.logger.info("数据库关闭成功.")


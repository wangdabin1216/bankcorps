# -*- coding: utf-8 -*-
'''
 脚本名称：operate_os.py
 脚本功能：执行操作系统命令、文件处理的通用方法
 编写人：  guxn
 编写日期：20191107
 修改记录： 
'''
import os 
import sys
import shutil
import time 
import setting
import importlib

class Operate_os:
  def __init__(self,logger):
    self.logger =logger
	
  def delete_file(self,file_name):
    '''文件存在时，删除文件'''
    try:
    	if os.path.isfile(file_name):
            #bak_file(file_name)
            os.remove(file_name)
            self.logger.debug("文件"+str(file_name)+"已删除")
    except Exception as err:
        self.logger.error("文件"+str(file_name)+"删除时发生异常,"+str(err.args))
        sys.exit(-1)

  def write_file(self,file_name,info_text):
    '''信息追加写入文件'''
    try:
        with open(file_name, 'a+') as f:
            f.write(info_text)
        self.logger.debug("信息：'"+info_text+"',已写入文件"+file_name)
    except Exception as err:
        self.logger.error("写入信息'"+str(info_text)+"' 时发生异常,"+str(err.args))
        sys.exit(-1)

  def bak_file(self,file_name):
    '''文件按照服务器时间作为结尾备份操作'''
    #数据库文件备份
    try:
        if os.path.isfile(file_name):
            bak_file = file_name+".bak_"+time.strftime('%Y%m%d_%H%M%S',time.localtime(time.time()))
            shutil.copyfile(file_name,bak_file)
            self.logger.debug("文件-"+file_name+"备份成功，备份文件为："+bak_file)
        else:
            raise Exception
    except Exception as err:
        self.logger.error("文件-"+file_name+"备份失败，错误信息："+str(err))
        return -1
    return 1

  def exec_cmd(self,cmd):
    '''执行shell命令,不获取输出结果 '''
    try:
        os.system(cmd)
        self.logger.debug("系统命令-"+cmd+"执行成功")
    except Exception as err:
        self.logger.error("系统命令-"+cmd+"执行失败，错误信息："+str(err))
        return -1
    return 1

  def compile_inceptor_file(self,sql_file):
    '''使用beeline命令执行sql文件的编译执行'''
    try:
        print("库表创建编译已完成，请检查执行结果.")
        cmd = setting.BEELINE_CMD + "-f \""+sql_file+"\""
        os.system(cmd)
        self.logger.debug("文件-"+sql_file+"编译执行成功")
    except Exception as err:
        self.logger.error("文件-"+sql_file+"编译执行失败，错误信息："+str(err))
        return -1
    return 1
	
  def compile_mysql_file(self,sql_file):
    '''使用beeline命令执行sql文件的编译执行'''
    try:
        cmd = setting.MYSQL_CMD + "< \""+sql_file+"\""
        os.system(cmd)
        self.logger.debug("文件-"+sql_file+"编译执行成功")
    except Exception as err:
        self.logger.error("文件-"+sql_file+"编译执行失败，错误信息："+str(err))
        return -1
    return 1
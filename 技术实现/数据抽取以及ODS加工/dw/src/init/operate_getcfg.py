# -*- coding: UTF-8 -*-
'''
脚本名称：operate_getcfg.py
脚本功能：获取配置文件信息类
输入参数：无
编写人：  guxn  
编写日期：20191107
修改记录：
by guxn  20191107  新建
'''

import configparser
import os,sys

#定义获取配置文件信息类


class Getcfginfo:
    #获取python脚本所在的绝对路径
    basePath=os.path.split(os.path.realpath(__file__))[0]
    config = configparser.ConfigParser()
    cfgPath = basePath+'/../../conf/config.ini'
    if os.path.exists(cfgPath):pass
    else:print('no config.ini is found'), sys.exit(1)

    def __init__(self):
        pass

    # DISP知识库读取函数
    def getDispdb(self):
        self.config.read(self.cfgPath)
        # 读取配置文件获取disp知识库数据库信息
        dispinfo = dict(self.config.items(section='disp'))
        # 返回mysql 数据库连接数组
        dispLink=(dispinfo["host_name"],int(dispinfo["port_num"]),dispinfo["user_name"],dispinfo["pass_word"]
                  ,dispinfo["db_name"])
        return dispLink

    #ETL知识库读取函数
    def getEtldb(self):
        self.config.read(self.cfgPath)
        #读取配置文件获取ETL知识库数据库信息
        edbinfo = dict(self.config.items(section='etl'))
        #返回mysql数据库连接字数组
        etlLink=(edbinfo["host_name"],int(edbinfo["port_num"]),edbinfo["user_name"],edbinfo["pass_word"]
                 ,edbinfo["db_name"])
        return etlLink

    #获取配置文件路径
    def getFilepath(self,pathName):
        self.config.read(self.cfgPath)
        #读取配置文件文件路径信息并返回
        pathInfo=dict(self.config.items(section='filepath'))
        return pathInfo[pathName]

    #获取hive连接
    def getHivinfo(self):
        self.config.read(self.cfgPath)
        hdbinfo=dict(self.config.items(section='hive'))
        #返回hive连接信息
        return hdbinfo

    #获取hdfs连接信息
    def getHdfsinfo(self):
        self.config.read(self.cfgPath)
        hdfsinfo = dict(self.config.items(section='hdfs'))
        #返回hdfs连接信息
        return hdfsinfo

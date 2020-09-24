#!/usr/bin/python
#! encoding=utf-8
'''
 脚本名称：operate_log.py
 脚本功能：记录日志通用类
 输入参数：无
 编写人：  guxn
 编写日期：20191107
 修改记录：
 by guxn 20191107  1.新建
'''
import logging
import time
import os

class MyFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        self.filename =  filename
        self.mode = mode
        self.encoding = encoding
        self.delay = delay
        self.baseFilename = os.path.abspath(self.filename)
        self.__createFile()
        super(MyFileHandler, self).__init__(self.filename, mode, encoding, delay)
        
    def close(self):
        logging.FileHandler.close(self)
        
    def _open(self):
        return logging.FileHandler._open(self)
        
    def __createFile(self):
        if not os.path.exists(self.filename):
            with open(self.filename, "w") as fout:
                pass

class Logger():
    def __init__(self, logname):
        '''
           指定保存日志的文件
           将日志存入到该文件中
        '''
        # 创建一个logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        # 创建一个handler，用于写入日志文件
        #fh = logging.FileHandler(logname)
        fh = MyFileHandler(logname)
        fh.setLevel(logging.DEBUG)
        # 定义handler的输出格式
        string = '[%(levelname)s %(asctime)s %(process)d %(pathname)s:%(lineno)d %(funcName)s] %(message)s'
        formatter = logging.Formatter(string)
        fh.setFormatter(formatter)
        # 给logger添加handler
        self.logger.addHandler(fh)
    def createLogger(self):
        return self.logger

def getLogger(logname):
    logger = Logger(logname)
    return logger.createLogger()

# -*- coding: utf-8 -*-
'''
 脚本名称：setting.py
 脚本功能：ETL程序常量组及常规日志参数配置
 输入参数:  
 编写人：  guxn
 编写日期：20191107
 修改记录：
 by guxn  20191107 新建
'''
import logging


#设置初始程序路径
BASE_PATH = '/Users/wangdabin1216/git/dw'
# 元数据库文件路径
META_FILE = BASE_PATH+'/conf/etl.db'
# 初始化日志文件
INIT_LOG = BASE_PATH + '/log/init/init2.log'
# sql文件路径
SQL_PATH = BASE_PATH + '/sql/'
# template文件路径
TEMPLATE_PATH = BASE_PATH + '/template/'
# 日志内容格式
LOG_FORMAT = "[%(asctime)s]-[%(levelname)s]-[%(filename)s %(funcName)s %(lineno)s]-%(message)s"
# 日志时间格式
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
# 统一日志等级
LOG_LEVEL = logging.DEBUG
# CC库配置表数据文件存储目录
DATA_FILE = '/data/put/init/'

# BEELINE字符串
BEELINE_CMD = "beeline -u \"jdbc:hive2://bjhqtdh04:10000/cc;\" -n dw -p dw  --outputformat=csv --silent=true --showHeader=true "
#KINIT 字符串
DW_KINIT_CMD = "kinit dw -kt "+BASE_PATH+"/conf/dw.keytab"
#mysql
MYSQL_CMD="mysql -u root -ptoor -h 10.211.55.4 -D etl"
MYSQL_ULD_CMD="mysql -u root -ptoor -h 10.211.55.4 -D etl --default-character-set=utf8 -N -e"
MYSQL_DISP_CMD="mysql -u root -ptoor -h 10.211.55.4 -D disp --default-character-set=utf8 -N -e"
# META模块表清单
META_TAB_LIST = ['etl_meta_tables', 'etl_meta_args', 'etl_meta_type_convert', 'etl_load_system']
# LOAD模块表清单
LOAD_TAB_LIST = ['etl_load_table', 'etl_load_column']
# TRANS模块表清单
TRANS_TAB_LIST = ['etl_trans_desc', 'etl_trans_table', 'etl_trans_column', 'etl_trans_mapping']
# SCHE模块表清单
SCHE_TAB_LIST = ['etl_sche_depend_tbl', 'etl_sche_status_para_tbl']
# CHECK模块表清单
CHECK_TAB_LIST = ['etl_check_quality_rules', 'etl_check_cols_rule']
# SERVE模块表清单
SERVE_TAB_LIST = ['etl_serve_unload', 'etl_serve_distribute']
VIEW_TAB_LIST = ['etl_serve_view', 'etl_serve_view_mapping']

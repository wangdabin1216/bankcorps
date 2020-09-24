# coding=utf-8
# !/usr/bin/python
################################################################################
# 脚本名称：init_server.py
# 脚本功能：初始化入口主程序
# 输入参数：
# 编写人：  guxn
# 编写日期：20191114
# 修改记录：
# by dongs 20191114   1.创建init_server.py
#
################################################################################

import os
import sys
import getopt
import init_meta_struct
import init_meta_data
import init_load_tab
import init_trans_tab
import init_hist_data
import init_gen_pro
import init_cc_data
import operate_os
import operate_log
import operate_mysql
import setting


class init_server():

    def __init__(self, args):
        for op, value in args:
            if op == "-d":
                self.func_name = value.lower()
            elif op == "-m":
                self.model_name = value.lower()
            elif op == "-l":
                self.level_name = value.lower()
            elif op == "-c":
                self.all_flag = value.lower()
            elif op == "-s":
                self.sys_name = value.lower()
            elif op == "-t":
                self.tab_name = value.lower()
            elif op == "-v":
                self.view_name = value.lower()
            elif op == "-f":
                self.file_name = value.lower()
        # 全局变量
        self.insert_hist_file = setting.DATA_FILE + 'dw_sm_hist_ext.txt'  # 历史拉链配置表写入文件
        self.meta_excel_name = setting.TEMPLATE_PATH + 'meta/etl_meta_init.xlsx'  # meta模块配置excel文档
        self.sche_excel_name = setting.TEMPLATE_PATH + 'sche/etl_sche_conf.xlsx'  # 调度模块配置表excel文档名
        self.view_excel_name = setting.TEMPLATE_PATH + 'server/etl_serve_view.xlsx'  # 调度模块配置表excel文档名
        self.server_excel_name = setting.TEMPLATE_PATH + 'server/etl_serve_conf.xlsx'  # 卸载分发模块配置表excel文档名
        self.check_excel_name = setting.TEMPLATE_PATH + 'check/etl_check_cols_quality.xlsx'  # 卸载分发模块配置表excel文档名
        self.logger = operate_log.getLogger("/Users/wangdabin1216/git/dw/log/init/init_server.log")
        self.opos = operate_os.Operate_os(self.logger)
        self.db = operate_mysql.Operate_mysql(self.logger)
        self.ilt = init_load_tab.Init_load_tab(self.logger, self.opos, self.db)
        self.itt = init_trans_tab.Init_trans_tab(self.logger, self.opos, self.db)
        self.ihd = init_hist_data.Init_hist_data(self.logger, self.opos)
        self.igp = init_gen_pro.Init_gen_pro(self.logger, self.opos, self.db)
        self.icd = init_cc_data.Init_cc_data(self.logger, self.opos, self.db)
        self.ims = init_meta_struct.Init_meta_struct(self.logger, self.opos)
        self.imd = init_meta_data.Init_meta_data(self.logger, self.opos, self.db)

    def get_tab_sql(self):
        '''
        :return:
        '''
        tab_sql = ''
        if self.level_name in ['tbo', 'omi']:
            tab_sql = "select concat(sys,'_',tab_name) from etl_load_table where is_valid=1 and  is_create = 1;"
        elif self.level_name in ['fmi', 'cmi', 'dsa']:
            if self.all_flag == 'all':
                tab_sql = "select tab_name from etl_trans_table where is_valid=1 and lower(sys) = '" + self.level_name + "' ;"
            elif self.all_flag == 'tab':
                tab_sql = "select tab_name from etl_trans_table where is_valid=1 and lower(sys) = '" + self.level_name + "' and upper(tab_name)='" + self.tab_name.upper() + "';"
        return tab_sql

    def up_date_tab_list(self):
        if self.level_name in ['tbo', 'omi']:
            if self.all_flag == 'all':
                tab_sql1 = "update etl_load_table set is_create = 0 ;"
                tab_sql2 = "update etl_load_table set is_create = 1 where is_valid=1 ;"
                self.db.execute_sql(tab_sql1)
                self.db.execute_sql(tab_sql2)
                self.db.commit()
            elif self.all_flag == 'sys':
                tab_sql1 = "update etl_load_table set is_create = 0 ;"
                tab_sql2 = "update etl_load_table set is_create = 1 where is_valid=1 and upper(sys)='" + self.sys_name.upper() + "';"
                self.db.execute_sql(tab_sql1)
                self.db.execute_sql(tab_sql2)
                self.db.commit()
            elif self.all_flag == 'tab':
                tab_sql1 = "update etl_load_table set is_create = 0 ;"
                tab_sql2 = " update etl_load_table set is_create = 1 where is_valid=1 and concat(upper(sys),'_',upper(tab_name))='" + self.tab_name.upper() + "';"
                self.db.execute_sql(tab_sql1)
                self.db.execute_sql(tab_sql2)
                self.db.commit()
        elif self.level_name in ['fmi', 'cmi', 'dsa']:
            if self.all_flag == 'all':
                tab_sql1 = "update etl_trans_table set is_create = 0 ;"
                tab_sql2 = "update etl_trans_table set is_create = 1 where is_valid=1 and upper(sys)='" + self.level_name.upper() + "' ;"
                self.db.execute_sql(tab_sql1)
                self.db.execute_sql(tab_sql2)
                self.db.commit()
            elif self.all_flag == 'tab':
                tab_sql1 = "update etl_trans_table set is_create = 0 ;"
                tab_sql2 = "update etl_trans_table set is_create = 1 where is_valid=1 and upper(sys)='" + self.level_name.upper() + "' and upper(tab_name)='" + self.tab_name.upper() + "';"
                self.db.execute_sql(tab_sql1)
                self.db.execute_sql(tab_sql2)
                self.db.commit()
        return 0

    def get_tab_list(self, tab_sql):
        '''
        获取所有要创建的表清单
        :return:tab_list
        '''
        tab_list = []
        cu = self.db.search(tab_sql)
        for row in cu:
            tab_list.append(str(row[0]))
        return tab_list

    def get_view_list(self):
        '''
        获取所有要创建的视图清单
        :return:
        '''
        view_list = []
        return view_list

    def get_model_excel_list(self):
        '''
        获取所有要导入的excel清单
        :return:
        '''
        model_excel_list = []
        if self.model_name == 'trans':
            path = setting.TEMPLATE_PATH + self.model_name + '/' + self.level_name + '/'
        else:
            path = setting.TEMPLATE_PATH + self.model_name + '/'
        for file in os.listdir(path):
            file_path = os.path.join(path, file)
            if os.path.splitext(file_path)[1] == '.xlsx':
                model_excel_list.append(file_path)
            else:
                pass
        return model_excel_list

    def run_ddl(self):
        '''
        通过判断参数创建各层级表结构
        :return:0
        '''

        if self.level_name == 'tbo':
            # 更新所有要创建的表的状态

            self.up_date_tab_list()
            # 获取建表DDL文件名称
            file_name = self.ilt.init_load_tab('TBO', 'EXT')
            print(file_name)
            # 执行编译DDL文件，创建表
            print(file_name)
            re = self.opos.compile_inceptor_file(file_name)
            if re == 1:
                print("库表创建编译已完成，请检查执行结果.")
            else:
                print("文件编译出现错误.")
        elif self.level_name == 'omi':
            # 更新所有要创建的表的状态
            self.up_date_tab_list()
            # 获取建表DDL文件名称
            file_name = self.ilt.init_load_tab('OMI', 'ORC')
            # 执行编译DDL文件，创建表
            re = self.opos.compile_inceptor_file(file_name)
            if re == 1:
                print("库表创建编译已完成，请检查执行结果.")
            else:
                print("文件编译出现错误.")

            # 获取建表DDL文件名称
            file_name = self.ilt.init_load_tab('OMI', 'ORCHS')
            # 执行编译DDL文件，创建表
            re = self.opos.compile_inceptor_file(file_name)
            if re == 1:
                print("库表创建编译已完成，请检查执行结果.")
            else:
                print("文件编译出现错误.")
        elif self.level_name in ['fmi', 'cmi', 'dsa']:
            # 更新所有要创建的表的状态
            self.up_date_tab_list()
            # 获取建表DDL文件名称
            file_name = self.itt.init_trans_tab(self.level_name, 'ORC')
            # 执行编译DDL文件，创建表
            re = self.opos.compile_inceptor_file(file_name)
            if re == 1:
                print("FMI层ORC表创建编译已完成，请检查执行结果.")
            else:
                print("文件编译出现错误.")
            # 获取建表DDL文件名称
            file_name = self.itt.init_trans_tab(self.level_name, 'ORCHS')
            # 执行编译DDL文件，创建表
            re = self.opos.compile_inceptor_file(file_name)
            if re == 1:
                print("FMI层ORCHS表创建编译已完成，请检查执行结果.")
            else:
                print("文件编译出现错误.")

        else:
            print('目前支持创建DDL的层级-l [tbo/omi/fmi/cmi]')
        return 0

    def run_hist(self):
        '''
        插入历史存储配置信息
        :return:
        '''
        if self.level_name == 'omi':
            # 更新所有要创建的表的状态
            self.up_date_tab_list()
            tab_sql = self.get_tab_sql()
            # 获取要创建的所有表
            tab_list = self.get_tab_list(tab_sql)
            self.opos.delete_file(self.insert_hist_file)
            # 遍历列表，写入历史拉链配置表insert语句
            for t in range(len(tab_list)):
                self.ihd.init_hist_data('tbo', 'omi', tab_list[t].lower())
            # 连接incerptor数据库，执行insert语句
            self.ihd.init_exec_pro()
            print('历史存储配置信息插入完成')
        elif self.level_name in ['fmi', 'cmi', 'dsa']:
            # 更新所有要创建的表的状态
            self.up_date_tab_list()
            tab_sql = self.get_tab_sql()
            tab_list = self.get_tab_list(tab_sql)
            self.opos.delete_file(self.insert_hist_file)
            # 遍历列表，创建表
            for t in range(len(tab_list)):
                self.ihd.init_hist_data(self.level_name, self.level_name, tab_list[t].lower())
            self.ihd.init_exec_pro()
            print('历史存储配置信息插入完成')

        else:
            print('目前支持创建hist的层级-l [omi/fmi/cmi/dsa]')
        return 0

    def run_pro(self):
        '''
        通过判断参数创建编译各层级存储过程
        :return:0
        '''
        if self.level_name in ['fmi', 'cmi', 'dsa']:
            tab_sql = self.get_tab_sql()
            tab_list = self.get_tab_list(tab_sql)
            # 遍历列表，创建表
            for t in range(len(tab_list)):
                print(('trans模块编译存储过程' + tab_list[t] + '开始处理').center(150, '*'))
                self.igp.call_fun(tab_list[t])
                print(('trans模块编译存储过程' + tab_list[t] + '处理完成').center(150, '*'))
        else:
            print('编译存储过程目前只支持fmi,cmi,dsa')
        return 0

    def run_view(self):
        '''
        创建视图
        :return:
        '''
        if self.level_name == 'fmi':
            # 获取所有创建视图的列表
            view_list = self.view_list(self)
            # 遍历列表，创建视图

        elif self.level_name == 'cmi':
            # 获取所有创建视图的列表
            view_list = self.get_view_list(self)
            # 遍历列表，创建视图

        elif self.level_name == 'cmi':
            # 获取所有创建视图列表
            view_list = self.get_view_list(self)
            # 遍历列表，创建视图
        return 0

    def run_imp(self):
        '''
        数据导入函数入口
        :return:
        '''
        # db_name = self.sqlite_path
        if self.model_name == 'meta':
            if self.all_flag == 'all':
                print('完整meta模块初始化创建及数据导入开始处理'.center(150, '*'))
                file_name = self.ims.init_meta_struct(self.meta_excel_name, setting.META_TAB_LIST[0])
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print('完整meta模块初始化创建已完成')
                for tab in setting.META_TAB_LIST:
                    file_name = self.imd.init_meta_data(self.meta_excel_name, tab, '1=1')
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                    print('meta模块{}表数据导入完成'.format(tab))
                print('完整meta模块初始化创建及数据导入处理完成'.center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                print('meta模块' + tab_name + '表数据导入开始处理'.center(50, '*'))
                file_name = self.imd.init_meta_data(self.meta_excel_name, tab_name, '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print('meta模块' + tab_name + '表数据导入处理完成'.center(50, '*'))
            else:
                print('meta模块目前只支持全量、表级数据导入处理')
        elif self.model_name == 'load':
            if self.all_flag == 'all':
                print('完整load模块数据导入开始处理'.center(150, '*'))
                # 获取load模块所有excel文件列表
                load_all_excel_list = self.get_model_excel_list()
                for i in range(0, len(load_all_excel_list)):
                    model_excel_name = load_all_excel_list[i]
                    limit_term = "upper(sys) = \'" + model_excel_name[-7:-5].upper() + "\'"
                    for tab in setting.LOAD_TAB_LIST:
                        file_name = self.imd.init_meta_data(model_excel_name, tab, limit_term)
                        re = self.opos.compile_mysql_file(file_name)
                        if re == 1:
                            print("库表创建编译已完成，请检查执行结果.")
                        else:
                            print("文件编译出现错误.")
                    print('load模块' + model_excel_name + '数据导入完成')
                print('完整load模块数据导入处理完成'.center(150, '*'))
            elif self.all_flag == 'sys':
                sys_name = self.sys_name
                model_excel_name = '/Users/wangdabin1216/git/dw/template/load/etl_load_' + sys_name + '.xlsx'
                limit_term = "upper(sys) = \'" + sys_name.upper() + "\'"
                print(('load模块' + sys_name + '系统数据导入开始处理').center(150, '*'))
                for tab in setting.LOAD_TAB_LIST:
                    file_name = self.imd.init_meta_data(model_excel_name, tab, limit_term)
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                print(('load模块' + sys_name + '系统数据导入处理完成').center(150, '*'))
            elif self.all_flag == 'file':
                model_excel_name = self.file_name
                limit_term = "upper(sys) = \'" + model_excel_name[-7:-5].upper() + "\'"
                print(('load模块指定excel' + model_excel_name + '数据导入开始处理').center(150, '*'))
                for tab in setting.LOAD_TAB_LIST:
                    file_name = self.imd.init_meta_data(model_excel_name, tab, limit_term)
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                print(('load模块指定excel' + model_excel_name + '数据导入处理完成').center(150, '*'))
            else:
                print('load模块目前只支持全量、系统级、指定excel文件数据导入处理')
        elif self.model_name == 'trans':
            if self.all_flag == 'level':
                print('完整trans模块数据导入开始处理'.center(150, '*'))
                # 获取load模块所有excel文件列表
                trans_all_excel_list = self.get_model_excel_list()
                for i in range(0, len(trans_all_excel_list)):
                    model_excel_name = trans_all_excel_list[i]
                    limit_term = "upper(tab_name) = \'" + model_excel_name[:-5][52:].upper() + "\'"
                    print('trans模块' + model_excel_name + '数据开始导入')
                    for tab in setting.TRANS_TAB_LIST:
                        file_name = self.imd.init_meta_data(model_excel_name, tab, limit_term)
                        # 执行编译DDL文件，创建表
                        re = self.opos.compile_mysql_file(file_name)
                        if re == 1:
                            print("库表创建编译已完成，请检查执行结果.")
                        else:
                            print("文件编译出现错误.")
                    print('trans模块' + model_excel_name + '数据导入完成')
                print('完整trans模块数据导入处理完成'.center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                if tab_name[0:1] == 'f':
                    self.level_name = 'fmi'
                elif tab_name[0:1] == 'c':
                    self.level_name = 'cmi'
                else:
                    self.level_name = 'dsa'
                model_excel_name = '/etl/dw/template/trans/' + self.level_name + '/etl_trans_' + self.tab_name + '.xlsx'
                limit_term = "upper(tab_name) = \'" + tab_name.upper() + "\'"
                print('trans模块' + tab_name + '表数据导入开始处理'.center(50, '*'))
                for tab in setting.TRANS_TAB_LIST:
                    file_name = self.imd.init_meta_data(model_excel_name, tab, limit_term)
                    # 执行编译DDL文件，创建表
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                print('trans模块' + tab_name + '表数据导入处理完成'.center(50, '*'))
            else:
                print('trans模块目前只支持层级、表级')
        elif self.model_name == 'view':
            model_excel_name = self.view_excel_name
            if self.all_flag == 'all':
                print(('完整view模块数据导入开始处理').center(150, '*'))
                for tab in setting.VIEW_TAB_LIST:
                    file_name = self.imd.init_meta_data(model_excel_name, tab, '1=1')
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                    print('view模块{}表数据导入完成'.format(tab))
                print(('完整view模块初始化数据导入处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                print(('view模块' + tab_name + '表数据导入开始处理').center(150, '*'))
                file_name = self.imd.init_meta_data(model_excel_name, tab_name, '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print(('view模块' + tab_name + '表数据导入处理完成').center(150, '*'))
            else:
                print('view模块目前只支持全量、表级数据导入处理')
        elif self.model_name == 'server':
            model_excel_name = self.server_excel_name
            if self.all_flag == 'all':
                print(('完整server模块数据导入开始处理').center(150, '*'))
                file_name = self.imd.init_meta_data(model_excel_name, 'etl_serve_unload', '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print('server模块etl_serve_unload表数据导入完成')
                file_name = self.imd.init_meta_data(model_excel_name, 'etl_serve_distribute', '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print('server模块etl_serve_distribute表数据导入完成')
                # file_name=self.imd.init_meta_data(model_excel_name, 'etl_serve_move', '1=1')
                # re = self.opos.compile_mysql_file(file_name)
                # if re == 1 :print("库表创建编译已完成，请检查执行结果.")
                # else:print("文件编译出现错误.")
                # print('server模块etl_serve_move表数据导入完成')
                print(('完整serve模块初始化数据导入处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                print(('server模块' + tab_name + '表数据导入开始处理').center(150, '*'))
                file_name = self.imd.init_meta_data(model_excel_name, tab_name, '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print(('server模块' + tab_name + '表数据导入处理完成').center(150, '*'))
            else:
                print('server模块目前只支持全量、表级数据导入处理')
        elif self.model_name == 'check':
            model_excel_name = self.check_excel_name
            if self.all_flag == 'all':
                print(('完整check模块数据导入开始处理').center(150, '*'))
                for tab in setting.CHECK_TAB_LIST:
                    file_name = self.imd.init_meta_data(model_excel_name, tab, '1=1')
                    re = self.opos.compile_mysql_file(file_name)
                    if re == 1:
                        print("库表创建编译已完成，请检查执行结果.")
                    else:
                        print("文件编译出现错误.")
                    print('check模块{}表数据导入完成'.format(tab))
                print(('完整check模块初始化数据导入处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                print(('checke模块' + tab_name + '表数据导入开始处理').center(150, '*'))
                file_name = self.imd.init_meta_data(model_excel_name, tab_name, '1=1')
                re = self.opos.compile_mysql_file(file_name)
                if re == 1:
                    print("库表创建编译已完成，请检查执行结果.")
                else:
                    print("文件编译出现错误.")
                print(('check模块' + tab_name + '表数据导入处理完成').center(150, '*'))
            else:
                print('check模块目前只支持全量、表级数据导入处理')
        else:
            print('目前支持数据导入的模块-m[meta/load/trans/sche/serve/check]')
        return 0

    def run_syn(self):
        '''
        数据同步函数入口
        :return:
        '''
        # db_name = self.sqlite_path
        if self.model_name == 'check':
            if self.all_flag == 'all':
                print(('完整check模块数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data('etl_check_quality_rules', 'dw_sm_check_quality_rules_ext')
                print('check模块dw_sm_check_quality_rules表数据同步完成')
                self.icd.init_cc_data('etl_check_cols_rule', 'dw_sm_check_cols_rule_ext')
                print('check模块etl_check_cols_rule表数据同步完成')
                print(('完整check模块数据同步处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                aim_tab_name = tab_name.replace('etl', 'dw_sm') + '_ext'
                print(('checke模块' + tab_name + '表数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data(tab_name, aim_tab_name)
                print(('check模块' + tab_name + '表数据同步处理完成').center(150, '*'))
            else:
                print('check模块目前只支持全量、表级数据同步处理')
        elif self.model_name == 'view':
            if self.all_flag == 'all':
                print(('完整view模块数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data('etl_serve_view', 'dw_sm_serve_view_ext')
                print('view模块etl_serve_view表数据同步完成')
                self.icd.init_cc_data('etl_serve_view_mapping', 'dw_sm_serve_view_mapping_ext')
                print('view模块etl_serve_view_mapping表数据同步完成')
                print(('完整view模块数据同步处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                aim_tab_name = tab_name.replace('etl_', '')
                print(('view模块' + tab_name + '表数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data(tab_name, aim_tab_name)
                print(('view模块' + tab_name + '表数据同步处理完成').center(150, '*'))
            else:
                print('view模块目前只支持全量、表级数据同步处理')
        elif self.model_name == 'server':
            if self.all_flag == 'all':
                print(('完整server模块数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data('etl_serve_unload', 'dw_sm_unload_ext')
                print('server模块etl_serve_unload表数据同步完成')
                print(('完整server模块数据同步处理完成').center(150, '*'))
            elif self.all_flag == 'tab':
                tab_name = self.tab_name
                aim_tab_name = tab_name.replace('etl_' + self.model_name, 'dw_sm') + '_ext'
                print(('server模块' + tab_name + '表数据同步开始处理').center(150, '*'))
                self.icd.init_cc_data(tab_name, aim_tab_name)
                print(('server模块' + tab_name + '表数据同步处理完成').center(150, '*'))
            else:
                print('server模块目前只支持全量、表级数据同步处理')
        else:
            print('目前支持数据同步的模块-m[sche/server/check]')
        return 0

    def run(self):
        # 根据功能类型执行
        if self.func_name == "ddl":
            print('ddl')
            self.run_ddl()
        elif self.func_name == "imp":
            print('imp')
            self.run_imp()
        elif self.func_name == "pro":
            self.run_pro()
        elif self.func_name == "view":
            self.run_view()
        elif self.func_name == "syn":
            self.run_syn()
        elif self.func_name == "hist":
            self.run_hist()
        else:
            print('目前支持操作方法-d[ddl/imp/pro/view/syn/hist]')


if __name__ == '__main__':
    # 获取参数
    opts, srgs = getopt.getopt(sys.argv[1:], "d:m:l:c:s:t:f:v:")
    if len(opts) == 0:
        msg = '''使用说明：python init_server.py 
             -d   [选择操作方法:   [ ddl  建表    /imp  导入数据    /pro 编译存储过程  /view 创建视图  /syn  数据同步  /hist 插入历史存储配置信息   ] ]
             -m   [选择操作模块:   [ meta 元数据    /load 源系统配置模块     /trans   模型配置模块    /sche 调度配置模块     / server 服务配置模块   /check 检核模块   ] ]
             -l   [选择操作层级:   [ tbo python -m trace --trace init_server.py -d imp -m meta -c tab -t etl_load_system    /omi  基础模型层   /fmi  整合模型层    /cmi  共性加工层  /dsa  数据服务层      ] ]
             -c   [选择操作方式：  [ all  全量操作     /sys  系统级  /level 层级  /tab  表级       /file  指定文件   /    view  视图  ] ]
             -s   [系统名称]
             -t   [表名称]
             -f   [文件名称]
             -v   [视图名称]
             '''
        print(msg)
        sys.exit(0)
    else:
        ch = init_server(opts)
        ch.run()
    sys.exit(0)

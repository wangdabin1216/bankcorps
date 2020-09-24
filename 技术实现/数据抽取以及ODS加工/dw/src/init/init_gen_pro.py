#!/usr/bin/python
# -*- coding: utf-8 -*-
################################################################################
# 脚本名称：init_gen_pro.py
# 脚本功能：读取mysql数据表meta元数据生成存储过程语句
# 输入参数：表名 eg: python /etl/dw/src/util/init_gen_pro.py f_ag_dp_deps_acct
# 编写人：  guxn
# 编写日期：20191114
# 修改记录：
# by guxn 20191114   1.创建init_gen_pro.py
#                                    2.
#                                    3.
################################################################################
import os
import sys
import time
import setting
import operate_mysql
import operate_os
import operate_log

class Init_gen_pro():
  def __init__(self,logger,opos,db):
    self.logger = logger
    self.opos=opos
    self.db = db
  # 输入参数判断
  def is_valid_arg(self):
    if len(sys.argv) < 2:
        print('请正确输入参数：python /etl/dw/src/util/init_gen_pro.py [表名]')
        raise Exception('请正确输入参数：python /etl/dw/src/util/init_gen_pro.py [表名]')
    else:
        pass

  # 判断配置表中是否存在模型表
  def is_exsi_tab(self,t_name):
    sql = "select count(1) from etl_trans_mapping where lower(tab_name) = '" + t_name + "';"
    cu = self.db.search(sql)
    for row in cu:
        if row[0] == 0:
            print("配置表中不存在{}表相关信息，请确认表名或插入配置信息".format(t_name))
            raise Exception("配置表中不存在{}表相关信息，请确认表名或插入配置信息".format(t_name))
    return 0

  # 备份原有程序sql文件
  def bak_Pro_Sql(self,t_name):
    try:
        path = '/etl/dw/sql/tdh/'
        files = os.listdir(path)
        data_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        for f in files:
            filename = os.path.splitext(f)[0]
            if t_name == filename and f.endswith('.sql'):
                ods_file = os.path.join(path, f)
                new_file = path + t_name + '_' + data_time + '.sql'
                os.rename(ods_file, new_file)
                self.logger.info("备份程序成功：" + new_file)
        return 0
    except BaseException as err:
        self.logger.error("备份本地程序时出现错误:" + str(t_name) + "," + str(err.args))

  # 写入本地文件
  def write_File(self,t_name, txt):
    try:
        file_name = '/etl/dw/sql/tdh/' + t_name + '.sql'
        with open(file_name, "a+") as f:
            f.write(txt + '\n')
        return 0
    except BaseException as err:
        self.logger.error("写入本地文件时出现错误:" + str(t_name) + "," + str(err.args))

  # 获取程序sql模板里指定内容
  def fetch(self,startStr,endStr):
    try:
        filePath = '/etl/dw/template/trans/pro_template.sql'
        newLi = []
        with open(filePath, 'r', encoding='utf-8') as f:
            flag = False
            for line in f:
                if line.strip().startswith(startStr):
                    flag = True
                if line.strip().startswith(endStr):
                    flag = False
                if flag and line.strip():
                    newLi.append(line.strip())
        return newLi
    except BaseException as err:
        self.logger.error("获取程序sql模板里指定内容时出现错误:" + str(startStr) + "," + str(err.args))

  # 替换生成程序文件中的相关内容
  def replace_Pro_file(self,t_name):
    try:
        # 获取程序相关变量
        sql = "select lower(sys),lower(tab_name),lower(tab_cn_name),lower(func_desc),lower(comp_pers),lower(comp_date),lower(modif_record) from etl_trans_desc where lower(tab_name) = '" + t_name + "';"
        result = self.db.search(sql)
        for row in result:
            cols = [str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6])]
            if len(cols) > 0:
                system_flag = cols[0]
                pro_name = cols[0] + '.pro_' + cols[1]
                pro_chn_name = cols[2]
                func_desc = cols[3]
                comp_pers = cols[4]
                comp_date = cols[5]
                modif_record = cols[6]
            else:
                pass
        # 替换sql文件中相关内容
        pro_file = '/etl/dw/sql/tdh/' + t_name + '.sql'
        file = open(pro_file, 'r+')
        lines = file.readlines()
        file.seek(0, 0)
        for line in lines:
            line_new = line.replace("<t_name>", t_name)
            line_new = line_new.replace("<pro_name>", pro_name)
            line_new = line_new.replace("<system_flag>", system_flag)
            line_new = line_new.replace("<pro_chn_name>", pro_chn_name)
            line_new = line_new.replace("<func_desc>", func_desc)
            line_new = line_new.replace("<comp_pers>", comp_pers)
            line_new = line_new.replace("<comp_date>", comp_date)
            line_new = line_new.replace("<modif_record>", modif_record)
            # 将程序里的标签去除
            line_new = line_new.replace("<log_head start>", '--日志部分')
            line_new = line_new.replace("<log_end start>", '--日志部分')
            line_new = line_new.replace("<part2 start>", '--------------------')
            file.write(line_new)
        self.logger.info("替换生成程序文件中的相关内容成功")
        return 0
    except BaseException as err:
        self.logger.error("替换生成程序文件中的相关内容时出现错误:" + "," + str(err.args))

  # 获取逻辑加工中临时表列表
  def get_temp_tab_list(self,t_name):
    try:
        tmp_name = []
        sql = "select distinct lower(aim_tab_name) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and lower(tab_name) <> lower(aim_tab_name);"
        cu = self.db.search(sql)
        for row in cu:
            tmp_name.append(row[0])
        tmp_names = tmp_name
        #self.logger.info("获取逻辑加工中临时表列表成功")
        return tmp_names
    except BaseException as err:
        self.logger.error("获取逻辑加工中临时表列表失败:" + str(t_name) + "," + str(err.args))

  # 创建临时表：
  def gen_tmp_tab_ddl(self,t_name,tmp_tab):
    try:
        sql = "select lower(tab_name),lower(group_id),lower(group_desc),lower(part_id),lower(part_desc),lower(aim_db_name),lower(aim_tab_name),lower(aim_tab_cn_name),lower(part_col_id),lower(aim_col_name),lower(aim_col_cn_name),lower(aim_col_type),lower(sour_sys),lower(sour_db_name),lower(sour_tab_name),lower(sour_tab_cn_name),lower(sour_col_name),lower(sour_col_cn_name),lower(sour_col_type),ifnull(mapping,'null'),lower(main_tab),lower(main_tab_oth),lower(minor_tab),lower(minor_tab_oth),lower(part_type),lower(join_type),on_term,lower(on_term_desc),where_term,lower(where_term_desc),lower(var_name),lower(var_value),lower(var_desc) from etl_trans_mapping where upper(tab_name) = '" + t_name.upper() + "' and  upper(aim_tab_name) ='" + tmp_tab.upper() + "' ;"
        colm = []
        comment = []
        tab_db = ''
        tab_comment = ''
        cu = self.db.search(sql)
        for row in cu:
            cols = [str(row[5]), str(row[6]), str(row[7]), str(row[9]), str(row[10]), str(row[11]), str(row[4])]
            if len(cols) > 0:
                # 获取列信息
                col = cols[3] + ' ' + cols[5] + " COMMENT '" + cols[4] + "'"
                colm.append(col)
                tab_comment = "COMMENT '" + cols[2] + "' STORED AS ORC;"
                tab_db = cols[0] + '.' + cols[1]
            else:
                pass
        colms = ', '.join(colm)
        drop_tmp_sql = "EXECUTE IMMEDIATE ('drop table if exists " + tab_db + "');\n"
        self.write_File(t_name, drop_tmp_sql)
        tmp_tab_ddl = 'CREATE TABLE ' + tab_db + '( ' + colms + ') ' + tab_comment
        exu_tmp_sql = 'EXECUTE IMMEDIATE ("' + tmp_tab_ddl + '")\n'
        self.write_File(t_name, exu_tmp_sql)
        # 在数据库中创建临时表,否则表不存在，导致存储过程编译失败
        file_text = "DROP TABLE IF EXISTS " + tab_db + "; \n " + tmp_tab_ddl
        bk = os.system("beeline -u \"jdbc:hive2://bjuattdh02:10000/cc;\" -n dw -p dw -e \"" + file_text + "\" --outputformat=csv --silent=true --showHeader=true")

        self.logger.info("创建临时表{}成功".format(tab_db))
        return exu_tmp_sql
    except BaseException as err:
        self.logger.error("创建临时表{}失败:".format(tab_db) + str(t_name) + "," + str(err.args))

  # 遍历生成临时表建表语句
  def gen_Tmp_Tab_Sql(self,t_name):
    try:
        # 写入程序头部模块
        info2 = self.fetch('CREATE OR REPLACE PROCEDURE', '<part1 end>')  # 调用fetch函数进行内容匹配，结果返回列表保存到info中
        result2 = '\n'.join(info2)
        self.write_File(t_name, result2)
        txt = '--1.2创建临时表'
        self.write_File(t_name, txt)
        self.logger.info("写入程序头文件成功，开始生成临时表处理逻辑")
        # 获取临时表列表
        tmp_tab_list = self.get_temp_tab_list(t_name)
        if len(tmp_tab_list):
            for i in range(len(tmp_tab_list)):
                tmp_table = tmp_tab_list[i]
                self.gen_tmp_tab_ddl(t_name, tmp_table)
        self.logger.info("生成临时表处理逻辑完成")
        return 0
    except BaseException as err:
        self.logger.error("生成临时表处理逻辑失败:" + str(t_name) + "," + str(err.args))

  # 获取当前业务逻辑总组数
  def get_Group_No_Tal(self,t_name):
    try:
        group_no_tal = 0
        sql = "select max(group_id) from etl_trans_mapping where lower(tab_name) = '" + t_name + "';"
        cu = self.db.search(sql)
        for row in cu:
            cols = row[0]
        group_no_tal = cols
        return group_no_tal
        #self.logger.info("获取当前业务逻辑总组数完成")
    except BaseException as err:
        self.logger.error("获取当前业务逻辑总组数失败:" + str(t_name) + "," + str(err.args))

  # 获取当前组逻辑的总段数
  def get_Part_No_Tal(self,t_name, group):
    try:
        sql = "select max(part_id) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + ";"
        cu = self.db.search(sql)
        for row in cu:
            cols = row[0]
        group_no_tal = cols
        return group_no_tal
        #self.logger.info("获取当前组{}逻辑的总段数完成".format(group))
    except BaseException as err:
        self.logger.error("获取当前组{}逻辑的总段数失败：".format(group) + str(t_name) + "," + str(err.args))

  # 获取当前段的逻辑处理类型
  def get_Part_Type(self,t_name, group, part):
    try:
        sql = "select distinct lower(part_type) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and part_id = " + str(part) +" and part_type is not null;"
        cu = self.db.search(sql)
        for row in cu:
            cols = row[0]
        part_type = cols
        return part_type
        #self.logger.info("获取当前段{}的逻辑处理类型完成".format(part))
    except BaseException as err:
        self.logger.error("获取当前段{}的逻辑处理类型失败：".format(part) + str(t_name) + "," + str(err.args))
  # 生成段SQL逻辑，INSERT类型
  def make_Insert_Sql(self,t_name, group, part):
    try:
        colm1 = []
        colm2 = []
        tab_wones = []
        tables = []
        from_tab = ''
        join_items = []
        where = ''
        sql = "select lower(tab_name),lower(group_id),lower(group_desc),lower(part_id),lower(part_desc),lower(aim_db_name),lower(aim_tab_name),lower(aim_tab_cn_name),lower(part_col_id),lower(aim_col_name),lower(aim_col_cn_name),lower(aim_col_type),lower(sour_sys),lower(sour_db_name),lower(sour_tab_name),lower(sour_tab_cn_name),lower(sour_col_name),lower(sour_col_cn_name),lower(sour_col_type),ifnull(mapping,'null'),lower(main_tab),lower(main_tab_oth),lower(minor_tab),lower(minor_tab_oth),lower(part_type),lower(join_type),on_term,lower(on_term_desc),where_term,lower(where_term_desc),lower(var_name),lower(var_value),lower(var_desc) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and part_id = " + str(part) + " order by part_col_id ;"
        cu = self.db.search(sql)
        for row in cu:
            cols = [str(row[6]), str(row[9]), str(row[19]), str(row[20]), str(row[21]), str(row[22]), str(row[23]), str(row[25]), str(row[26]), str(row[28]), str(row[5]), str(row[10])]
            if len(cols) > 0:
                # 获取列信息
                colm1.append(cols[1] + '    --' + cols[11])
                colm2.append(cols[2] + '    --' + cols[11])
                tables.append(cols[0])
                tab_wones.append(cols[10])
                # 获取from表信息and where条件
                main_tab = cols[3]
                tmp_tab = t_name + '_t'
                tmp_tab_len = len(tmp_tab)
                if cols[3] != 'None':
                    main_tab = cols[3]
                    # 判断主表是否为历史表,规则：表名截取后三位为‘_HS’表示为历史表
                    if main_tab[-3:].upper() == '_HS':
                        if cols[9] != 'None':
                            where = 'WHERE ' + cols[9] + ' AND ' + cols[4] + '.' + 'partid = v_partid AND ' + cols[4] + '.' + 'begndt <= v_acct_date AND ' + cols[4] + '.' + 'overdt > v_acct_date'
                        else:
                            where = 'WHERE ' + cols[4] + '.' + 'partid= v_partid AND ' + cols[4] + '.' + 'begndt <= v_acct_date AND ' + cols[4] + '.' + 'overdt > v_acct_date'
                        from_tab = ' FROM ' + cols[3] + ' ' + cols[4]
                    elif main_tab[-3:].upper() != '_HS':
                        if cols[9] != 'None':
                            where = 'WHERE ' + cols[9]
                        else:
                            where = ''
                        from_tab = ' FROM ' + cols[3] + ' ' + cols[4]
                else:
                    pass
                # 获取join信息
                if cols[5] != 'None':
                    join_tab = cols[5]
                    if join_tab[-3:].upper() == '_HS':
                        join_tabs = cols[7] + " " + cols[5] + " " + cols[6] + " ON " + cols[8] + " AND " + cols[6] + "." + "partid = v_partid AND " + cols[6] + "." + "begndt <= v_acct_date AND " + cols[6] + "." + "overdt > v_acct_date"
                        join_items.append(join_tabs)
                    elif join_tab[-3:].upper() != '_HS':
                        join_tabs = cols[7] + " " + cols[5] + " " + cols[6] + " ON " + cols[8]
                        join_items.append(join_tabs)
                else:
                    pass
            else:
                pass
        colms1 = '\n,'.join(colm1)
        colms2 = '\n,'.join(colm2)
        table = tables[0]
        tab_wone = tab_wones[0]
        join = ('\n').join(join_items)
        insert_sql ='INSERT INTO ' + tab_wone + '.' + table + '(\n' + colms1 + '\n)\n' + 'select \n' + colms2 + '\n' + from_tab + '\n ' + join + '\n' + where + '\n;'
        # self.logger.info("insert类型逻辑段写入成功")
        return insert_sql
    except BaseException as err:
        self.logger.error("insert类型逻辑段写入失败：" + str(t_name) + "," + str(err.args))

  # 生成段SQL逻辑，MERGE类型
  def make_Merge_Sql(self,t_name, group, part):
    try:
        # 目标表字段集合
        colm1 = []
        # 来源字段映射集合
        colm2 = []
        # 来源字段集合
        colm4 = []
        # 目标与源字段对应集
        colm3 = []
        aim_tab = ''
        aim_tab_o = ''
        mer_tab = ''
        mer_tab_o = ''
        on_term = ''
        sql = "select lower(tab_name),lower(group_id),lower(group_desc),lower(part_id),lower(part_desc),lower(aim_db_name),lower(aim_tab_name),lower(aim_tab_cn_name),lower(part_col_id),lower(aim_col_name),lower(aim_col_cn_name),lower(aim_col_type),lower(sour_sys),lower(sour_db_name),lower(sour_tab_name),lower(sour_tab_cn_name),lower(sour_col_name),lower(sour_col_cn_name),lower(sour_col_type),ifnull(mapping,'null'),lower(main_tab),lower(main_tab_oth),lower(minor_tab),lower(minor_tab_oth),lower(part_type),lower(join_type),on_term,lower(on_term_desc),where_term,lower(where_term_desc),lower(var_name),lower(var_value),lower(var_desc) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and part_id = " + str(part) + " order by part_col_id ;"
        cu = self.db.search(sql)
        for row in cu:
            cols = [str(row[6]), str(row[9]), str(row[19]), str(row[20]), str(row[21]), str(row[22]), str(row[23]), str(row[25]), str(row[26]), str(row[28]), str(row[16]), str(row[10])]
            if len(cols) > 0:
                # 获取表名及关联条件
                if cols[3] != 'None':
                    aim_tab = cols[3]
                    aim_tab_o = cols[4]
                    mer_tab = cols[5]
                    mer_tab_o = cols[6]
                    on_term = cols[8]
                else:
                    pass
                #获取列信息
                colm1.append(cols[1] + '    --' + cols[11])
                colm2.append(cols[2] + '    --' + cols[11])
                colm3.append(aim_tab_o + '.' + cols[1] + ' = ' + cols[2] + '    --' + cols[11])
                colm4.append(cols[10] + '    --' + cols[11])
            else:
                pass
        colms1 = ('\n,').join(colm1)
        colms2 = ('\n,').join(colm2)
        colms3 = ('\n,').join(colm3)
        colms4 = ('\n,').join(colm4)
        sql_mergr = 'MERGE INTO ' + aim_tab + ' ' + aim_tab_o + '\n USING (\nSELECT \n' + colms4 + '\n FROM ' + mer_tab + ') ' + mer_tab_o + '\n ON (' + on_term + ')\n' + 'WHEN MATCHED THEN \n UPDATE SET \n' + colms3 + '\n WHEN NOT MATCHED THEN \n INSERT (\n' + colms1 + '\n) \n VALUES(\n' + colms2 + '\n)\n;'
        return sql_mergr
    except BaseException as err:
        self.logger.error("merge类型逻辑段写入失败：" + str(t_name) + "," + str(err.args))

  # 生成段SQL逻辑，UPDATE类型
  def make_Update_Sql(self,t_name, group, part):
    try:
        set_item = []
        where = ''
        table_aims = ''
        table_from = ''
        sql = "select lower(tab_name),lower(group_id),lower(group_desc),lower(part_id),lower(part_desc),lower(aim_db_name),lower(aim_tab_name),lower(aim_tab_cn_name),lower(part_col_id),lower(aim_col_name),lower(aim_col_cn_name),lower(aim_col_type),lower(sour_sys),lower(sour_db_name),lower(sour_tab_name),lower(sour_tab_cn_name),lower(sour_col_name),lower(sour_col_cn_name),lower(sour_col_type),ifnull(mapping,'null'),lower(main_tab),lower(main_tab_oth),lower(minor_tab),lower(minor_tab_oth),lower(part_type),lower(join_type),on_term,lower(on_term_desc),where_term,lower(where_term_desc),lower(var_name),lower(var_value),lower(var_desc) from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and part_id = " + str(part) + " order by part_col_id ;"
        cu = self.db.search(sql)
        for row in cu:
            cols = [str(row[6]), str(row[9]), str(row[19]), str(row[20]), str(row[21]), str(row[22]), str(row[23]), str(row[25]), str(row[26]), str(row[28]), str(row[16])]
            if len(cols) > 0:
                # 获取表名及关联条件
                if cols[3] != 'None':
                    table_aims = cols[3]
                    table_aims_o = cols[4]
                    table_from = cols[5]
                    table_from_o = cols[6]
                    where = cols[8]
                else:
                    pass
                #获取更新字段
                set_item.append(cols[1] + ' = ' + cols[2])
            else:
                pass
        set_items = (',\n ').join(set_item)
        update_sql = 'UPDATE ' + table_aims + ' ' + table_aims_o + '\n SET ' + set_items + '\n WHERE EXISTS \n (SELECT 1 FROM ' + table_from + ' ' + table_from_o + '\n WHERE ' + where + ' )\n ;'
        return update_sql
    except BaseException as err:
        self.logger.error("UPDATE类型逻辑段写入失败：" + str(t_name) + "," + str(err.args))

  # 获取当前组注释信息
  def get_Group_comment(self,t_name, group):
    try:
        sql = "select distinct group_id,group_desc from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and group_desc is not null;"
        cu = self.db.search(sql)
        for row in cu:
            cols = row[1]
        group_comment = cols
        return group_comment
    except BaseException as err:
        self.logger.error("获取当前组注释信息时出现异常：" + str(t_name) + "," + str(err.args))

  # 获取当前段注释信息
  def get_Part_comment(self,t_name, group , part):
    try:
        sql = "select distinct group_id,part_id, part_desc from etl_trans_mapping where lower(tab_name) = '" + t_name + "' and group_id = " + str(group) + " and part_id = " + str(part) + " and part_desc is not null;"
        cu = self.db.search(sql)
        for row in cu:
            cols = row[2]
        part_comment = cols
        return part_comment
    except BaseException as err:
        self.logger.error("获取当前段注释信息时出现异常：" + str(t_name) + "," + str(err.args))

  # 执行sql文本
  def execu_Sql_Indb(self,file):
    file_object = open(file)
    try:
        file_context = file_object.read()
        file_text = file_context.replace('"', '\\"')
        #bk =self.opos.compile_inceptor_file(file_text)
        bk = os.system("beeline -u \"jdbc:hive2://bjuattdh02:10000/cc;\" -n dw -p dw -e \"" + file_text + "\" --outputformat=csv --silent=true --showHeader=true")
    finally:
        file_object.close()
    return 0

  # 生成主逻辑段
  def gen_pro(self,t_name):
    try:
        self.logger.info("生成数据处理主逻辑段开始")
        # 初始化当前处理组，段序号
        txt1 = '--1.3 数据处理'
        self.write_File(t_name, txt1)
        group_no = 1
        part_no = 1
        groups_no_tal = self.get_Group_No_Tal(t_name)
        while (group_no <= groups_no_tal):
            # 写入组注释
            group_comment = self.get_Group_comment(t_name, group_no)
            coment_txt = '--' + group_comment
            self.write_File(t_name, coment_txt)
            # 获取当前组总段数
            part_no_tal = self.get_Part_No_Tal(t_name, group_no)
            while (part_no <= part_no_tal):
                # 获取段注释
                comment = self.get_Part_comment(t_name, group_no, part_no)
                part_comment = str(group_no) + '.' + str(part_no) + comment
                # 写入日志处理
                info = self.fetch('<log_head start>', '<log_head end>')  # 调用fetch函数进行内容匹配，结果返回列表保存到info中
                result = '\n'.join(info)
                log_begin = result.replace("<part_comment>", part_comment)
                self.write_File(t_name, log_begin)
                # 写入数据处理逻辑
                part_type = self.get_Part_Type(t_name, group_no, part_no)
                if part_type == 'insert':
                    sql = self.make_Insert_Sql(t_name, group_no, part_no)
                    self.write_File(t_name, sql)
                elif part_type == 'merge':
                    sql = self.make_Merge_Sql(t_name, group_no, part_no)
                    self.write_File(t_name, sql)
                elif part_type == 'update':
                    sql = self.make_Update_Sql(t_name, group_no, part_no)
                    self.write_File(t_name, sql)
                else:
                    pass
                # 写入日志处理模块
                info1 = self.fetch('<log_end start>', '<log_end end>')  # 调用fetch函数进行内容匹配，结果返回列表保存到info中
                result1 = '\n'.join(info1)
                self.write_File(t_name, result1)
                part_no = part_no + 1
            group_no = group_no + 1
            # 将当前段
            part_no = 1
        # 写入程序尾部模块
        info2 = self.fetch('<part2 start>', '<part2 end>')  # 调用fetch函数进行内容匹配，结果返回列表保存到info中
        result2 = '\n'.join(info2)
        self.write_File(t_name, result2)
        self.logger.info("生成数据处理主逻辑段结束")
        return 0
    except BaseException as err:
        self.logger.error("生成数据处理主逻辑段时出现异常：" + str(t_name) + "," + str(err.args))


  # 在入口程序或其他脚本中调用
  def call_fun(self,t_name):
    try:
        t_name = t_name.lower()
        file = '/etl/dw/sql/tdh/' + t_name + '.sql'
        self.logger.info("\n\n\n\n\------------------------------------开始处理生成" + t_name + "程序-------------------\n")
        self.is_exsi_tab(t_name)
        self.bak_Pro_Sql(t_name)
        self.gen_Tmp_Tab_Sql(t_name)
        self.gen_pro(t_name)
        self.replace_Pro_file(t_name)
        self.execu_Sql_Indb(file)
        self.logger.info("\n\n------------------------------------成功生成" + t_name + "程序-------------------\n\n\n\n")
    except Exception as err:
        self.logger.error("程序处理异常：" + str(t_name) + "," + str(err.args))
    finally:
        self.db.close_mysql()

  def main(self):
    try:
        self.is_valid_arg()
        t_name = sys.argv[1].lower()
        file = '/etl/dw/sql/tdh/' + t_name + '.sql'
        self.logger.info("\n\n\n\n\------------------------------------开始处理生成" + t_name + "程序-------------------\n")
        self.is_exsi_tab(t_name)
        self.bak_Pro_Sql(t_name)
        self.gen_Tmp_Tab_Sql(t_name)
        self.gen_pro(t_name)
        self.replace_Pro_file(t_name)
        self.execu_Sql_Indb(file)
        self.logger.info("\n\n------------------------------------成功生成" + t_name + "程序-------------------\n\n\n\n")
    except Exception as err:
        self.logger.error("程序处理异常：" + str(t_name) + "," + str(err.args))
    finally:
        self.db.close_mysql()

if __name__ == '__main__':
    logger=operate_log.getLogger("/Users/wangdabin1216/git/dw/log/init/init_gen_pro.log")
    igp=Init_gen_pro(logger,operate_os.Operate_os(logger),operate_mysql.Operate_mysql(logger))
    igp.main()

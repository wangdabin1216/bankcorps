#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Administrator
# datetime:2020/2/24 14:06
# software: PyCharm


import xlrd
import argparse
import os
import time
import pinyin
#import win_unicode_console

#win_unicode_console.enable()


def args_parse():
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', '-p', required=True, help='excel file path')
    ap.add_argument('--type', '-t', required=True, help='excel type(1:nomal excel 2:controlpath excel)', type=int)
    ap.add_argument('--source', '-s', required=True, help='excel file source(like:zs)')
    args = vars(ap.parse_args())
    return args


def getStrAllAplha(str, sty):
    if sty == 'U':
        return pinyin.get_initial(str, delimiter="").upper()
    elif sty == 'L':
        return pinyin.get_initial(str, delimiter="").lower()
    else:
        return str


def xlsx_to_txt(filepath, source):
    (filedir, filename) = os.path.split(filepath)
    (fn, ext) = os.path.splitext(filename)
    if fn.find('-'):
        fn=fn.split('-')[0]
    else:
        exit(1)
    workbook = xlrd.open_workbook(filepath)
    sheets = workbook.sheet_names()
    #list_tab = '%s_%s_%s.txt' % (fn, getStrAllAplha(source, 'U'), getStrAllAplha('LIST_TABLES', 'U'))
    #list_tabs = open(list_tab, 'w', encoding='UTF-8')
    for i in range(len(sheets)):
        sheet_name = sheets[i].replace('-', '_').replace(' ', '')
        table = workbook.sheet_by_index(i)
        #sht = '%s/%s_%s_%s.txt' % (filedir, getStrAllAplha(source, 'U'), fn, sheet_name)  # 处理过的文件名--中文
        sht = '%s/%s_%s_%s.txt' % (filedir, getStrAllAplha(source, 'U'), getStrAllAplha(fn,'U'), getStrAllAplha(sheet_name,'U')) #处理过的文件名--英文
        #list_tabs.write('%s_%s_%s.txt\n' % (getStrAllAplha(source, 'U'),fn, sheet_name))  # 需要加载的文件匹配--中文
        #list_tabs.write('%s_%s_%s.txt\n'%(getStrAllAplha(source, 'U'), getStrAllAplha(fn,'U'), getStrAllAplha(sheet_name,'U')))  #需要加载的文件匹配--英文
        #sht_tab = '%s/%s_%s_%s_CREATE.txt' % (filedir,  getStrAllAplha(source, 'U'),fn, sheet_name)  # 创建SQL的文件名
        #cre_tab=open(sht_tab, 'a+', encoding='UTF-8')
        with open(sht, 'a+', encoding='UTF-8') as sht_file:
            for row_num in range(0, table.nrows):
                row_value = table.row_values(row_num)  # list类型
                data = ''
                if row_num == 0:
                    continue
                    #cre_tab.write('字段个数:%s\n' % len(row_value))
                    #cre_tab.write('CREATE EXTERNAL TABLE IF NOT EXISTS omi.EXT_%s_%s_%s\n(' % (getStrAllAplha(source, 'U'),getStrAllAplha(fn,'U'), getStrAllAplha(sheet_name, 'U')))
                    for x in range(len(row_value)):
                        value = row_value[x]
                        if x == 0:
                            cre_tab.write('%s STRING COMMENT \'%s\'\n' % (getStrAllAplha(value, 'U'), value))
                        else:
                            cre_tab.write(',%s STRING COMMENT \'%s\'\n' % (getStrAllAplha(value, 'U'), value))
                    cre_tab.write(
                        ')\nCOMMENT \'%s\'\nROW FORMAT SERDE \'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe\'\nWITH SERDEPROPERTIES(\'input.delimited\' = \'|+|\')\nSTORED AS TEXTFILE\nLOCATION \'/dw/tbo/%s_%s\';\n\n' % (
                            sheet_name, getStrAllAplha(source, 'L'), getStrAllAplha(sheet_name, 'L')))
                else:
                    for x in range(len(row_value)):
                        if x == 0:
                            value = row_value[x]
                        else:
                            value = '|+|%s' % row_value[x]
                        data = data + value
                    sht_file.write(data + '\n')
        # print(sht, " created!")
        #print(sht_tab, " created!")
    #list_tabs.close()
    #cre_tab.close()


def cp_xlsx_to_txt(filepath, source):
    (filedir, filename) = os.path.split(filepath)
    (fn, ext) = os.path.splitext(filename)
    workbook = xlrd.open_workbook(filepath)
    sheets = workbook.sheet_names()
    for i in range(len(sheets)):
        sheet_name = sheets[i].replace('-', '_').replace(' ', '')
        table = workbook.sheet_by_index(i)
        file_name1 = '%s/%s_%s_%s.txt' % (filedir, getStrAllAplha(source, 'U'), getStrAllAplha(sheet_name, 'U'), getStrAllAplha('股东', 'U'))
        file_name2 = '%s/%s_%s_%s.txt' % (filedir, getStrAllAplha(source, 'U'), getStrAllAplha(sheet_name, 'U'), getStrAllAplha('对外投资', 'U'))
        file_name3 = '%s/%s_%s_%s.txt' % (filedir, getStrAllAplha(source, 'U'), getStrAllAplha(sheet_name, 'U'), getStrAllAplha('实际控制人', 'U'))
        with open(file_name1, 'a', encoding='UTF-8') as file1, open(file_name2, 'a', encoding='UTF-8') as file2, open(
                file_name3, 'a', encoding='UTF-8') as file3:
            flag = 1
            for row_num in range(0, table.nrows):
                row_value = table.row_values(row_num)  # list类型
                data = ''
                if row_num==0:
                    target=row_value[0]
                    if '：' in target:
                        target=target.split('：')[1]
                else:
                    if row_value[0] == '股东':
                        continue
                    elif row_value[0] == '对外投资':
                        flag = 2
                        continue
                    elif row_value[0] == '实际控制人':
                        flag = 3
                        continue
                    else:
                        for x in range(len(row_value)):
                            if x == 0:
                                value = '%s|+|%s'%(target,row_value[x])
                            else:
                                value = '|+|%s' % row_value[x]
                            data = data + value
                    if flag == 1:
                        file1.write(data + '\n')
                    elif flag == 2:
                        file2.write(data + '\n')
                    elif flag == 3:
                        file3.write(data + '\n')
            #print(file_name1, " created!")
            #print(file_name2, " created!")


def delete(path):
    if not os.path.exists(path):
        return -1
    for root, dirs, names in os.walk(path):
        for filename in names:
            filepath = os.path.join(root, filename)
            #print('delete~~~~%s'%filepath)  # 路径和文件名连接构成完整路径
            (fn, ext) = os.path.splitext(filename)
            if ext == '.txt':
                os.remove(filepath)


def walk(path, type_num, source):
    if not os.path.exists(path):
        return -1
    for root, dirs, names in os.walk(path):
        for filename in names:
            filepath = os.path.join(root, filename)
            #print(filepath)  # 路径和文件名连接构成完整路径
            (fn, ext) = os.path.splitext(filename)
            if ext == '.xlsx':
                print('----------')
                if type_num == 1:
                    xlsx_to_txt(filepath, source)
                if type_num == 2:
                    cp_xlsx_to_txt(filepath, source)
                print("Completed to convert ", filepath, " to txt files!")


if __name__ == '__main__':
    st = time.time()
    args = args_parse()
    print(args)
    path = args['path']
    type_num = args['type']
    source = args['source']
    delete(path)
    walk(path, type_num, source)
    nd = time.time()
    tm = nd - st
    print("Spend time: ", tm, "s")

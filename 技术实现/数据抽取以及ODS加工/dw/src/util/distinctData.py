#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Administrator
# datetime:2020/2/27 15:55
# software: PyCharm

"""
测试数据：50亿+的数据去重，200G+超大文件
此法不耗内存，但比上面千万级数据快速去重略慢
data此文件夹需先创建好
"""
import argparse
from time import time
import os
#import win_unicode_console

#win_unicode_console.enable()

SPLIT_COUNT = 10


def args_parse():
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument('--inputpath', '-inp', required=True, help='txt file path')
    ap.add_argument('--generate_dir', '-gp', required=True, help='must exist')
    ap.add_argument('--outputpath', '-outp', required=True, help='new txt file path')
    args = vars(ap.parse_args())
    return args


def write_data(t_file, value):
    t_file.write(value)


# hash方法：从大文件中取出每行数据X，并对数据进行hash(X)%N，N是需要hash到的文件数目，这就达到了对相同的数据映射到同一个文件的办法，而在分配的过程中根据计算机的内存来调整N的大小，这样做的目的就是为了让内存读入小文件进行set操作。
def calcu_hash(filename, handle_file):
    with open(filename, 'r', encoding='UTF-8') as f:
        for line in f:
            write_data(handle_file[hash(line) % SPLIT_COUNT], line)


# 生成文件
def generate_file(dir):
    handle_file, files = [], []
    for i in range(SPLIT_COUNT):
        # path = dir + "split_" + str(i)
        path = os.path.join(dir, 'split_%s' % str(i))
        files.append(path)
        f = open(path, 'w', encoding='UTF-8')  # 此f不能关闭
        handle_file.append(f)
    return files, handle_file


# 关闭文件
def close_file(handle_file):
    for i in range(len(handle_file)):
        handle_file[i].close()


# 数据去重
def data_uniq(files, new_file):
    dataset = dict()
    n_file = open(new_file, 'w', encoding='UTF-8')
    i = 0
    for filename in files:
        f = open(filename, 'r', encoding='UTF-8')
        for line in f:
            dataset[line] = 1
        f.close()
        for key in dataset:
            n_file.write(key)
            i += 1
        dataset = {}
    n_file.close()
    print('去重后总行数为%s行。' % i)


def walk(input_dir, generate_dir, out_dir):
    if not os.path.exists(input_dir):
        return -1
    generate_dir = os.path.join(generate_dir)
    print(generate_dir)
    for root, dirs, names in os.walk(input_dir):
        for filename in names:
            input_file = os.path.join(root, filename)
            new_file = os.path.join(out_dir, filename)
            run(input_file, generate_dir, new_file)


def run(filename, generate_dir, new_file):
    print('开始去重...')
    start = time()
    files, handle_file = generate_file(generate_dir)
    calcu_hash(filename, handle_file)
    close_file(handle_file)
    data_uniq(files, new_file)
    end = time()
    shi = end - start
    print('去重完毕！')
    print('总耗时%s秒！' % shi)


if __name__ == "__main__":
    args = args_parse()
    print(args)
    input_dir = args['inputpath']
    generate_dir = args['generate_dir']
    out_dir = args['outputpath']
    walk(input_dir, generate_dir, out_dir)

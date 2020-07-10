# -*- coding: utf-8 -*-
# @Time    : 2019/9/29 11:21
# @Author  : Ye Jinyu__jimmy
# @File    : test
import pandas as pd
import cx_Oracle
import os
import numpy as np
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 500)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
# 注：设置环境编码方式，可解决读取数据库乱码问题
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
from matplotlib import pyplot as plt

'''program是对产品的图片信息进行文字提取的操作'''

#parser是根据字符串解析成datetime,字符串可以很随意，可以用时间日期的英文单词，
# 可以用横线、逗号、空格等做分隔符。没指定时间默认是0点，没指定日期默认是今天，没指定年份默认是今年。
# from pylab import *
plt.switch_backend('agg')
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
#如下是支持中文数字
# mpl.rcParams['font.sans-serif'] = ['SimHei']
#读取得到数据
from sklearn.ensemble import RandomForestRegressor
from tqdm import *
import itertools
from datetime import datetime,date

from sklearn.neighbors import KNeighborsRegressor
import warnings
import time

# import chinese_calendar as calendar  #
warnings.filterwarnings("ignore")

from PIL import Image
import pytesseract

def get_ocr_text(file_path):
    text = pytesseract.image_to_string(Image.open
                                       (file_path),lang='chi_sim')
    return text

def file_name(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.jpeg':
                L.append(os.path.join(root, file))
    return L

def save_word(string):
    print(string)
    date_1 = datetime.now()
    str_10 = datetime.strftime(date_1, '%Y-%m-%d')
    file = open('D:/jimmy-ye/AI/AI_supply_chain/data/decision/' + str(str_10) + '.txt', 'a')
    file.write(str(string) + '\n')


jepg_list = file_name('D:/jimmy-ye/AI/AI_supply_chain/product_design/DATA_SOURCE/DM/水果知识手册')
for i in jepg_list:
    text = get_ocr_text(i)
    save_word(text)














# -*- coding: utf-8 -*-
# @Time    : 2020/4/27 11:37
# @Author  : Ye Jinyu__jimmy
# @File    : 2rd_stock.py

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import *
import itertools
import datetime
import os
import pymysql
import copy
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 500)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
import math
import warnings
import cx_Oracle
import psycopg2



import importlib,sys
importlib.reload(sys)
LANG="en_US.UTF-8"
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


'''对比逻辑，拿到每日的订货数据，库存数据，查看到底是人更合理还是ai更加合理，
从pg库里面获取仓库维度订货次日的销售数据查看对比情况，对比会预计会有如下两个结论
1：选择哪些SKU全部由机器进行补货
2：如果采用ai补货的话，那么将会提高多少的库存周转率'''


def get_real_sales_data(wh_code, start_date):
    conn = psycopg2.connect(database="dc_rpt", user="ads", password="ads@xfsg2019", host="192.168.1.205",
                            port="3433")
    print("Opened database successfully,connected with PG DB,read sales data")
    ads_rpt_ai_wh_d_sql = """SELECT sty_code,in_qty_1d FROM ads_aig_supply_chain.ads_rpt_ai_wh_d 
    WHERE wh_code ='%s'   AND ds ='%s'""" % \
                          (wh_code, start_date)
    try:
        wh_sales            = pd.read_sql(ads_rpt_ai_wh_d_sql, conn)
    except:
        print("load data from postgres failure !")
        wh_sales            = pd.DataFrame()
        exit()
    conn.close()
    #===============================================方便后面程序的字段匹配，这里先进行字段匹配
    wh_sales.columns                = ['goods_code','次日销量']

    return wh_sales


#------------------------------------------->获取每个SKU的装箱规则
#-------------------------------------------------------------->从门店叫货目录获取产品规格和单位
def read_oracle_data(wh_name):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid = "hdapp"  # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    goods_sql = """SELECT osoc.GOODS_CODE,osoc.SPEC,osoc.UNIT FROM OA_STORE_ORDERS_CATALOG osoc  
                    WHERE osoc.ALC_CODE = (SELECT a.CODE FROM ALCSCHEME a WHERE a.name 
                    like '%s%%'AND a.NOTE LIKE '鲜丰门店%%')""" %(wh_name)
    goods = pd.read_sql(goods_sql, conn)
    goods.columns      = ['goods_code','box_gauge','unit']
    #将SKU的的iD转成list，并保存前80个，再返回值
    conn.close
    return goods



#--------------------------------------------->解析库存数据并读取日期数据，方便查找数据库并进行merge
# detail_df = pd.read_excel('D:/jimmy-ye/AI/AI_supply_chain/WAREHOUSE/V2.0/2020-04-09补货记录.xlsx',
#                           index_col=False,dtype={'goods_code': u'str'})
# # date_list = datetime.datetime.strptime(detail_df[['order_dt']].drop_duplicates()['order_dt'], '%Y-%m-%d')
# date_list = detail_df[['order_dt']].drop_duplicates()['order_dt'].\
#             apply(lambda x:datetime.datetime.strftime(x,'%Y-%m-%d')).tolist()
#
# total_df = pd.DataFrame()
# for i in date_list:
#     wh_code  = '001'
#     tomorrow = (datetime.datetime.strptime(i, '%Y-%m-%d') + datetime.timedelta(1)).strftime('%Y-%m-%d')
#     wh_sales = get_real_sales_data(wh_code,tomorrow)
#     stock_detail = detail_df[detail_df['order_dt'] == i]
#     mid_df = pd.merge(stock_detail,wh_sales,on=['goods_code'],how='left')
#     total_df = total_df.append(mid_df)
#     print('wh_sales',wh_sales)
# total_df.to_excel('./total_df.xlsx',encoding='utf_8_sig')

#---------------------------------------------》将实际的次日销售的最小耽误转换成件
total_df = pd.read_excel('./total_df.xlsx',
                          index_col=False,dtype={'goods_code': u'str'})
wh_name = '杭州'
goods = read_oracle_data(wh_name)
merge_df = pd.merge(total_df,goods,on='goods_code',how='left')
merge_df['box_gauge'] = merge_df['box_gauge'].fillna(1)
merge_df['次日销售件数'] = merge_df['次日销量'] / merge_df['box_gauge']
merge_df['次日销售件数'] = merge_df['次日销售件数'].fillna(1)
merge_df['次日销售件数'] = merge_df['次日销售件数'].apply(lambda x: round(x))
merge_df['ai期末库存']  = merge_df['期末库存'] + merge_df['建议数据量'] - merge_df['次日销售件数']
merge_df['人工期末库存']  = merge_df['期末库存'] + merge_df['最终订货'] - merge_df['次日销售件数']
merge_df.to_excel('./merge_df.xlsx',encoding='utf_8_sig')








# -*- coding: utf-8 -*-
# @Time    : 2020/5/28 23:11
# @Author  : Ye Jinyu__jimmy
# @File    : check_witout_transit_day

import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing, Holt
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn import metrics
from sklearn.model_selection import cross_val_score
import matplotlib.pyplot as plt
import os
import math
import cx_Oracle
from matplotlib.pylab import rcParams
import psycopg2
import pymysql
import time
import datetime
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 500)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
import multiprocessing
import data_process             #导入数据处理
import price_feature            #导入价格特征
import time_feature             #导入时间特征
import weather_feature          #导入天气特征
import get_holiday              #导入节假日爬取函数
import sales_feature            #导入销售特征处理函数
import forecast_model           #导入预测函数




def get_order_code(wh_name):
    print('正在读取叫货目录的数据')

    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid  = "hdapp"  # 数据库名称
    parameters  = cx_Oracle.makedsn(host, port, sid)
    conn        = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    get_orders = """SELECT osoc.GOODS_NAME,osoc.GOODS_CODE FROM OA_STORE_ORDERS_CATALOG osoc  
                    WHERE osoc.ALC_CODE = (SELECT a.CODE FROM ALCSCHEME a WHERE a.name like 
                    '%s%%'AND a.NOTE LIKE '鲜丰门店%%') AND osoc.GOODS_TYPE NOT LIKE '杨果铺%%' """%  (wh_name)
    orders = pd.read_sql(get_orders, conn)
    conn.close
    orders['GOODS_CODE']  =      orders['GOODS_CODE'].astype(str)
    orders['wh_name']     = wh_name
    return orders


#------------------------------获取在途时间
#---------------------------------------------------------------------------------------->获取在途天数的数据
def transit_days(wh_code):
    print('连接到mysql服务器...读取在途天数')
    db = pymysql.connect(host="rm-bp109y7z8s1hj3j64.mysql.rds.aliyuncs.com",
                         database="aiprediction", user="aiprediction",
                         password="aiprediction@123", port=3306, charset='utf8')
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    transit_sql = """SELECT aco.day,acg.GOODS_CODE FROM  aip_config_origin aco
                            RIGHT JOIN (SELECT acg.asvcstore_name,
                             acg.asvcstore_code,acg.goods_code,acg.goods_name,acg.origin
                            FROM  aip_config_goods acg  WHERE acg.asvcstore_code ='%s')acg
                            ON acg.asvcstore_code= aco.asvcstore_code 
                            AND acg.origin = aco.origin""" %(wh_code)
    db.cursor()
    transit = pd.read_sql(transit_sql, db)
    transit = transit.rename(columns={'goods_code':'GOODS_CODE'})
    transit['GOODS_CODE'] = transit['GOODS_CODE'].astype(str)
    return transit


wh_code = ('001','004','006','007','008','009','017','021','023','025','026','027','030','037','043','044','052')
wh_name = ('杭州','重庆','合肥','上海','浙东','浙南','无锡','成都','武汉','福建','天津','郑州','南京','西安','长沙','南昌','石家庄')

total_df = pd.DataFrame()
for i in range(len(wh_code)):

    name    = wh_name[i]
    code    = wh_code[i]
    orders  = get_order_code(name)
    print(orders.columns)
    transit = transit_days(code)
    print(transit.columns)
    merge_df    = pd.merge(orders,transit,on='GOODS_CODE',how='outer')
    print(merge_df)
    total_df = total_df.append(merge_df)
total_df.to_excel('./total_df.xls',encoding='utf_8_sig')






# -*- coding: utf-8 -*-
# @Time    : 2019/10/22 10:36
# @Author  : Ye Jinyu__jimmy
# @File    : compare_real_AI
import pandas as pd
import cx_Oracle
import os
import numpy as np

'''
该程序是对比较人工实际的下单和AI_建议下单的区别
'''

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', 500)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
# 注：设置环境编码方式，可解决读取数据库乱码问题
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
from matplotlib import pyplot as plt
import re
plt.switch_backend('agg')
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import datetime
import warnings
import time
import pymysql


#--------------------------------获取决策算法建议的补货数据
def get_AI_replenishment(today_date):
    print('连接到mysql服务器...')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123", port=3306, charset='utf8')
    print('连接成功')
    replenishment_sql = """SELECT Sku_id,Code,Warehouse_name,Sku_name,rate,Forecast_box,SS,Stock,Suggestion_qty,Account_date
     FROM dc_replenishment where Account_date = DATE(\'%s\')""" % (today_date)
    db.cursor()
    print(replenishment_sql)
    read_replenishment_sql = pd.read_sql(replenishment_sql, db)
    read_replenishment_sql['Code'] = read_replenishment_sql['Code'].astype(str)
    print(read_replenishment_sql)
    def polishing(x):
        return x['Code'].rjust(5, '0')
    read_replenishment_sql['Code'] = read_replenishment_sql.apply(lambda x: polishing(x), axis=1)
    db.close()
    return read_replenishment_sql



#--------------------------获取oracle主库里面实际下单的情况
def get_real_orders(yes_date,today):
    dbconn = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com", database="purchare_sys",
                                 user="purchare_sys",password="purchare_sys@123",port = 3306,
                                 charset='utf8')
    get_orders = """SELECT DATE_FORMAT(e.create_time,'%%Y-%%m-%%d') Account_date,e.received_warehouse_name Warehouse_name ,a.group_name ,a.goods_code Code,
  case when e.is_urgent=0 then '不是' when e.is_urgent=1 then '是' end urgent,sum(a.amount) real_orders
from p_warehouse_order_dtl a
left join p_warehouse_order e on e.id=a.warehouse_order_id
LEFT JOIN (
select b.warehouse_order_id,b.warehouse_order_dtl_id,a.plan_order_id id,b.id dtlid from p_purchase_plan_order a,p_purchase_plan_order_dtl b
 where a.plan_order_id=b.p_purchase_plan_order_id
) b on a.warehouse_order_id=b.warehouse_order_id and a.id=b.warehouse_order_dtl_id
where a.amount<>0 AND e.create_time > date ('%s') AND e.create_time < date ('%s') AND e.received_warehouse_name='杭州配送商品仓'
group by DATE_FORMAT(e.create_time,'%%Y-%%m-%%d'),e.received_warehouse_name,a.group_name,a.goods_code,a.goods_name,e.is_urgent""" \
                 %(yes_date,today)
    orders= pd.read_sql(get_orders,dbconn)
    orders['Code'] = orders['Code'].astype(str)
    # print('orders')
    # print(orders)
    # def polishing(x):
    #     x['Code'].rjust(5, '0')
    # orders['Code'] = orders.apply(lambda x: polishing(x), axis=1)
    dbconn.close()
    return orders


#由于数据库里面的补货数据的五位code对于首位数字是0的话，会自动省略，所以将真实补货数据也有7位gid进行匹配
def get_7th_code(i):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid = "hdapp"  # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    #读取的数据包括销量的时间序列，天气和活动信息
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    goods_sql = """SELECT * FROM GOODSH g WHERE g.CODE IN %s""" %(i,)
    goods = pd.read_sql(goods_sql, conn)
    conn.close
    sku_id = goods['GID'].to_list()
    return sku_id


today = datetime.date.today().strftime('%Y%m%d')
yes_date = (datetime.date.today()-datetime.timedelta(1)).strftime('%Y%m%d')

real_orders = get_real_orders(yes_date,today)
# AI_suggestion = get_AI_replenishment(yes_date)
AI_suggestion = pd.read_csv('D:/jimmy-ye/AI/AI_supply_chain/V1.0/result_start_up_10.21/' + str(yes_date) + 'merge_data.csv',
                     encoding='utf_8_sig',converters={u'Code': str})
AI_suggestion['Account_date'] = pd.to_datetime(AI_suggestion['Account_date']).dt.strftime('%Y-%m-%d')
real_orders['Account_date'] = pd.to_datetime(real_orders['Account_date']).dt.strftime('%Y-%m-%d')
merge_data = pd.merge(real_orders,AI_suggestion,on=['Warehouse_name','Code'],how='outer')
print('merge_data')
print(merge_data)
merge_data.to_csv('D:/jimmy-ye/AI/AI_supply_chain/V1.0/result_start_up_10.21/'+str(today)+'_merge_data.csv',
                  encoding='utf_8_sig')

'''查看采购目录的sku和库存的sku的交集，是否有存在在采购目录里面没有，但是库存里面有的SKU情况'''
def get_order_code():
    dbconn = pymysql.connect(host="rm-bp1jfj82u002onh2tco.mysql.rds.aliyuncs.com", database="purchare_sys",
                             user="purchare_sys", password="purchare_sys@123", port=3306,
                             charset='utf8')
    get_orders = """SELECT pcr.goods_code GOODS_CODE,pcr.goods_id,pcr.goods_name 
    FROM p_call_record pcr WHERE pcr.warehouse_id ='1000255'"""
    orders = pd.read_sql(get_orders, dbconn)
    orders['GOODS_CODE'] = orders['GOODS_CODE'].astype(str)
    dbconn.close()
    return orders


def get_stock(today_date):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid = "hdapp"  # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    #读取的数据包括销量的时间序列，天气和活动信息
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    goods_sql = """SELECT PRODUCT_POSITIONING, GOODS_CODE, GOODS_NAME, DIFFERENCE_QTY,
    FILDATE, WAREHOUSE, UP_ID, UP_TIME, INVENTORY FROM DC_hangzhou_inv
    WHERE FILDATE =  to_date('%s','yyyy-mm-dd')""" %(today_date)
    # goods_sql = """SELECT  dhi.PRODUCT_POSITIONING,dhi.GOODS_CODE,dhi.GOODS_NAME,dhi.DIFFERENCE_QTY,
  # dhi.FILDATE,dhi.WAREHOUSE,dhi.UP_ID,dhi.UP_TIME,dhi.INVENTORY   FROM DC_HANGZHOU_INV dhi INNER JOIN
  # (select * from (select * from DC_HANGZHOU_INV dhi order by dhi.UP_TIME desc) where rownum = 1)b ON
  # dhi.UP_TIME = b.UP_TIME
  #   WHERE dhi.FILDATE =  to_date('%s','yyyy-mm-dd')"""%(today_date)
    goods = pd.read_sql(goods_sql, conn)
    goods.dropna(axis=0, how='any', inplace=True)
    goods['GOODS_CODE'] = goods['GOODS_CODE'].astype(str)
    conn.close
    return goods

# orders = get_order_code()
# goods = get_stock('20191023')
# result = pd.merge(orders,goods,on='GOODS_CODE',how='inner')
# print(result)
# result.to_csv('D:/jimmy-ye/AI/AI_supply_chain/V1.0/result_start_up_10.21/'+str(20191023)+'result.csv',encoding='utf_8_sig')

# -*- coding: utf-8 -*-
# @Time    : 2020/7/10 10:21
# @Author  : Ye Jinyu__jimmy
# @File    : get_store.py

import pandas  as pd
import numpy as np
import jieba
import cpca
import pymysql
import datetime
import psycopg2
import cx_Oracle
'''

该脚本的目的是获取目前鲜丰水果门店信息的城市

'''


#----------------------------------读取门店名称和地址名字----------------------------
def get_all_store_code(days):
    three_day_before = (datetime.datetime.strptime(days, '%Y%m%d') - datetime.timedelta(3)).strftime('%Y%m%d')
    conn_pg = psycopg2.connect(database="proj_xfsg", user="LTAI4GEpn2DpdummgN2sBPqg",
                            password="0HVomPYRaIddrxvuFr2MQJX9zh9Ym1",
                            host="lightning.cn-hangzhou.maxcompute.aliyun.com",
                            port="443")
    print("Opened database successfully,connected with PG DB,read store_code")
    sql = """SELECT str_code,str_name,addr,comp_name,city FROM dim_xf_str WHERE ds = '20200515' AND str_code in 
                    (SELECT str_code FROM ads_newstr_purchase_amount_more_d 
                    WHERE ds > '%s' AND  LENGTH(str_code) > 4 
                    and str_code < 80000000 GROUP BY str_code) and comp_name like '%%公司' ORDER BY comp_name""" % (three_day_before)
    # try:
    final_df = pd.read_sql(sql, conn_pg)
    conn_pg.close()
    return final_df



#----------------------------------->依据得到的门店列表，汇总得到对应的城市名称
def fill_city(dataframe):
    df          = dataframe.fillna('空')
    df['new_address']   = df['city']+ df['str_name'] + df['addr']
    location_str    =   df['new_address'].to_list()
    df_city = cpca.transform(location_str, cut=False, lookahead=3)
    df['city_name'] = df_city['市']
    def check_city_name(x):
        if x['city_name'] == '':
            #-----------------加了一个市str是为了保证city格式一致
            city_name = x['comp_name'].split('公司', 1)[0] + '市'
        else:
            city_name = x['city_name']
        return city_name
    df['city_name'] = df.apply(lambda x: check_city_name(x), axis=1)
    df = df.drop(columns = ['city','addr','new_address'])
    return df


#------------------------------------------->依据城市公司和城市维度进行计算
def algorithm_each(days):
    final_df    =   get_all_store_code(days)
    df          =   fill_city(final_df)
    df.to_excel('./df.xls')
    comp_name_list  =   set(df['comp_name'].to_list())
    print(comp_name_list)
    for comp_name in comp_name_list:
        print(comp_name)
        mid_df  =   df[df['comp_name'] == comp_name]
        city_name_list  =  set(mid_df['city_name'].to_list())
        for city_name_old in city_name_list:
            city_name = city_name_old.split('市', 1)[0]
            print(city_name)
            #--------------------------------------->接下来是要计算每个城市下面所有门店的计算
            city_store  =   mid_df[mid_df['city_name'] ==city_name_old]['str_code'].to_list()




#--------------------------------------->获取叫货目录的商品和对应的code
def get_store_goods(wh_name):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid  = "hdapp"  # 数据库名称
    parameters  = cx_Oracle.makedsn(host, port, sid)
    conn        = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    get_orders = """SELECT osoc.GOODS_NAME,osoc.GOODS_CODE FROM OA_STORE_ORDERS_CATALOG osoc  
                    WHERE osoc.ALC_CODE = (SELECT a.CODE FROM ALCSCHEME a WHERE a.name like 
                    '%s%%'AND a.NOTE LIKE '鲜丰门店%%') """%\
                 (wh_name)   # and osoc.GOODS_CODE = '05990'
    print(get_orders)
    orders = pd.read_sql(get_orders, conn)
    conn.close
    orders['GOODS_CODE']  =      orders['GOODS_CODE'].astype(str)
    code_list             =      orders['GOODS_CODE'].to_list()
    name_list             =      orders['GOODS_NAME'].to_list()
    print(str(wh_name)+',叫货目录读取完成,共有商品%s'%(len(code_list)))
    return code_list,name_list










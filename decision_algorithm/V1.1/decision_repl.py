# -*- coding: utf-8 -*-
# @Time    : 2019/9/25 9:44
# @Author  : Ye Jinyu__jimmy
# @File    : decision_repl
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
import re
plt.switch_backend('agg')
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import datetime
import warnings
import time
import pymysql


# import chinese_calendar as calendar  #
warnings.filterwarnings("ignore")

#--------------------------------------------------------------获取商品的五位码
def read_oracle_data(i):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid = "hdapp"  # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    #读取的数据包括销量的时间序列，天气和活动信息
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    goods_sql = """SELECT g.GID,g.CODE FROM GOODSH g WHERE g.GID IN %s""" %(i,)
    goods = pd.read_sql(goods_sql, conn)
    goods = goods.rename(index=str, columns={'GID': 'Sku_id','CODE':'Code'})
    #将SKU的的iD转成list，并保存前80个，再返回值
    conn.close

    return goods

#----------------------------------------------------------------对数据预测数据获取，并进行处理
def get_forecast(data):
    #获取五位的商品码
    print('data')
    print(data)
    data_sku_id = data[['Sku_id']]
    data_sku_id = data_sku_id.drop_duplicates()
    data_sku_id = data_sku_id.reset_index(drop=True)
    sku_id_list = data_sku_id['Sku_id'].to_list()
    gid_tuple = tuple(sku_id_list)
    print('gid_tuple',gid_tuple)
    sku_code= read_oracle_data(gid_tuple)
    print('sku_code',sku_code)
    data_sku_id = pd.merge(data_sku_id,sku_code,on='Sku_id',how='inner')
    final = pd.merge(data_sku_id,data,on='Sku_id',how='right')
    return final

#取出所有sku对应的装箱比例
def resource_convert(i):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"  # 端口
    sid = "hdapp"  # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    #读取的数据包括销量的时间序列，天气和活动信息
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    resource_sql = """SELECT s.QPC,COUNT(s.QPC) 总次数 FROM STKOUTDTL s WHERE s.GDGID= %s AND rownum < 1000
AND s.CLS='统配出' GROUP BY s.QPC ORDER BY count(s.QPC)DESC""" %(i)
    resource = pd.read_sql(resource_sql, conn)
    #将SKU的的iD转成list，并保存前80个，再返回值
    conn.close
    resource = resource.sort_values(by=['总次数'],ascending=False)
    resource_conver_rate = resource['QPC'].iloc[0]
    return resource_conver_rate

# resource_conver_rate = resource_convert(3006430)
# print(resource_conver_rate)
#如下是要拿到所有的sku对应的转换比例是多少

# 获取五位的商品码
def get_convert_rate(data):
    data_sku_id = data[['Sku_id']]
    data_sku_id = data_sku_id.drop_duplicates()
    data_sku_id = data_sku_id.reset_index(drop=True)
    sku_id_list = data_sku_id['Sku_id'].to_list()
    sku_convert_rate = pd.DataFrame(columns={'Sku_id','rate'})
    for sku in sku_id_list:
        convert_rate = resource_convert(sku)
        # sku_convert_rate = sku_convert_rate.append({'Sku_id': sku}, ignore_index=True)
        sku_convert_rate = sku_convert_rate.append({'rate': convert_rate,'Sku_id': sku}, ignore_index=True)
    return sku_convert_rate


#将资源转换因子和产品相匹配
def get_final_forecast(data,sku_convert):
    forecast = get_forecast(data)
    final = pd.merge(forecast,sku_convert,on='Sku_id',how='left')
    final['Forecast_box'] = final['Forecast_qty']/final['rate']
    final['Forecast_box'] = final['Forecast_box'].apply(lambda x: round(x))
    return final


#获取昨天，今天，明天和后天共四天的日期str
def get_all_date():
    yes_date = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d')
    today_date = datetime.date.today().strftime('%Y%m%d')
    tomorrow_date = (datetime.date.today() + datetime.timedelta(1)).strftime('%Y%m%d')
    TDAT_date = (datetime.date.today() + datetime.timedelta(2)).strftime('%Y%m%d')


    # yes_date = (datetime.date.today() - datetime.timedelta(2)).strftime('%Y%m%d')
    # tomorrow_date = datetime.date.today().strftime('%Y%m%d')
    # today_date = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d')
    # TDAT_date = (datetime.date.today() + datetime.timedelta(1)).strftime('%Y%m%d')


    return yes_date,today_date,tomorrow_date,TDAT_date


#----------------获取预测数据
def get_original_forecast(today_date):
    print('连接到mysql服务器...')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123", port=3306, charset='utf8')
    print('连接成功,开始读取预测数据')
    weather_sql = """SELECT * FROM dc_forecast where Update_time = DATE(\'%s\')"""%(today_date)
    db.cursor()
    read_orignal_forecast = pd.read_sql(weather_sql, db)
    db.close()
    return read_orignal_forecast

#定义函数从Oracle数据库里面选择每日的库存数据
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


#设置主函数用于读取数据库的库存和预测数据
def get_db(yes_date,today_date):
    stock_data = get_stock(today_date)
    original_forecast = get_original_forecast(today_date)
    return stock_data,original_forecast


#-----------------------------------------------这里是进行编码转换，基本的数据
def cleaning_data(original_forecast, stock_data):
    sku_convert_rate = get_convert_rate(original_forecast)
    # sku_convert_rate.to_csv('D:/jimmy-ye/AI/AI_supply_chain/data/decision/sku_convert_rate.csv',encoding='utf_8_sig')
    final = get_final_forecast(original_forecast,sku_convert_rate)
    #以下需要对每日的盘点后的库存进行解析，并保存
    # df = data_xls.parse(sheet_name='采购更新模块', header=1, converters={u'订货编码': str})
    data_stock = stock_data[['GOODS_CODE','GOODS_NAME','DIFFERENCE_QTY','INVENTORY']]
    data_stock = data_stock.dropna(axis=0,how='any')
    data_stock.columns = ['Code','Sku_name','Stock','Inventory']
    return data_stock,final



'''分仓采购场景中默认提前期是一天，对于协同的压力将会在统采的嘉兴仓中产生'''
#=========================================================获取在途库存的数据
#-----------------------------------------------先确定决策的日
def get_orders_time(data_xls):
    df = data_xls.parse(sheet_name='采购更新模块')
    columns = list(df.columns.values)
    date = re.sub("\D", "", columns[4])
    date_time = datetime.datetime.strptime(date, '%Y%m%d')
    date_delta = date_time - datetime.timedelta(1)
    date_str_today = datetime.datetime.strftime(date_time, "%Y-%m-%d")
    date_str_yesterday = datetime.datetime.strftime(date_delta, "%Y-%m-%d")
    date_TDAT = date_time + datetime.timedelta(2)
    date_TDAT = datetime.datetime.strftime(date_TDAT, "%Y-%m-%d")
    return date_str_yesterday,date_str_today,date_TDAT


#<-----------------------------------------------------------------------—读取数据库的数据
# def database_read(date_str_yesterday,date_str_today):
#     dbconn = pymysql.connect(host="rm-bp1jfj82u002onh2tco.mysql.rds.aliyuncs.com", database="purchare_sys",
#                              user="purchare_sys",password="purchare_sys@123",port = 3306,
#                              charset='utf8')
#     get_orders = """SELECT DATE_FORMAT(e.create_time,'%%Y-%%m-%%d') 订单日期,e.received_warehouse_name 仓位,a.group_name 采购组,a.goods_code 商品代码,a.goods_name 商品名称,
#   case when e.is_urgent=0 then '不是' when e.is_urgent=1 then '是' end 是否紧急订货,sum(a.amount) 订单箱数
# from p_warehouse_order_dtl a
# left join p_warehouse_order e on e.id=a.warehouse_order_id
# LEFT JOIN (
# select b.warehouse_order_id,b.warehouse_order_dtl_id,a.plan_order_id id,b.id dtlid from p_purchase_plan_order a,p_purchase_plan_order_dtl b
#  where a.plan_order_id=b.p_purchase_plan_order_id
# ) b on a.warehouse_order_id=b.warehouse_order_id and a.id=b.warehouse_order_dtl_id
# where a.amount<>0 AND e.create_time > date ('%s') AND e.create_time < date ('%s') AND e.received_warehouse_name='杭州配送商品仓'
# group by DATE_FORMAT(e.create_time,'%%Y-%%m-%%d'),e.received_warehouse_name,a.group_name,a.goods_code,a.goods_name,e.is_urgent""" \
#                  %(date_str_yesterday,date_str_today)
#     print(get_orders)
#     orders= pd.read_sql(get_orders,dbconn)
#     dbconn.close()
#     return orders


def algorithm_SS(final, tomorrow_date, TDAT_date):
    #=================================================以下SS的计算,K取值2
    Code = list(set(final['Code'].tolist()))
    data_SS = pd.DataFrame()
    for i in Code:
        data_mid = final[final['Code'] == i]
        forecast = data_mid['Forecast_box'].values
        std = np.sqrt(((forecast - np.mean(forecast)) ** 2).sum() / (forecast.size - 1))
        data_SS = data_SS.append({'Code': i,'SS': std,}, ignore_index=True)
    # print(data_SS)
    data_merge = pd.merge(final,data_SS,on='Code',how='left')
    data_merge.fillna(method='ffill')
    data_final = data_merge[data_merge['Account_date'] == TDAT_date ]

    #如下是获得第二天的预测补货值，对二配行为进行部分规避
    forecast_tomorrow = final[final['Account_date'] == tomorrow_date]
    forecast_tomorrow = forecast_tomorrow[['Code','Forecast_box']]
    forecast_tomorrow= forecast_tomorrow.rename(index=str, columns={'Forecast_box': 'Forecast_box_tomorrow'})
    data_final = pd.merge(data_final,forecast_tomorrow,on='Code',how='inner')
    def compare(x):
        if x['SS']>x['Forecast_box']:
            return x['Forecast_box']
        else:
            return x['SS']
    print(data_final)
    data_final['SS'] = data_final.apply(lambda x: compare(x), axis=1)
    data_final = data_final[['Sku_id','Code','Dc_name','Dc_code','Munit','Wrh','Warehouse_name','Sku_name','rate','Forecast_box','SS','Forecast_box_tomorrow']]
    return data_final


#-----------------------------------------------对流转时间表进行数据清洗操作
# def get_circulation():
#     path_circulation = 'D:/jimmy-ye/AI/AI_supply_chain/data/decision/流转天数模板.xlsx'
#     data_xls = pd.ExcelFile(path_circulation)
#     df = data_xls.parse(sheet_name='采购更新模块',header=1,converters = {u'订货编码':str})
#     df.columns =['产品定位', '订货编码', '商品名称', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
#     df_time = data_xls.parse(sheet_name = '发出地到杭州仓流转时间',header=0)
#     df_time.columns =['发出地', '时间']
#     # print([column for column in df])
#
#     def compare_city_time(x):
#         reslut = df_time[df_time['发出地']==x]
#         reslut = reslut.reset_index()
#         time = reslut['时间'].iloc[0]
#         return time
#     for i in range(1,13):
#         df[str(i)] = df[str(i)].apply(lambda x:compare_city_time(x))
#     print(df)


def main_function(data_stock,data_final):
    data_stock['Code'] = data_stock['Code'].astype(str)
    data_stock = data_stock[['Code','Stock','Inventory']]
    '''选择外连接合并，是因为存在仓库的表格并没有数据，但是实际情况是需要订货的，因此采取外连接，并进行补零操作'''
    data_merge = pd.merge(data_final,data_stock,on='Code',how='outer')
    data_merge= data_merge.fillna(0)
    print('data_merge')
    print(data_merge)
    def compare_predict(x):
        predict = x['Inventory'] + x['Stock']
        if predict <= x['Forecast_box_tomorrow']:
            return x['Inventory'] - x['Forecast_box_tomorrow']
        else:
            return x['Stock']
    data_merge['Stock_mid'] = data_merge.apply \
        (lambda x: compare_predict(x), axis=1)

    def calculate_final(x):
        demand = x['Forecast_box'] + x['SS']
        if demand <= x['Stock_mid']:
            return 0
        else:
            return round(demand - x['Stock_mid'])
    data_merge['Suggestion_qty'] = data_merge.apply \
        (lambda x: calculate_final(x), axis=1)
    data_merge['Update_time'] = pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data_merge['Account_date'] = datetime.date.today().strftime('%Y-%m-%d')
    data_merge = data_merge.drop(['Forecast_box_tomorrow','Inventory','Stock_mid'], axis=1)
    return data_merge

#定义主计算调度逻辑进行函数的汇总
def main():
    yes_date, today_date, tomorrow_date, TDAT_date = get_all_date()
    stock_data,original_forecast = get_db(yes_date, today_date)
    data_stock, final = cleaning_data(original_forecast, stock_data)
    data_final = algorithm_SS(final, tomorrow_date, TDAT_date)
    AI_suggestion = main_function(data_stock, data_final)
    print('AI_suggestion')
    print(AI_suggestion)
    db = connectdb()
    drop_data(db,today_date)
    if AI_suggestion.empty:
        print("The data frame is empty")
        print("result:1")
        closedb(db)
    else:
        insertdb(db,AI_suggestion)
        closedb(db)
        print("result:1")


def connectdb():
    print('连接到mysql服务器...')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123",port=3306, charset='utf8')
    print('连接成功')
    return db

#《---------------------------------------------------------------------------------------------------------------------删除重复日期数据
def drop_data(db,today_date):
    cursor = db.cursor()
    # date_parameter = datetime.date.today().strftime('%Y-%m-%d')
    # sql = """delete from dc_replenishment"""
    sql = """delete from dc_replenishment where Account_date = str_to_date(\'%s\','%%Y-%%m-%%d')"""%(today_date)
    cursor.execute(sql)

#<======================================================================================================================
def insertdb(db,data):
    cursor = db.cursor()
    # param = list(map(tuple, np.array(data).tolist()))
    data_list = data.values.tolist()
    print('data_list')
    print(data_list)
    sql = """INSERT INTO dc_replenishment (Sku_id,Code,Dc_name,
    Dc_code,Munit,Wrh,Warehouse_name,Sku_name,rate,Forecast_box,
    SS,Stock,Suggestion_qty,update_time,Account_date)
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql, data_list)
        print("所有品牌的sku数据插入数据库成功")
        db.commit()
    except OSError as reason:
        print('出错原因是%s' % str(reason))
        db.rollback()
#<=============================================================================
def closedb(db):
    db.close()


#《============================================================================主函数入口
if __name__ == '__main__':
    try:
        main()
    except OSError as reason:
        print('出错原因是%s'%str(reason))
        print ("result:0")

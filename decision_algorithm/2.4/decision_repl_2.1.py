# -*- coding: utf-8 -*-
# @Time    : 2020/3/30 10:55
# @Author  : Ye Jinyu__jimmy
# @File    : decision_program

import sys
print(sys.version)
import pandas as pd
import cx_Oracle
import os
import numpy as np

'''2019-11-29日与产品中心沟通，在安全库存处进行优化，增大安全库存的设置'''
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
import tqdm
import pymysql
import redis

client = redis.Redis(host="192.168.1.180",port=6379, decode_responses=True,socket_connect_timeout=6000)



def mkdir(path):
    folder = os.path.exists('/root/ai/wh_repl/program/'+path)
    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs('/root/ai/wh_repl/program/'+path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print(
            "----生成新的文件目录----")
    else:
        print("当前文件夹已经存在")

def print_in_log(string):
    print(string)
    date_1 = datetime.datetime.now()
    str_10 = datetime.datetime.strftime(date_1, '%Y%m%d')
    file = open('/root/ai/wh_repl/program/log/' + 'log_decision' + str(str_10) + '.txt', 'a')
    file.write(str(string) + '\n')


# def print_in_log(string):
#     print(string)
#     date_1 = datetime.datetime.now()
#     str_10 = datetime.datetime.strftime(date_1, '%Y-%m-%d')
#     file = open('./log/log_decision' + str(str_10) + '.txt', 'a')
#     file.write(str(string) + '\n')

warnings.filterwarnings("ignore")




#-------------------------------------------------------------------------------------------->按照历史的装箱选择装箱的规则
def resource_convert(i):
    host = "192.168.1.11"  # 数据库ip
    port = "1521"           # 端口
    sid = "hdapp"           # 数据库名称
    parameters = cx_Oracle.makedsn(host, port, sid)
    #读取的数据包括销量的时间序列，天气和活动信息
    # hd40是数据用户名，xfsg0515pos是登录密码（默认用户名和密码）
    conn = cx_Oracle.connect("hd40", "xfsg0515pos", parameters)
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    resource_sql = """SELECT s.QPC,COUNT(s.QPC) 总次数 FROM STKOUTDTL s WHERE s.GDGID= %s AND rownum < 200
AND s.CLS='统配出' GROUP BY s.QPC ORDER BY count(s.QPC) DESC""" %(i)
    resource = pd.read_sql(resource_sql, conn)
    #将SKU的的iD转成list，并保存前80个，再返回值
    conn.close
    resource = resource.sort_values(by=['总次数'],ascending=False)
    if resource.empty ==True:
        resource_conver_rate = 1
    else:
        resource_conver_rate = resource['QPC'].iloc[0]
    return resource_conver_rate



#-------------------------------------------------------------------------------->读取转运单的日期
def get_receiver_stock(wh_code,T,T_1):
    print_in_log('连接服务器正在进行')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123", port=3306, charset='utf8')
    print_in_log('读取叫货目录的数据，读取T天的预计到货的SKU')
    receiver_stock_sql = """  SELECT ppod.goods_name,ppod.goods_id,ppod.actual_packing_qty,ppod.packing_spe,packing_qty 
                            FROM p_purchase_order_dtl ppod  WHERE ppod.id IN 
                        (select purchase_order_dtl_id from p_transport_order_dtl where transport_order_id in (
                            (SELECT pto.id FROM p_transport_order pto WHERE 
                            pto.receiving_warehouse_id LIKE '%s%%'  
                                AND pto.expected_arrival_time >= date('%s')
                                AND pto.expected_arrival_time < date('%s')
                                )))""" % (wh_code,T,T_1)
    db.cursor()
    print_in_log(receiver_stock_sql)
    receiver_stock = pd.read_sql(receiver_stock_sql, db)
    return receiver_stock


#————————————————————读取数据库中的特殊：这里选择出对应商品在当前节点的逻辑规则,当前只加入安全库存的设置————————————————————
def special_rule_data(wh_code):
    print_in_log('连接到mysql服务器...,正在读取特殊规则数据')
    db       = pymysql.connect(host="rm-bp109y7z8s1hj3j64.mysql.rds.aliyuncs.com",
                         database="aiprediction", user="aiprediction",
                         password="aiprediction@123", port=3306, charset='utf8')
    # 查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    rule_sql = """SELECT goods_code,expert_dim,expert_rule from expert_rule_dtl 
                    WHERE warehouse_code='%s' """ % (wh_code)
    db.cursor()
    rule     = pd.read_sql(rule_sql, db)
    print_in_log('连接成功,预测数据读取完成')
    db.close()
    rule['goods_code'] = rule['goods_code'].astype(str)
    rule.columns       = ['sku_code','expert_dim','expert_rule']
    return rule

#————————————————传入对应的五位码，返回对应的安全库存的设置量——————————————————
def get_special_rule(code,data):
    rule_data       = data[data['expert_dim'] == '安全库存']
    print('code',code,rule_data['sku_code'].to_list())
    if code in rule_data['sku_code'].to_list():
        print('存在特殊规则')
        expert = rule_data[rule_data['sku_code'] == code]['expert_rule'].iloc[0]
        if expert == '不需要':
            expert_rule = 0
        elif expert == '少量':
            expert_rule = 1
        elif expert == '适当':
            expert_rule = 2
        else:
            expert_rule = 3
    else:
        print('不存在特殊规则')
        expert_rule = 2
    return expert_rule


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



#----------------------------------------------------------------------------------------------------->获取商品的装箱因子
def get_convert_rate(Id,wh_code):
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    #---------------------------------------------------------------------------------------------->先叫货目录，再特殊值处理
    print_in_log('连接到mysql服务器...')
    db    = pymysql.connect(host      = "rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                            database  = "purchare_sys",
                            user      = "purchare_sys",
                            password  = "purchare_sys@123",
                            port      = 3306,
                            charset   = 'utf8')
    print_in_log('读取叫货目录的数据，进行装箱规则的计算')
    box_gauge_sql = """SELECT pcr.goods_code,pcr.mini_order,pcr.warehouse_name,pcr.template_effective
                            FROM p_call_record pcr
                            WHERE pcr.warehouse_code LIKE '%s%%' AND pcr.group_name
                            NOT LIKE '物料%%'""" % (wh_code)
    db.cursor()
    print_in_log(box_gauge_sql)
    box_gauge               = pd.read_sql(box_gauge_sql, db)
    box_gauge['goods_code']  = box_gauge['goods_code'].astype(str)
    wh_name = box_gauge['warehouse_name'].iloc[1][0:2]
    print_in_log('wh_name'+str(wh_name))
    db.close()
    goods = read_oracle_data(wh_name)
    #--------------------------------------------------------------------->将装箱规则与最小起订量结合
    box_gauge = box_gauge.drop(columns='warehouse_name',axis=1)
    box_gauge.columns   = ['goods_code','mini_order','template_effective']
    sku_df = pd.merge(goods,box_gauge,on='goods_code',how = 'left')
    #-------------------------------------------------------------------->加入特殊逻辑处理一个品多个采购组的逻辑
    sku_code_list       =   list(set(sku_df['goods_code'].to_list()))
    new_box             =   pd.DataFrame()
    for code in sku_code_list:
        mid_box     = sku_df[sku_df['goods_code'] == code]
        if len(mid_box) == 1:
            mid_box = mid_box[['goods_code','box_gauge','unit','mini_order']]
            new_box = new_box.append(mid_box)
        else:
            mid_box     = mid_box.sort_values(by='template_effective',axis=0,ascending=False)
            mid_box_01  = mid_box.head(1)
            mid_box_01  = mid_box_01[['goods_code','box_gauge','unit','mini_order']]
            new_box     = new_box.append(mid_box_01)

    def verify_box_gauge(x):
        print(x['box_gauge'])
        if x['box_gauge'] == 'null' or x['box_gauge'] == '0' or x['box_gauge'] == '1':
            print_in_log('该资源规格box_gauge为"NAN or 0 or 1",选择历史的装箱规格')
            sku_code    = x['goods_code']
            rate        = resource_convert(sku_code)

        else:
            rate        = x['box_gauge']
        return rate
    new_box['box_gauge'] = new_box.apply(lambda x: verify_box_gauge(x), axis=1)
    new_box['box_gauge'] = new_box['box_gauge'].astype(float)
    new_box.columns      = ['Sku_id','rate','Munit','min_order']
    return new_box


#------------------------------------------------------------------------------------------------>将资源转换因子和产品相匹配
def forecast_box_convert(data,sku_convert):
    #------------------------------------------------------>选择inner是保证叫货目录有的就可以
    final = pd.merge(data,sku_convert,on = 'Sku_id', how = 'inner')
    final['Forecast_box'] = final['Forecast_qty']/final['rate']
    final['Forecast_box'] = final['Forecast_box'].apply(lambda x: round(x))
    # final.to_csv('./final_co.csv',encoding='utf_8_sig')
    return final

#------------------------------------------------------------------------------------------------------>获取未来一周的日期
def get_all_date(Id,today_date):
    T   = datetime.datetime.strptime(today_date, '%Y-%m-%d')
    T_1 = (T + datetime.timedelta(1)).strftime('%Y%m%d')
    #--------------------------------------------------------------->与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #================================================================>与redis进行交互
    return today_date,T_1

#----------------------------------------------------------------------------------------------------------->获取预测数据
def get_original_forecast(wh_code,today_date):
    print_in_log('连接到mysql服务器...')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123", port=3306, charset='utf8')
    print_in_log('连接成功,开始读取预测数据')
    forecast_sql = """SELECT * FROM dc_forecast df WHERE df.Dc_code='%s' AND 
                        df.Update_time = DATE('%s')"""%(wh_code,today_date)
    db.cursor()
    read_original_forecast = pd.read_sql(forecast_sql, db)
    print_in_log('连接成功,预测数据读取完成')
    db.close()

    return read_original_forecast

#--------------------------------------------------------------------------------->定义函数从mysql数据库里面选择每日的库存数据
def get_stock(wh_code,today_date):
    print_in_log('连接到mysql服务器...,正在进行库存数据的读取')
    db = pymysql.connect(host="rm-bp109y7z8s1hj3j64.mysql.rds.aliyuncs.com",
                         database="aiprediction", user="aiprediction",
                         password="aiprediction@123", port=3306, charset='utf8')
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    stock_sql = """SELECT scode,sname,goods_code,goods_name,inventory,inventory_left from aip_order_detail 
                    WHERE scode='%s' AND order_dt AND order_dt = '%s'""" %(wh_code,today_date)
    db.cursor()
    stock = pd.read_sql(stock_sql, db)
    print_in_log('连接成功,库存数据读取完成')
    db.close()
    stock['goods_code'] = stock['goods_code'].astype(str)
    return stock

'''该设置的参数是在试水阶段采用特定的SKU进行补货的方式和方法'''
#--------------------------------------------------------先设置需要的SKU，ssd=selected_sku_dataframe
def test_sku(data):
    ssd = pd.DataFrame({'GOODS_CODE':['65550','07540','07310','11620','11600','16010','16040','07350','13160','06390',
                                      '05200','01310','08890','05020','07640','11120','06850','65770','07600','12190',
                                      '01270','07340','07300','11650','07950','11710','12130','01020','07220','12240'
                                      ]})
    result_data = pd.merge(data,ssd,on='GOODS_CODE',how='inner')
    return result_data

#----------------------------------------------------------------------------------------------->读取数据库的库存和预测数据
def get_db(Id,wh_code,today_date):
    stock_data = get_stock(wh_code,today_date)
    stock_data = stock_data.drop_duplicates(subset=['goods_code'], keep='first')
    #+++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    #使用测试的SKU作为使用点
    # stock_data = test_sku(stock_data)
    original_forecast   = get_original_forecast(wh_code,today_date)
    #+++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    return stock_data,original_forecast




#---------------------------------------------------------------------------------------->获取在途天数的数据
def transit_days(wh_code):
    print_in_log('连接到mysql服务器...读取在途天数')
    db = pymysql.connect(host="rm-bp109y7z8s1hj3j64.mysql.rds.aliyuncs.com",
                         database="aiprediction", user="aiprediction",
                         password="aiprediction@123", port=3306, charset='utf8')
    #查看详细的出库数据，进行了日期的筛选，查看销量签50名的SKU
    transit_sql = """SELECT aco.day transit_day,acg.goods_code FROM  aip_config_origin aco
                            RIGHT JOIN (SELECT acg.asvcstore_name,
                             acg.asvcstore_code,acg.goods_code,acg.goods_name,acg.origin
                            FROM  aip_config_goods acg  WHERE acg.asvcstore_code ='%s')acg
                            ON acg.asvcstore_code= aco.asvcstore_code 
                            AND acg.origin = aco.origin""" %(wh_code)
    db.cursor()
    transit = pd.read_sql(transit_sql, db)

    # ---------------------------------------------------------------------------------->读取本地excel获取采摘时间
    def get_ready_goods():
        # get_ready_goods = pd.read_excel('./采摘单品汇总.xlsx', converters={u'商品代码': str})
        get_ready_goods = pd.read_excel('/root/ai/wh_repl/program/采摘单品汇总.xlsx', converters={u'商品代码': str})
        get_ready_goods.columns = ['goods_name', 'goods_code', 'get_days']
        get_ready_goods = get_ready_goods[['goods_code', 'get_days']]
        return get_ready_goods

    #--------------------------------------------------------------------------->
    get_ready_goods = get_ready_goods()
    transit         = pd.merge(transit,get_ready_goods,on='goods_code',how='left')
    transit['day']  = transit['transit_day'] +transit['get_days']
    transit         = transit.drop(['transit_day','get_days'], axis=1)

    #----------------------------------------------------------------------------->如果有空值按照上海到分仓的在途进行维护
    #获取上海仓到分仓的在途时间
    day_sql = """SELECT aco.day FROM aip_config_origin aco 
                WHERE aco.origin = '上海' AND aco.asvcstore_code = '%s'""" %(wh_code)
    db.cursor()
    df      = pd.read_sql(day_sql, db)
    if len(df) > 0:
        day     = (pd.read_sql(day_sql, db)).iloc[0]

        transit = transit.fillna(day)
    else:
        day     = 1
        transit = transit.fillna(day)
    db.close()
    print_in_log('在途数据读取完成，长度%d'%(len(transit)))
    transit['goods_code'] = transit['goods_code'].astype(str)
    return transit




#----------------------------------------------------------------------------------------->先从本地获取在途
def transit_bendi_days():
    data_xls = pd.ExcelFile('D:/AI/xianfengsg/decision_algorithm/2.0/商品配置.xlsx')
    transit  = data_xls.parse(sheet_name='Sheet2',header=0,usecols=[0,4],
                      converters={u'商品代码': str})
    transit.columns       = ['goods_code','day']
    transit['goods_code'] = transit['goods_code'].astype(str)
    return transit

#--------------------------------------------------------------------------------------->这里是进行单位换算，在途天数确认
def cleaning_data(Id,wh_code,original_forecast,stock_data):
    #------------------------------------------------>读取装箱规则数据
    sku_convert_rate = get_convert_rate(Id,wh_code)
    #+++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    #------------------------------------------------->读取预测销量
    final            = forecast_box_convert(original_forecast,sku_convert_rate)
    #+++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #+++++++++++++++++++++++++++++++++++++++++++>与redis进行交互

    #------------------------------------------------->读取在途数据
    transit          = transit_days(wh_code)
    # transit          = transit_bendi_days()

    #-------------------------------------------------->以下需要对每日的盘点后的库存进行解析，并加入在途的数据
    data_stock       = stock_data[['goods_code','goods_name','inventory','inventory_left']]

    #---------------------先读取在途的数据，如果没有维护，为了不让系统报错，选择1天作为代替
    merge_stock      = pd.merge(data_stock,transit,on='goods_code',how='left').fillna(1)
    merge_stock.columns = ['Sku_code','Sku_name','Stock','Inventory','LT']  #----------->stock期初，inventory期末

    merge_stock  = merge_stock.dropna(axis=0,how='any')
    print_in_log('数据清洗完成')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    return merge_stock,final


#------------------------------------------------------------------------------>加入对业务保质期进行判断
def expiration_rule(code,data):
    rule_data       = data[data['expert_dim'] == '保质期']

    if code in rule_data['sku_code'].to_list():
        print('存在特殊规则')
        expert = rule_data[rule_data['sku_code'] == code]['expert_rule'].iloc[0]
        if expert == '0':
            expert_rule = 0
        else:
            expert_rule = 1
    else:
        print('不存在特殊规则')
        expert_rule = 1
    return expert_rule

#----------------------------------------------------->将未来某个时间的需求独立出来封装成函数,加入铺货节奏
def distribution_stock(data_stock,data_merge,rule_data,T,T_1):
    #-------------------------------------------------------------------------------->计算到货时间点的需求
    mid_df        = pd.DataFrame()
    Code_list     = list(set(data_stock['Sku_code'].tolist()))
    for code in Code_list:
        expert_rule = expiration_rule(code,rule_data)
        print('expert_rule',expert_rule)
        days            = data_stock[data_stock['Sku_code'] == code]['LT'].iloc[0]
        Account_date = (datetime.datetime.strptime(T, '%Y-%m-%d') + datetime.timedelta(days)).strftime('%Y-%m-%d')
        mid_merge    = data_merge[data_merge['Sku_code'] == code]
        print(datetime.datetime.strptime(Account_date, '%Y-%m-%d').weekday(),code,'\n',Account_date)
        if datetime.datetime.strptime(Account_date, '%Y-%m-%d').weekday() == 3 and expert_rule == 1:

            print_in_log('%s,code需要加入铺货的节奏'%code)
            print_in_log(datetime.datetime.strptime(Account_date, '%Y-%m-%d'))
            # -------------------------------------------------->在途显示，超过预测按mean处理->仅在此版本中进行特殊处理
            if days >= 6:
                mid_merge['Forecast_box'] = 3 * round(np.mean(mid_merge['Forecast_box'].values))
                each_df = mid_merge[mid_merge['Account_date'] == T_1]
                each_df['Account_date'] = Account_date
            else:
                new_merge                 = mid_merge[mid_merge['Account_date'] >= Account_date]
                forecast_qty              = new_merge['Forecast_box'][0:3].sum()
                each_df                   = mid_merge[mid_merge['Account_date'] == Account_date]
                new_merge['Forecast_box'] = forecast_qty
            mid_df = mid_df.append(each_df)
        else:
            print_in_log('%s,code，不必要加入铺货的节奏' % code)
            # -------------------------------------------------->在途显示，超过预测按mean处理->仅在此版本中进行特殊处理
            if days >= 6:
                mid_merge['Forecast_box'] = round(np.mean(mid_merge['Forecast_box'].values))
                each_df = mid_merge[mid_merge['Account_date'] == T_1]
                each_df['Account_date'] = Account_date
            else:
                each_df = mid_merge[mid_merge['Account_date'] == Account_date]
            mid_df = mid_df.append(each_df)
    return mid_df


'''当下阶段就需要考虑预计到货那天的预测需求,安全库存的计算，与销量预测的修正'''
def algorithm_SS(Id,wh_code,final,data_stock,T,T_1,rule_data):
    #--------------------------------------------------------------------------------->以下SS的计算,K根据实际的规则进行选择
    Code    = list(set(final['Sku_code'].tolist()))
    data_SS = pd.DataFrame()
    for i in Code:
        k        = get_special_rule(i,rule_data)
        if k == 0:
            std = 0
        else:
            data_mid = final[final['Sku_code'] == i]
            forecast = data_mid['Forecast_box'].values
            #---------------------------------------------------------------------------->特殊商品由人为设定，其他为系统自动判断
            std      = k * (np.sqrt(((forecast - np.mean(forecast)) ** 2).sum() / (forecast.size)))
            if std < 5:
                std = 5
            else:
                pass
        data_SS  = data_SS.append({'Sku_code': i,'SS': std,}, ignore_index=True)
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    # print('data_SS',len(data_SS))
    # print('final',len(final))
    data_merge      = pd.merge(final,data_SS,on='Sku_code',how='left')
    data_merge['Account_date'] = pd.to_datetime(data_merge['Account_date'])
    data_merge.fillna(method='ffill')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    mid_df = distribution_stock(data_stock,data_merge,rule_data,T,T_1)
    # mid_df.to_csv('./mid_df.csv', encoding='utf_8_sig')
    #------------------------------------------------------------------------------------->对SS第一次revised
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    def compare(x):
        #--------------------------------------------------------------------------------->SS最大不超过一天的量
        if x['SS'] > x['Forecast_box']:
            qty = x['Forecast_box']
            return qty
        else:
            return x['SS']
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    mid_df['SS'] = mid_df.apply(lambda x: compare(x), axis=1)
    def compare_min(x):
        #--------------------------------------------------------------------------------->SS最小不小于10件
        if x['SS'] <= 10:
            qty = 10
            return qty
        else:
            return x['SS']

    #----------------------------------------------------------------------------->只针对浙北仓才设置最小安全库存
    if wh_code == '001':
        mid_df['SS'] = mid_df.apply(lambda x: compare_min(x), axis=1)
    else:
        pass

    #--------------------------------------------------------------------------------->安全库存再修正做数据准备
    revised_pre     = final[final['Account_date'] == T]
    revised_pre     = revised_pre[['Sku_code', 'Forecast_box']]
    revised_pre     = revised_pre.rename(columns={'Forecast_box': 'Forecast_box_tomorrow'})
    data_final      = pd.merge(mid_df,revised_pre,on='Sku_code',how='left')
    return data_final

def main_function(Id,wh_code,data_stock,data_final,today_date):
    data_stock['Sku_code'] = data_stock['Sku_code'].astype(str)
    data_stock = data_stock[['Sku_code','Stock','Inventory']]
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    '''选择外连接合并，是因为存在仓库的表格并没有数据，但是实际情况是需要订货的，因此采取外连接，并进行补零操作'''
    '''最终选择采用内连接的方式，在于产品进行测试的阶段，先对一些指定的SKU进行计算'''
    data_merge = pd.merge(data_final,data_stock,on='Sku_code',how='inner')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    data_merge= data_merge.fillna(0)
    print_in_log('data_merge_length:'+str(len(data_merge)))
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    #在加一个逻辑是基于昨日销量的一个对预测值的修正情况，如果预测的数量小于实际的销售数量的话，将会在预测的时候进行未来那天的修正
    def predict_revised(x):
        #-------------->当前版本有可能存在负值的情况
        stock       = x['Stock']
        Inventory   = x['Inventory']
        if Inventory < 0:
            Inventory = 0
        else:
            pass
        real_sales = stock - Inventory
        if real_sales < x['Forecast_box_tomorrow']:
            return x['Forecast_box']
        elif 2 * real_sales < x['Forecast_box_tomorrow']:
            return real_sales * 2
        else:
            return x['Forecast_box'] + (real_sales - x['Forecast_box_tomorrow'] )
    data_merge['Forecast_box'] = data_merge.apply \
        (lambda x: predict_revised(x), axis=1)


    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互


    #---------------------------------------------------------------------------->判断实际需要的的数量
    def calculate_final(x):
        Inventory   = x['Inventory']
        if Inventory < 0:
            Inventory = 0
        else:
            pass


        foreacst_demand = x['Forecast_box'] + x['SS']
        if foreacst_demand <= Inventory:
            return 0
        else:
            return round(foreacst_demand - Inventory)
    data_merge['Suggestion_qty'] = data_merge.apply \
        (lambda x: calculate_final(x), axis=1)


    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互

    # data_merge.to_csv('./00001.csv',encoding='utf_8_sig')
    #----------------------------------------------------------------------->如果建议的数量不能够满足前日的销售数据将会进行修正
    data_merge['Sales_box'] = data_merge['Stock'] - data_merge['Inventory']
    def suggestion_revised(x):
        Inventory   = x['Inventory']
        if Inventory < 0:
            Inventory = 0
        else:
            pass


        if x['Suggestion_qty'] + Inventory > x['Sales_box']:
            qty = x['Suggestion_qty']
        else:
            qty = x['Sales_box'] - Inventory
        return qty
    data_merge['Suggestion_qty'] = data_merge.apply \
        (lambda x: suggestion_revised(x), axis=1)
    # data_merge.to_csv('./00002.csv', encoding='utf_8_sig')
    #------------------------------------------------------------------------>采用新的逻辑，将最终的订货建议显示成整数模式
    def round_function(x):
        length = int(-len(str(int(x['Suggestion_qty']))) + 2)
        result = round(x['Suggestion_qty'], length)
        return result

    #----------------------------------------------------------------------->单独的取整操作也是为浙北仓单独设计
    if wh_code == '001' or wh_code == '006' or wh_code == '007'\
            or wh_code == '008'or wh_code == '009'or wh_code == '017'\
            or wh_code == '030':
        data_merge['Suggestion_qty'] = data_merge.apply \
        (lambda x: round_function(x), axis=1)
    else:
        pass
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    #------------------------------------------------------------------------------------>最小起订量的判断

    def revised_min_order(x):
        if x['min_order'] >= 5:
            qty = x['min_order']
        else:
            qty = 5
        return qty


    #---------------------------------------------------------------------------------->只有杭州仓对最小起订量进行修正
    if wh_code == '001':
        data_merge['min_order'] = data_merge.apply(lambda x: revised_min_order(x), axis=1)
    else:
        pass


    def min_compare(x):
        if x['Suggestion_qty'] >= x['min_order']:
            qty = x['Suggestion_qty']
        elif  x['min_order'] > x['Suggestion_qty']>= x['min_order'] * 0.75:
            qty = x['min_order']
        else:
            qty = 0
        return qty
    data_merge['Suggestion_qty'] = data_merge.apply(lambda x: min_compare(x), axis=1)

    data_merge['Update_time']       = today_date
    data_merge['Date_timestamp']    = pd.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data_merge['Account_date']      = [x.strftime('%Y-%m-%d') for x in data_merge['Account_date']]

    # data_merge['rate']              = data_merge['rate'].apply(lambda x: '1*' + "%.f"%x )
    data_merge = data_merge.drop(['Forecast_box_tomorrow','Stock','Forecast_qty','Price','min_order'], axis=1)
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    return data_merge


def connectdb():
    print_in_log('连接到mysql服务器...')
    db = pymysql.connect(host="rm-bp1jfj82u002onh2t.mysql.rds.aliyuncs.com",
                         database="purchare_sys", user="purchare_sys",
                         password="purchare_sys@123",port=3306, charset='utf8')
    print_in_log('连接成功')
    return db


#《---------------------------------------------------------------------------------------------------------------------删除重复日期数据
def drop_data(Id,db,wh_code,today_date):
    cursor = db.cursor()
    sql = """delete from dc_replenishment where Dc_code = '%s' and Update_time = DATE('%s')"""%(wh_code,today_date)
    print_in_log('已经删除重复数据')
    print_in_log(str(sql))
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    client.hincrby(Id, '20', 1)
    print_in_log('redis+1')
    #++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
    cursor.execute(sql)

#<======================================================================================================================
def insertdb(Id,db,data):
    cursor = db.cursor()
    # param = list(map(tuple, np.array(data).tolist()))
    data_list = data.values.tolist()
    print(data.columns)

    print_in_log('data_list'+str(data_list))
    sql = """INSERT INTO dc_replenishment (Sku_code, Dc_name, Dc_code, Wrh, Warehouse_name, Sku_name,
       Account_date, Sku_id, Update_time, rate, Munit,
       Forecast_box, SS, Inventory, Suggestion_qty, Sales_box,
       Date_timestamp)
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.executemany(sql, data_list)
        print_in_log("所有品牌的sku数据插入数据库成功")
        # ++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
        client.hincrby(Id, '20', 1)
        print_in_log('redis+1')
        # ++++++++++++++++++++++++++++++++++++++++++++++++>与redis进行交互
        db.commit()
    except OSError as reason:
        print_in_log('出错原因是%s' % str(reason))
        db.rollback()

#<================================================================关闭连接函数
def closedb(db):
    db.close()

#定义主计算调度逻辑进行函数的汇总
def main(redis_key,wh_code,today_date_str):
    client.hset(redis_key,'20',0)
    #------------------------------------------------------------------------------->创建文件夹
    mkdir('log')
    #----------------------------------------------------->获取基本的参数
    time_start = datetime.datetime.now()
    today_date,T_1 = get_all_date(redis_key,today_date_str)
    print_in_log(redis_key+wh_code)
    #-------------------------------------------------->读取特商品的逻辑规则
    rule_data                       = special_rule_data(wh_code)
    stock_data,original_forecast    = get_db(redis_key,wh_code,today_date)
    # stock_data.to_csv('./stock_data.csv', encoding='utf_8_sig')
    # original_forecast.to_csv('./original_forecast.csv', encoding='utf_8_sig')
    data_stock, final               = cleaning_data(redis_key,wh_code,original_forecast, stock_data)
    # final.to_csv('./final.csv', encoding='utf_8_sig')
    #-------------------------------------------------->安全库存与预测修正
    data_final                      = algorithm_SS(redis_key,wh_code,final,data_stock,today_date, T_1,rule_data)
    # data_final.to_csv('./data_final.csv',encoding='utf_8_sig')
    AI_suggestion                   = main_function(redis_key,wh_code,data_stock, data_final,today_date)
    AI_suggestion                   = AI_suggestion.drop_duplicates(subset=['Sku_code'], keep='first')
    # AI_suggestion.to_csv('./AI_suggestion.csv', encoding='utf_8_sig')
    db = connectdb()
    drop_data(redis_key,db,wh_code,today_date)
    time_end = datetime.datetime.now()
    if AI_suggestion.empty:
        print_in_log("The data frame is empty")
        print_in_log("总耗时："+str(time_end-time_start))
        closedb(db)
    else:
        insertdb(redis_key,db,AI_suggestion)
        closedb(db)
        print_in_log("总耗时："+str(time_end-time_start))

def get_parameter():
    redis_key = sys.argv[1]
    wh_code = sys.argv[2]
    today_date_str = sys.argv[3]
    return redis_key,wh_code,today_date_str

#《============================================================================主函数入口
if __name__ == '__main__':
    # # # transit_days('001')
    try:
        redis_key,wh_code,today_date_str = get_parameter()
        # redis_key = 'test'
        # wh_code = '001'
        # today_date_str = '2020-06-14'
        main(redis_key,wh_code,today_date_str)
    except OSError as reason:
        print_in_log('出错原因是%s' % str(reason))
        print_in_log ("result:0")

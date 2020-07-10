# -*- coding: utf-8 -*-
# @Time    : 2019/10/15 8:11
# @Author  : Ye Jinyu__jimmy
# @File    : schedule_program


#-*- encoding:utf-8 -*-
from apscheduler.schedulers.blocking import BlockingScheduler
from store_monitor_peisong import *;
from HDERP_Interface_monitor import *;
from store_monitor_importalert import send_import_alert;

from WebServiceMonitor import WebServiceMonitor;

from LogServiceMonitor import LogServiceMonitor;


from check_oracle_alive import checkOracleAlive

from guanyuan_data_reflash import guan_card_reflash_dd

### 定时调度任务：监控门店

sched = BlockingScheduler()
#sched.add_job(monitor_peisong, 'interval', seconds=5)
### 门店配送-加货情况监控
# sched.add_job(monitor_peisong, 'cron',hour = '9')

## 总部监控

## 监控 非码订单非当日接入的情况
sched.add_job(job_hderp_stkout_feima,'cron',hour='9')


### 总部 重要提醒
#sched.add_job(send_import_alert,'cron',hour='8,10,12,16,21')

### 总部 帆软报表用户密码更新 删除离职用户
#sched.add_job(update_finereport_userpwd,'cron',hour='*/1')

## 总部 Web服务监控
sched.add_job(WebServiceMonitor().checkService,'cron',hour='5-23', minute ='*/12')

## 总部 日志服务监控 LogServiceMonitor
sched.add_job(LogServiceMonitor().checkLog,'cron',hour='5-23', minute ='*/12')


## 总部 服务器磁盘、DG 服务监控 LogServiceMonitor
#sched.add_job(checkDiskAndDatabaseBak,'cron',hour='9,11,12,14,16,18,21')



## 总部 中控人事同步接口异常监控
#sched.add_job(getZkRsJKInfo,'cron',hour='8,9,10,11,12,13,14,15,16,17,18')



## 总部 在线率监控 合集：中控、JPOS、八乐在线
#sched.add_job(dailyonline_Monitor,'cron',hour='8,10,14,16,18')

## 总部数据库可用性监控
sched.add_job(checkOracleAlive, 'cron', hour='7-22', minute ='*/10')

## 观远卡片更新  督导考勤
sched.add_job(guan_card_reflash_dd, 'cron', hour='7-13', minute='4,34')


sched.start()
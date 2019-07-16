# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 10:52:27 2019

@author: viola
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json
from urllib import parse,request
import datetime
import logging
import matplotlib.pyplot as plt
import re
import math

logger = logging.getLogger('setiment_index')  
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('setiment_index.log')  
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  
fh.setFormatter(formatter)  
ch.setFormatter(formatter)  
logger.addHandler(fh)  
logger.addHandler(ch)  

def getdata(engine ,sql):
    starttime = datetime.datetime.now()
    conn = engine.connect()
    try:
        getrst = pd.read_sql(sql,conn)
    except Exception as e:
        print('getdata error: {} \n engine and sql as below: \n {} \n {}'.format(repr(e),engine,sql))
    finally:
        conn.close()
    endtime = datetime.datetime.now()
    logger.info('......it took {} seconds to get rawdata.'.format((endtime - starttime).seconds))
    return getrst

def get_raw(data):
    engine_server_news = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/news?charset=utf8')
    raw_data = getdata(engine_server_news,data)
    return raw_data

#获取雪球评论数量
yesterday = datetime.date.today()+datetime.timedelta(days=-1)
sql_get_xueqiu= "select*from stock_day_count_xueqiu WHERE p_date>DATE_SUB('{}',interval 365 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
df_xueqiu_1year=get_raw(sql_get_xueqiu)
df_xueqiu_1year=df_xueqiu_1year.rename(columns={"p_date":"date"})
df_xueqiu_1year.drop("update_time",axis=1,inplace=True)

#获取微博评论数量
sql_get_weibo= "select*from stock_day_count_weibo WHERE p_date>DATE_SUB('{}',interval 365 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
df_weibo_1year=get_raw(sql_get_weibo)
df_weibo_1year=df_weibo_1year.rename(columns={"p_date":"date"})
df_weibo_1year.drop("update_time",axis=1,inplace=True)

#获取新闻数量
sql_get_news= "select*from stock_day_count_news  WHERE p_date>DATE_SUB('{}',interval 365 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
#WHERE stock_id LIKE '%.SZ' or stock_id like '%.SH';
df_news_1year=get_raw(sql_get_news)
df_news_1year=df_news_1year.rename(columns={"p_date":"date"})
df_news_1year.drop(["update_time"],axis=1,inplace=True)


'''
用最近一年的数据算a
'''
def get_para_a(r):
        
    r_std=r.std()
    r_mean=r.mean()
    
    x_max=min(max(r),r_mean+3*r_std)
    
    a=math.pow((1/0.99-1),-1/x_max)
    
    return a

#xueqiu
xueqiu_1year=df_xueqiu_1year["count"].values
xueqiu_para_a=get_para_a(xueqiu_1year)


#weibo
weibo_1year=df_weibo_1year["count"].values
weibo_para_a=get_para_a(weibo_1year)

#news
news_1year=df_news_1year["count"].values
news_para_a=get_para_a(news_1year)

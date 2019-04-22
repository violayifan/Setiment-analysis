# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 16:19:47 2019

@author: viola
"""

'''
获取数据
'''
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
import requests


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

'''
舆情指数参数para_a(一年调整)
'''
xueqiu_para_a=1.63
weibo_para_a=1.43
news_para_a=1.05

'''
取最近30天的数据算舆情指数
'''
yesterday = datetime.date.today()+datetime.timedelta(days=-1)
#获取雪球评论数量
sql_get_xueqiu= "select*from stock_day_count_xueqiu WHERE p_date>=DATE_SUB('{}',interval 30 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
df_xueqiu_1month=get_raw(sql_get_xueqiu)
df_xueqiu_1month.drop("update_time",axis=1,inplace=True)

#获取微博评论数量
sql_get_weibo= "select*from stock_day_count_weibo WHERE p_date>=DATE_SUB('{}',interval 30 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
df_weibo_1month=get_raw(sql_get_weibo)
df_weibo_1month.drop("update_time",axis=1,inplace=True)

#获取新闻数量
sql_get_news= "select*from stock_day_count_news  WHERE p_date>=DATE_SUB('{}',interval 30 day) and p_date<='{}' ORDER BY p_date ASC;".format(yesterday, yesterday)
df_news_1month=get_raw(sql_get_news)
df_news_1month.drop(["update_time"],axis=1,inplace=True)

#股票名称对应股票代码
#获取股票名称list
 def get_stock_name():
    '''获取股票代码列表'''
    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
                   "Content-Type": "application/json"}
    url_getname = 'https://stkcode.tigerobo.com/HB_IDCenterWeb/JS.ashx?type=bk&cmd=901&rtntype=1&' \
                  'tkn=3e715abf133fa24da68e663c5ab98857'
    req = request.Request(url=url_getname, headers=header_dict)
    res = request.urlopen(req)
    res = res.read()
    r = json.loads(res.decode(encoding='utf-8'))
    res_dict = dict()
    for data in r:
        if data["ShowMkt"] == "SZ" or data["ShowMkt"] == "SH":
            stockName = data["Name"]
            stockName = re.sub("\s*", '', stockName)
            res_dict[stockName] = data["Code"]
    return res_dict         
stock_name=get_stock_name()
df_stock_name=pd.DataFrame([stock_name]).T.reset_index()
df_stock_name.columns=["stock_name","stock_id"]

#df_weibo 加stock_name
df_weibo=pd.merge(df_weibo_1month,df_stock_name)
#df_xueqiu 加stock_name
df_xueqiu=pd.merge(df_xueqiu_1month,df_stock_name)
#df_news 改stock_id
df_news_1month["stock_id"]=[i[:6] for i in df_news_1month["stock_id"]]
df_news=df_news_1month.copy()

'''
舆情指数 version 2
标准化
'''
def yuqin_index(t,a):
    
    y=(2/(1+math.pow(a,-t))-1)*100
    
    return y

'''
R=100%/3*yuqin(weibi)+100%/3*yuqin(xueqiu)+100%/3*yuqin(news)
'''
df_weibo["weibo_score"]=df_weibo["count"].transform(lambda x:yuqin_index(x,weibo_para_a))
df_xueqiu["xueqiu_score"]=df_xueqiu["count"].transform(lambda x:yuqin_index(x,xueqiu_para_a))
df_news["news_score"]=df_news["count"].transform(lambda x:yuqin_index(x,news_para_a))
df_combine=pd.DataFrame()
df_combine=pd.merge(df_weibo,df_xueqiu,on=["stock_id","p_date"],how="outer")
df_combine=pd.merge(df_combine,df_news,on=["stock_id","p_date"],how="outer")
df_combine["xueqiu_score"].fillna(yuqin_index(0,xueqiu_para_a),inplace=True)
df_combine["weibo_score"].fillna(yuqin_index(0,weibo_para_a),inplace=True)
df_combine["news_score"].fillna(yuqin_index(0,news_para_a),inplace=True)
df_rank_daily=df_combine[["stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
df_rank_daily=pd.merge(df_rank_daily,df_stock_name)

df_rank_daily["setiment_index"]=0.2*df_rank_daily["weibo_score"]+0.6*df_rank_daily["xueqiu_score"]+0.2*df_rank_daily["news_score"]


#生成json，上传redis 近30天的舆情指数折线图
import json_serialize as js
import redis
l_stock_daily=list(df_rank_daily["stock_id"].unique())
count=0
for each in l_stock_daily:
    df=df_rank_daily.copy()[df_rank_daily["stock_id"]==each][["p_date","setiment_index","stock_name"]].reset_index(drop=True)
    df=df.sort_values(by="p_date")
    title_1 = df["stock_name"][0]+'舆情指数'
    df_1=df[["p_date","setiment_index"]]
    df_1["p_date"]=df_1["p_date"].transform(lambda x:str(x))
    df_1=df_1.set_index("p_date")
    json_1=js.df2json1line(df_1)(title=title_1)
    json_1_dump = json.dumps(json_1)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_name="setiment_index_daily_"+each
    redis_c.set(json_name,json_1_dump)
    count=count+1
    print(count)
'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_daily_"stock_code"
'''


"""
一周热度均值，峰值，一天变化趋势表格
"""
#取一周数据
week=datetime.date.today()+datetime.timedelta(days=-7)

df_rank_weekly=df_rank_daily[df_rank_daily["p_date"]>=week]

df_weekly_rst=pd.DataFrame()

count=0

for stock in df_rank_weekly["stock_id"].unique().tolist():
    df_temp=df_rank_weekly[df_rank_weekly["stock_id"]==stock].reset_index(drop=True)
    dic_temp={}
    dic_temp["max_index"]=max(df_temp["setiment_index"])
    dic_temp["mean_index"]=df_temp["setiment_index"].mean()
    length_index=len(df_temp["setiment_index"])
    if length_index>=2:
        dic_temp["change_index"]=df_temp["setiment_index"].tolist()[-1]-df_temp["setiment_index"].tolist()[-2]
    else:
        if df_temp["p_date"][0]==yesterday:
            dic_temp["change_index"]=0-df_temp["setiment_index"].tolist()[-1]
        else:
            dic_temp["change_index"]=df_temp["setiment_index"].tolist()[-1]-0
    dic_temp["stock_id"]=df_temp["stock_id"][0]
    df_temp_1=pd.DataFrame([dic_temp])
    df_weekly_rst=df_weekly_rst.append(df_temp_1)
    count=count+1
    print(count)
df_weekly_rst=pd.merge(df_weekly_rst,df_stock_name)
df_weekly_rst_1=df_weekly_rst[["stock_name","stock_id","mean_index","max_index","change_index"]]
df_weekly_rst_1.columns=["股票名称","股票代码","一周热度均值","一周热度峰值","热度变化趋势"]


#生成json，上传redis 个股一周热度均值，峰值，一周内变化趋势
l_stock_weekly=list(df_weekly_rst_1["股票代码"].unique())
count=0
for each in l_stock_weekly:
    df=df_weekly_rst_1[df_weekly_rst_1["股票代码"]==each].reset_index(drop=True)
    title_2 = df["股票名称"][0]+'热度概括'    
    df_1=df[["一周热度均值","一周热度峰值","热度变化趋势"]]
    json_2=js.table(df_1)(title=title_2)
    json_2_dump = json.dumps(json_2)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_2_name="setiment_index_summary_"+each
    redis_c.set(json_2_name,json_2_dump)
    count=count+1
    print(count)

'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_summary_"stock_code"

'''


'''
30天社交评论热度，专业评论热度，新闻热度
'''
df_rank_specific=df_rank_daily[["stock_name","stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
df_rank_specific.columns=["股票名称","股票代码","日期","社交关注热度","专业关注热度","新闻关注热度"]

#生成json，上传redis 当日（昨日）社交评论热度，专业评论热度，新闻热度

l_stock_specific=list(df_rank_specific["股票代码"].unique())
count=0
for each in l_stock_specific:
    df=df_rank_specific[df_rank_specific["股票代码"]==each].reset_index(drop=True)
    title_3 = df["股票名称"][0]+"热度详情"    
    df_1=df[["日期","社交关注热度","专业关注热度","新闻关注热度"]]
    df_1["日期"]=df_1["日期"].transform(lambda x:str(x))
    df_1=df_1.set_index("日期")
    json_3=js.multiline(df_1)(title=title_3)
    json_3_dump = json.dumps(json_3)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_3_name="setiment_index_specific_"+each
    redis_c.set(json_3_name,json_3_dump)
    count=count+1
    print(count)

'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_specific_"stock_code"

'''

"""
个股每日热榜
"""
df_daily_index=df_rank_weekly[df_rank_weekly["p_date"]==yesterday].sort_values(by="setiment_index",ascending=False).reset_index(drop=True)[["stock_name","setiment_index"]]

df_daily_index.columns=["股票名称","舆情指数"]
title_4="个股每日热榜"
json_4=js.table(df_daily_index)(title=title_4)
json_4_dump = json.dumps(json_4)
json_4_name = "daily"
redis_c.set(json_4_name, json_4_dump)


'''
http://47.100.219.4:7080/?topic=5&aris_data=daily

'''


"""
行业概念板块舆情指数
"""
db_share = create_engine(
    "mysql+pymysql://db_admin:User@123@rm-uf6xzs2aqh4nx78m2.mysql.rds.aliyuncs.com:3306/share?charset=utf8")


def get_stock_bk():
    # 获取选股宝概念板块
    sql = "SELECT SecuCode as code, BankuaiInfo as bk FROM bankuai"

    # code_d={}

    # bk_d={}
    df_bk = pd.read_sql(sql, db_share)

    data = []

    for idx, row in df_bk.iterrows():
        code = row['code'].split('.')[0]
        bk_l = row['bk']
        bk_l = json.loads(bk_l)
        for bk_l_i in bk_l:
            bk_key = bk_l_i['name']
            bk_id=bk_l_i['id']
            bk_key = bk_key.replace('概念', '')

            data.append([code, bk_key,bk_id])
   
    df_f = pd.DataFrame(data, columns=['code', 'bk', 'id'])

    return df_f

df_bk=get_stock_bk()


df_rank_yesterday=df_rank_weekly[df_rank_weekly["p_date"]==yesterday][["stock_id","setiment_index"]]

df_rank_bk=pd.merge(df_rank_yesterday,df_bk,left_on="stock_id",right_on="code")

df_bk_index=df_rank_bk.groupby("bk")["setiment_index"].mean().reset_index().sort_values(by="setiment_index",ascending=False).reset_index(drop=True)

df_bk_index=pd.merge(df_bk_index,df_bk[["bk","id"]]).drop_duplicates(subset="bk")

df_bk_index.columns=["概念板块名称","舆情指数","概念板块代码"]


title_6="概念板块每日热榜"
json_6=js.table(df_bk_index)(title=title_6)
json_6["headName"]=["概念板块名称","舆情指数"]
json_6["headCol"]=["gainianbankuaimingcheng0",'yuqingzhishu1']
json_6_dump = json.dumps(json_6)
json_6_name = "bk_daily"
redis_c.set(json_6_name, json_6_dump)

'''
http://47.100.219.4:7080/?topic=5&aris_data=bk_daily

'''

#概念板块对应的子页面
df_stock_bk=pd.merge(df_rank_bk,df_stock_name)
l_bk=df_stock_bk["bk"].unique().tolist()
count=0

for bk in l_bk:
    df_bk=df_stock_bk[df_stock_bk["bk"]==bk][["stock_name","stock_id","setiment_index","id"]].reset_index(drop=True)
    df_bk.columns=["股票名称","股票代码","舆情指数","概念板块代码"]
    title_7="概念板块个股舆情指数"
    df_bk_1=df_bk[["股票名称","股票代码","舆情指数"]]
    json_7=js.table(df_bk_1)(title=title_7)
    json_7_dump = json.dumps(json_7)
    json_7_name = "bk_"+str(df_bk["概念板块代码"][0])
    redis_c.set(json_7_name, json_7_dump)
    count=count+1
    print(count)

'''
http://47.100.219.4:7080/?topic=5&aris_data=bk_"bk_index"

'''








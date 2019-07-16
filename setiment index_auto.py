# -*- coding: utf-8 -*-
"""
Created on Sun Apr 28 17:05:35 2019

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
import requests
import json_serialize as js
import redis

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

def get_stock_name():
    '''获取股票代码-名字列表'''
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
            
    df_stock_name=pd.DataFrame([res_dict]).T.reset_index()
    df_stock_name.columns=["stock_name","stock_id"]

    return df_stock_name        


def get_month_data(yesterday):
    '''
    提取数据并计算舆情指数
    '''
    
    '''data perparation'''
    #取30天数据
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
    
    #adjusted data
    df_stock_name=get_stock_name()
    #df_weibo 加stock_name
    df_weibo=pd.merge(df_weibo_1month,df_stock_name)
    #df_xueqiu 加stock_name
    df_xueqiu=pd.merge(df_xueqiu_1month,df_stock_name)
    #df_news 改stock_id
    df_news_1month["stock_id"]=[i[:6] for i in df_news_1month["stock_id"]]
    df_news=df_news_1month.copy()
    
    '''
    setiment index calculation
    '''      
    def yuqin_index(t,a):
        
        y=(2/(1+math.pow(a,-t))-1)*100
        
        return y
    
    #舆情指数参数para_a(一年调整)    
    xueqiu_para_a=1.63
    weibo_para_a=1.43
    news_para_a=1.05
    
    #R=0.2*yuqin(weibi)+0.6*yuqin(xueqiu)+0.2*yuqin(news)
    
    df_weibo["weibo_score"]=df_weibo["count"].transform(lambda x:yuqin_index(x,weibo_para_a))
    df_news["news_score"]=df_news["count"].transform(lambda x:yuqin_index(x,news_para_a))
    df_xueqiu["xueqiu_score"]=df_xueqiu["count"].transform(lambda x:yuqin_index(x,xueqiu_para_a))
    
    df_combine=pd.DataFrame()
    df_combine=pd.merge(df_weibo,df_xueqiu,on=["stock_id","p_date"],how="outer")
    df_combine=pd.merge(df_combine,df_news,on=["stock_id","p_date"],how="outer")
    df_combine["xueqiu_score"].fillna(yuqin_index(0,xueqiu_para_a),inplace=True)
    df_combine["weibo_score"].fillna(yuqin_index(0,weibo_para_a),inplace=True)
    df_combine["news_score"].fillna(yuqin_index(0,news_para_a),inplace=True)
    df_rank_daily=df_combine[["stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
    df_rank_daily=pd.merge(df_rank_daily,df_stock_name)
    df_rank_daily["setiment_index"]=0.2*df_rank_daily["weibo_score"]+0.6*df_rank_daily["xueqiu_score"]+0.2*df_rank_daily["news_score"]

    return df_rank_daily
    

def month_index_line(df_rank_daily,stock):
    """
    个股舆情子页面 近30天的舆情指数折线图
    """
    df=df_rank_daily.copy()[df_rank_daily["stock_id"]==stock][["p_date","setiment_index","stock_name"]].reset_index(drop=True)
    df=df.sort_values(by="p_date")
    title_1 = df["stock_name"][0]+'舆情指数'
    df_1=df[["p_date","setiment_index"]]
    df_1["p_date"]=df_1["p_date"].transform(lambda x:str(x))
    df_1=df_1.set_index("p_date")
    df_1.columns=["舆情指数"]
    json_1=js.df2json1line(df_1)(title=title_1)
    json_1_dump = json.dumps(json_1)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_name="setiment_index_daily_"+stock
    redis_c.set(json_name,json_1_dump)
'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_daily_"stock_code"
'''
def week_summary(df_rank_daily,week1,week2,yesterday):
    """
    个股舆情子页面 一周热度均值，峰值，一周变化趋势(这周与上周变化的差值)的df
    """

    df_weekly_rst=pd.DataFrame()
    df_rank_weekly_1=df_rank_daily[(df_rank_daily["p_date"]>=week1)& (df_rank_daily["p_date"]<=yesterday)]
    df_rank_weekly_2=df_rank_daily[df_rank_daily["p_date"]>=week2]

    count=0

    for stock in stock_list:
        dic_temp={}
        #近两周都有数据的股票
        try:
            df_temp_1=df_rank_weekly_1[df_rank_weekly_1["stock_id"]==stock]
            df_temp_2=df_rank_weekly_2[df_rank_weekly_2["stock_id"]==stock]
            dic_temp["max_index"]=max(df_temp_2["setiment_index"])
            dic_temp["mean_index"]=df_temp_2["setiment_index"].mean()
            dic_temp["change_index"]=df_temp_2["setiment_index"].mean()-df_temp_1["setiment_index"].mean()
            dic_temp["stock_id"]=stock
            df_temp_1=pd.DataFrame([dic_temp])
        #处理这周或者上周没有数据,或两周都没数据的股票
        except:
            l_stock_1=df_rank_weekly_1["stock_id"].unique().tolist()
            l_stock_2=df_rank_weekly_2["stock_id"].unique().tolist()
            if (stock not in l_stock_1)&(stock not in l_stock_2):
                dic_temp["max_index"]=0
                dic_temp["mean_index"]=0
                dic_temp["change_index"]=0
                dic_temp["stock_id"]=stock
                df_temp_1=pd.DataFrame([dic_temp])
            if (stock not in l_stock_1)&(stock in l_stock_2):
                df_temp_2=df_rank_weekly_2[df_rank_weekly_2["stock_id"]==stock]
                dic_temp["max_index"]=max(df_temp_2["setiment_index"])
                dic_temp["mean_index"]=df_temp_2["setiment_index"].mean()
                dic_temp["change_index"]=df_temp_2["setiment_index"].mean()-0
                dic_temp["stock_id"]=stock
                df_temp_1=pd.DataFrame([dic_temp])        
            if (stock not in l_stock_2)&(stock in l_stock_1):
                df_temp_1=df_rank_weekly_1[df_rank_weekly_1["stock_id"]==stock]
                dic_temp["max_index"]=0
                dic_temp["mean_index"]=0
                dic_temp["change_index"]=0-df_temp_1["setiment_index"].mean()
                dic_temp["stock_id"]=stock
                df_temp_1=pd.DataFrame([dic_temp])
       
        df_weekly_rst=df_weekly_rst.append(df_temp_1)
        count=count+1
        print(count)
    
    df_weekly_rst=pd.merge(df_weekly_rst,df_stock_name)
    df_weekly_rst=df_weekly_rst[["stock_name","stock_id","mean_index","max_index","change_index"]]
    df_weekly_rst.columns=["股票名称","股票代码","一周热度均值","一周热度峰值","一周热度变化趋势"]

    
    return df_weekly_rst


def week_summary_json(stock,df_weekly_rst): 
    
    """
    生成个股 一周舆情总结的json文件
    """       
    df=df_weekly_rst[df_weekly_rst["股票代码"]==stock]
    df_1=df[["股票代码","一周热度均值"]].set_index("股票代码").T
    dic_1=df_1.to_dict()
    json_2_dump_1 = json.dumps(dic_1)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_2_name_1="setiment_index_mean_"+stock
    redis_c.set(json_2_name_1,json_2_dump_1)
    
    df=df_weekly_rst[df_weekly_rst["股票代码"]==stock]
    df_2=df[["股票代码","一周热度峰值"]].set_index("股票代码").T
    dic_2=df_2.to_dict()
    json_2_dump_2 = json.dumps(dic_2)
    json_2_name_2="setiment_index_max_"+stock
    redis_c.set(json_2_name_2,json_2_dump_2)

    df=df_weekly_rst[df_weekly_rst["股票代码"]==stock]
    df_3=df[["股票代码","一周热度变化趋势"]].set_index("股票代码").T
    dic_3=df_3.to_dict()
    json_2_dump_3 = json.dumps(dic_3)
    json_2_name_3="setiment_index_change_"+stock
    redis_c.set(json_2_name_3,json_2_dump_3)
    

'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_mean_"stock_code"
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_max_"stock_code"
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_change_"stock_code"
'''

def get_specific_line(df_rank_daily,stock):
    
    """
    30天社交评论热度，专业评论热度，新闻热度 折线图
    """

    df_rank_specific=df_rank_daily[["stock_name","stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
    df_rank_specific.columns=["股票名称","股票代码","日期","社交关注热度","专业关注热度","新闻关注热度"]

    df=df_rank_specific[df_rank_specific["股票代码"]==stock].reset_index(drop=True)
    title_3 = df["股票名称"][0]+"热度详情"    
    df_1=df[["日期","社交关注热度","专业关注热度","新闻关注热度"]]
    df_1["日期"]=df_1["日期"].transform(lambda x:str(x))
    df_1=df_1.set_index("日期")
    json_3=js.multiline(df_1)(title=title_3)
    json_3_dump = json.dumps(json_3)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_3_name="setiment_index_specific_"+stock
    redis_c.set(json_3_name,json_3_dump)
    
'''
http://47.100.219.4:7080/?topic=5&aris_data=setiment_index_specific_"stock_code"
'''
   
def get_day3_summary(df_rank_daily):
    '''
    三天 社交评论热度，专业评论热度，新闻热度均值的df

    '''
    
    day3=datetime.date.today()+datetime.timedelta(days=-3)
    df_rank_specific_1=df_rank_daily[df_rank_daily["p_date"]>=day3][["stock_name","stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
    df_specific_1=pd.DataFrame()
    count=0
    for stock in stock_list:
        dic_specific={}
        if stock in df_rank_specific_1["stock_id"].unique().tolist():
            df_temp=df_rank_specific_1[df_rank_specific_1["stock_id"]==stock]
            dic_specific["stock"]=stock
            dic_specific["weibo"]=df_temp["weibo_score"].mean()
            dic_specific["xueqiu"]=df_temp["xueqiu_score"].mean()
            dic_specific["news"]=df_temp["news_score"].mean()
            df_temp_1=pd.DataFrame([dic_specific])
        else:
            dic_specific["stock"]=stock
            dic_specific["weibo"]=0
            dic_specific["xueqiu"]=0
            dic_specific["news"]=0
            df_temp_1=pd.DataFrame([dic_specific])
        
        df_specific_1=df_specific_1.append(df_temp_1)
        count=count+1
        print(count)
    
    df_specific_1=df_specific_1[["stock","weibo","xueqiu","news"]]
    df_specific_1.columns=["股票代码","社交关注热度","专业关注热度","新闻关注热度"]
    
    return df_specific_1


def get_day3_summary_json(df_specific_1,stock):
   
  '''
  三天 社交评论热度，专业评论热度，新闻热度均值的json文件
  
  '''
  
  df=df_specific_1[df_specific_1["股票代码"]==stock]
  df_1=df[["社交关注热度","专业关注热度","新闻关注热度"]]
  title_4="三天关注热度"
  json_4=js.table(df_1)(title=title_4)
  json_4_dump = json.dumps(json_4)
  redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
  json_4_name="specific_day3_"+stock
  redis_c.set(json_4_name,json_4_dump)
    

'''
http://47.100.219.4:7080/?topic=5&aris_data=specific_day3_"stock_code"
'''

def get_week_summary(df_rank_daily,week2):
    
    '''
    一周 社交评论热度，专业评论热度，新闻热度均值df
    '''
    
    df_rank_specific_2=df_rank_daily[df_rank_daily["p_date"]>=week2][["stock_name","stock_id","p_date","weibo_score","xueqiu_score","news_score"]]

    df_specific_2=pd.DataFrame()
    count=0
    for stock in stock_list:
        dic_specific={}
        if stock in df_rank_specific_2["stock_id"].unique().tolist():
            df_temp=df_rank_specific_2[df_rank_specific_2["stock_id"]==stock]
            dic_specific["stock"]=stock
            dic_specific["weibo"]=df_temp["weibo_score"].mean()
            dic_specific["xueqiu"]=df_temp["xueqiu_score"].mean()
            dic_specific["news"]=df_temp["news_score"].mean()
            df_temp_1=pd.DataFrame([dic_specific])
        else:
            dic_specific["stock"]=stock
            dic_specific["weibo"]=0
            dic_specific["xueqiu"]=0
            dic_specific["news"]=0
            df_temp_1=pd.DataFrame([dic_specific])
        df_specific_2=df_specific_2.append(df_temp_1)
        count=count+1
        print(count)
        
    df_specific_2=df_specific_2[["stock","weibo","xueqiu","news"]]
    df_specific_2.columns=["股票代码","社交关注热度","专业关注热度","新闻关注热度"]
    
    return df_specific_2

def get_week_summary_json(df_specific_2,stock):
    
    df=df_specific_2[df_specific_2["股票代码"]==stock]
    df_1=df[["社交关注热度","专业关注热度","新闻关注热度"]]
    title_5="一周关注热度"
    json_5=js.table(df_1)(title=title_5)
    json_5_dump = json.dumps(json_5)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_5_name="specific_week_"+stock
    redis_c.set(json_5_name,json_5_dump)
    
'''
http://47.100.219.4:7080/?topic=5&aris_data=specific_week_"stock_code"
'''

def get_yesterday_summary(df_rank_daily,yesterday):
    
    '''
    当日 社交评论热度，专业评论热度，新闻热度的df
    '''

    df_rank_specific_3=df_rank_daily[df_rank_daily["p_date"]==yesterday][["stock_name","stock_id","p_date","weibo_score","xueqiu_score","news_score"]]
    
    df_specific_3=pd.DataFrame()
    count=0
    for stock in stock_list:
        dic_specific={}
        if stock in df_rank_specific_3["stock_id"].unique().tolist():
            df_temp=df_rank_specific_3[df_rank_specific_3["stock_id"]==stock]
            dic_specific["stock"]=stock
            dic_specific["weibo"]=float(df_temp["weibo_score"].values)
            dic_specific["xueqiu"]=float(df_temp["xueqiu_score"].values)
            dic_specific["news"]=float(df_temp["news_score"].values)
            df_temp_1=pd.DataFrame([dic_specific])
        else:
            dic_specific["stock"]=stock
            dic_specific["weibo"]=0
            dic_specific["xueqiu"]=0
            dic_specific["news"]=0
            df_temp_1=pd.DataFrame([dic_specific])
        df_specific_3=df_specific_3.append(df_temp_1)
        count=count+1
        print(count)
        
    df_specific_3=df_specific_3[["stock","weibo","xueqiu","news"]]
    df_specific_3.columns=["股票代码","社交关注热度","专业关注热度","新闻关注热度"]
    
    return df_specific_3

def get_yesterday_summary_json(df_specific_3,stock):
    
    '''
    当日 社交评论热度，专业评论热度，新闻热度的json
    '''      
    df=df_specific_3[df_specific_3["股票代码"]==stock]
    df_1=df[["社交关注热度","专业关注热度","新闻关注热度"]]
    title_6="今日关注热度"
    json_6=js.table(df_1)(title=title_6)
    json_6_dump = json.dumps(json_6)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_6_name="specific_today_"+stock
    redis_c.set(json_6_name,json_6_dump)    

'''
http://47.100.219.4:7080/?topic=5&aris_data=specific_today_"stock_code"
'''
    
def get_wordcloud():
    
    '''
    生成词云图的dic
    '''
    '''
    Part 1 获取雪球数据
    '''


    starttime = datetime.datetime.now()
    df_stock_name=get_stock_name()
    l_stock=df_stock_name["stock_id"].sort_values().tolist()
    
    engine_server_questions_ans = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/questions_ans?charset=utf8')
    sql_ans_xueqiu_comment = "select about,pub_time,comment_num,answer from xueqiu_comment where answer is not Null;"
    df_xueqiu=pd.read_sql(sql_ans_xueqiu_comment,engine_server_questions_ans)
    endtime = datetime.datetime.now()

    logger.info('......it took {} seconds to get rawdata.'.format((endtime - starttime).seconds))

    df_xueqiu_1=pd.DataFrame()
    count=0
    for each in l_stock:
        df_temp=df_xueqiu[df_xueqiu["about"]==each].sort_values(by="pub_time",ascending=False).reset_index(drop=True)
        df_temp_1=df_temp.iloc[:100]
        df_xueqiu_1=df_xueqiu_1.append(df_temp_1)
        count=count+1
        print(count)
    df_xueqiu_1=df_xueqiu_1.reset_index(drop=True)
    
    
    """
    Part 2 评论内容去标签&合并

    """
    logger.info("processing comment") 
    
    #去除标签和非评论个股内容
    def cleanhtml(raw_html):
        
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        cleantext = re.sub(r"\s+", "", cleantext)
        return cleantext
    
    l_answer_clean=[]
    
    for each in df_xueqiu_1["answer"]:
        l_answer_clean.append(cleanhtml(each))
    
    l_answer_clean=[i.replace("&nbsp;","") for i in l_answer_clean]
    l_answer_clean=[i.replace("A股","") for i in l_answer_clean]
    
    l_answer_clean=[re.sub("\$.+?\$", "", i) for i in l_answer_clean]
    l_answer_clean=["" if "选入" in i else i for i in l_answer_clean]
    l_answer_clean=["" if "入选" in i else i for i in l_answer_clean]
    l_answer_clean=["" if "请在 App 内点击查看" in i else i for i in l_answer_clean]
    l_answer_clean=["" if "我在雪球创建了" in i else i for i in l_answer_clean]
    
    
    l_answer_clean=["" if "龙虎榜数据" in i else i for i in l_answer_clean]
    l_answer_clean=["" if "龙虎榜揭秘" in i else i for i in l_answer_clean]
    l_answer_clean=["" if "龙虎榜日报" in i else i for i in l_answer_clean]
    
    cleanweb=re.compile("((http|ftp|https)?:\/\/)[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?")
    l_answer_clean=[re.sub(cleanweb,"",i) for i in l_answer_clean]
    
    l_answer_clean=[re.sub("^[～，ﾉ？￥？?：。；！（）=＝－％＋．＞]+", "", i) for i in l_answer_clean]
    
    #去除空值
    
    l_answer_clean_1=[np.nan if i=="" else i for i in l_answer_clean]
    
    df_xueqiu_1["answer_clean"]=l_answer_clean_1
    
    df_xueqiu_1=df_xueqiu_1.dropna(subset=["answer_clean"])
    
    #合并
    
    dic_answer={}
    l_answer_stock=df_xueqiu_1["about"].unique().tolist()
    count=0
    for stock in l_answer_stock:
        df_select=df_xueqiu_1[df_xueqiu_1["about"]==stock]
        l_answer=[]
        for answer in df_select["answer_clean"]:
            l_answer.append(answer)
            answer=",".join(l_answer)
        dic_answer[stock]=answer
        count=count+1
        print(count)    

    '''
    Part 3 生成云图关键词dic
    '''
    
    import jieba.analyse
    
    jieba.analyse.set_stop_words("C:\\Users\\viola\\Desktop\\舆情专题\\stopword.txt")
    
    dic_word={}
    
    logger.info("processing wordcloud_dic") 

    count=0
    for stock in stock_list:
        #allowPos 选择出现的词性,后期可能需要调整
        if stock in l_answer_stock:
            data=jieba.analyse.extract_tags(dic_answer[stock], topK=50, withWeight=True, allowPOS=('n','vn','vl','an','al','z'))
            l_data=[]
            for i in range(0,len(data)):
                dic_temp_word={}
                temp=data[i]
                dic_temp_word["name"]=temp[0]
                dic_temp_word["value"]=temp[1]
                l_data.append(dic_temp_word)
            dic_word[stock]=l_data
        else:
            dic_word[stock]=[]
        count=count+1
        print("getting wordcloud_dic"+str(count))
        
    return dic_word


def wordcloud_json(dic_word):
    
    dic_data={}
    df_stock_name=get_stock_name()
    name=df_stock_name[df_stock_name["stock_id"]==stock]["stock_name"].values[0]
    dic_data["outer_title"]={"text":name+"词云图"}
    word=dic_word[stock]
    dic_data["data"]=word
    if len(dic_word[stock])==0:
        dic_data["if_show"]=False
    else:
        dic_data["if_show"]=True

    json_dump = json.dumps(dic_data)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_name="word_cloud_"+stock
    redis_c.set(json_name,json_dump)
    
'''
http://47.100.219.4:7080/?topic=5&aris_data=word_cloud_"stock_code"
'''

def get_daily(df_rank_daily,yesterday,dic_daily_word):
    df_daily_index=df_rank_daily[df_rank_daily["p_date"]==yesterday].sort_values(by="setiment_index",ascending=False).reset_index(drop=True)[["stock_id","stock_name","setiment_index"]]

#找出没有数据的个股后舆情指数赋值为0
    l_stock_null=list(set(stock_list)-set(df_daily_index["stock_id"]))
    
    df_daily_index_1=pd.DataFrame()
    for stock in l_stock_null:
        dic_temp={}
        dic_temp["stock_id"]=stock
        dic_temp["setiment_index"]=0
        df_temp=pd.DataFrame([dic_temp])
        df_daily_index_1=df_daily_index_1.append(df_temp)
    df_daily_index_1=pd.merge(df_daily_index_1,df_stock_name)
    
    df_daily_index=df_daily_index.append(df_daily_index_1)  

    df_word=pd.DataFrame([dic_daily_word]).T.reset_index()
    df_word.columns=["stock_id","word"]
    
    df_daily_index_2=pd.merge(df_daily_index,df_word) 
    df_daily_index_2=df_daily_index_2[["stock_name","stock_id","setiment_index","word"]]
    df_daily_index_2.columns=["股票名称","股票代码","舆情指数","舆情热度词"]
    
    title_7="个股每日热榜"
    json_7=js.table(df_daily_index_2)(title=title_7)
    json_7_dump = json.dumps(json_7)
    json_7_name = "daily"
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    redis_c.set(json_7_name, json_7_dump)
    
    '''
    http://47.100.219.4:7080/?topic=5&aris_data=daily
    
    '''
def get_stock_industry(inds_type = '申银万国行业分类标准'):
    '''
    提取行业 code, name, stock_code
    '''
    get_stock_industry = create_engine("mysql+pymysql://db_admin:User@123@rm-uf6xzs2aqh4nx78m2.mysql.rds.aliyuncs.com:3306/xuangu?charset=utf8")
    sql_stock = "select stock_code, vary_date, f1, f2 from stock_industry_code_cn where standard = '{}'".format(inds_type)
    df_stock= pd.read_sql(sql_stock, get_stock_industry)
    df_stock["rank"]=df_stock["vary_date"].groupby(df_stock["stock_code"]).rank(ascending=0,method='dense')
    df_stock=df_stock[df_stock["rank"]==1].reset_index(drop=True).drop(['rank','vary_date'], axis=1)
    sql_industry="select OB_SORTCODE_0013, OB_SORTNAME_0013 from stock_industry_mapping where F003V_0013 = '{}'".format(inds_type)
    df_industry=pd.read_sql(sql_industry, get_stock_industry)
    df_stock_industry=pd.merge(df_stock,df_industry,left_on = 'f2', right_on = 'OB_SORTCODE_0013', how = 'left')
    df_stock_industry=df_stock_industry.drop(["f1","OB_SORTCODE_0013"],axis=1)
    df_stock_industry.columns=["stock_id","code","name"]
    return df_stock_industry


def get_stock_bk():
    # 获取选股宝概念板块
    db_share = create_engine(
        "mysql+pymysql://db_admin:User@123@rm-uf6xzs2aqh4nx78m2.mysql.rds.aliyuncs.com:3306/share?charset=utf8")
    sql = "SELECT SecuCode as code, BankuaiInfo as bk FROM bankuai"
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
    df_stock_xgb = pd.DataFrame(data, columns=['stock_id', 'name', 'code'])
    return df_stock_xgb

def get_industry(df_rank_daily,yesterday,df):
    
    '''
    行业板块每日热榜json
    '''
    
    df_rank_yesterday=df_rank_daily[df_rank_daily["p_date"]==yesterday][["stock_id","stock_name","setiment_index"]]
    df_rank_industry=pd.merge(df_rank_yesterday,df)
    
    # 行业舆情表
    df_industry_index=df_rank_industry["setiment_index"].groupby([df_rank_industry["name"],df_rank_industry["code"]]).mean().reset_index()
    
    # 提取行业top5热门股票
    dic_industry={}
    for industry in df_industry_index["code"].unique().tolist():
        df_temp=df_rank_industry[df_rank_industry["code"]==industry]
        if len(df_temp)>=5:
            df_temp_1=df_temp.sort_values(by="setiment_index")[:5]
            l_temp=df_temp_1["stock_name"].tolist()
            stock=','.join(l_temp) 
        dic_industry[industry]=stock
    # 热门股票转df
    df_industry_stock=pd.DataFrame([dic_industry]).T.reset_index()
    df_industry_stock.columns=["code","stock"]
    # 添加热门股票
    df_industry_index=pd.merge(df_industry_index,df_industry_stock)
    df_industry_index=df_industry_index[["name","code","setiment_index","stock"]]
    
    #判断是否输入的是行业板块还是概念板块,生成首页和子页面json
    try:
        df==get_stock_industry()
        df_industry_index.columns=["行业名称","行业代码","舆情指数","热门个股"]
        
        title_8="行业板块每日热榜"
        json_8=js.table(df_industry_index)(title=title_8)
        json_8["headName"]=["行业名称","舆情指数","热门个股"]
        json_8["headCol"]=["hangyemingcheng0",'yuqingzhishu2','remengegu3']
        json_8_dump = json.dumps(json_8)
        json_8_name = "industry_daily"
        redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
        redis_c.set(json_8_name, json_8_dump)
        '''
        http://47.100.219.4:7080/?topic=5&aris_data=industry_daily

        '''    
    except:
        
        df_industry_index.columns=["概念板块名称","概念板块代码","舆情指数","热门个股"]
    
        title_8="概念板块每日热榜"
        json_8=js.table(df_industry_index)(title=title_8)
        json_8["headName"]=["概念板块名称","舆情指数","热门个股"]
        json_8["headCol"]=["gainianbankuaimingcheng0",'yuqingzhishu2','remengegu3']
        json_8_dump = json.dumps(json_8)
        json_8_name = "industry_daily"
        redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
        redis_c.set(json_8_name, json_8_dump)
        '''
        http://47.100.219.4:7080/?topic=5&aris_data=industry_daily
        '''

def get_industry_stock(df_rank_daily,yesterday,df):
    
    '''
    行业/概念板块子页面
    '''
    df_rank_yesterday=df_rank_daily[df_rank_daily["p_date"]==yesterday][["stock_id","stock_name","setiment_index"]]
    df_rank_industry=pd.merge(df_rank_yesterday,df)
    
    l_industry=df_rank_industry["code"].unique().tolist()
    
    try:
        df==get_stock_industry()      
        count=0
        
        for industry in l_industry:
            
            df_industry=df_rank_industry[df_rank_industry["code"]==industry][["stock_name","stock_id","setiment_index","code"]].reset_index(drop=True)
            title_9="行业板块个股舆情指数"
            json_9=js.table(df_industry)(title=title_9)
            json_9["headName"]=["股票名称","股票代码","舆情指数"]
            json_9["headCol"]=["gupiaomingcheng0",'gupiaodaima1','yuqingzhishu2']
            json_9_dump = json.dumps(json_9)
            json_9_name = "industry_"+str(df_industry["code"][0])
            redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
            redis_c.set(json_9_name, json_9_dump)
            count=count+1
            print(count)
    
    except:
        
        count=0
        
        for industry in l_industry:
            
            df_industry=df_rank_industry[df_rank_industry["code"]==industry][["stock_name","stock_id","setiment_index","code"]].reset_index(drop=True)
            title_9="概念板块个股舆情指数"
            json_9=js.table(df_industry)(title=title_9)
            json_9["headName"]=["股票名称","股票代码","舆情指数","概念板块代码"]
            json_9["headCol"]=["gupiaomingcheng0",'gupiaodaima1','yuqingzhishu2']
            json_9_dump = json.dumps(json_9)
            json_9_name = "industry_"+str(df_industry["code"][0])
            redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
            redis_c.set(json_9_name, json_9_dump)
            count=count+1
            print(count)
    
    '''
    http://47.100.219.4:7080/?topic=5&aris_data=industry_"industry_code"
    '''
  

def get_heatmap(df_rank_daily,df=get_stock_industry()):
    
    """
    热门行业板块 热力图 Top10,一周
    """

    
    df_rank_week=df_rank_daily[df_rank_daily["p_date"]>=week2][["stock_id","stock_name","p_date","setiment_index"]]
    df_rank_industry_week=pd.merge(df_rank_week,df)


    #配合前端显示 缩短行业名称
    indus_name=[]
    for x in df_rank_industry_week["name"].tolist():
        if len(x)>=5:
            if "其他" in x:
                x=x.replace("其他","")
            if x in ["互联网传媒","农产品加工",'计算机应用','房地产开发','计算机设备']:
                x=x[:3]
            if x in ["光学光电子","环保工程及服务"]:
                x=x[:2]
            if x =="商业物业经营":
                x="商业物业"
            if x=="金属非金属新材料":
                x="新材料"
            if x=='高低压设备' or x=="电气自动化设备":
                x="电气设备"
            if x=="汽车零部件":
                x="汽车部件"
        else:
            x=x
        indus_name.append(x)
    
    df_rank_industry_week["name"]=indus_name
    
    #选出top10行业
    df_industry_index_week=df_rank_industry_week["setiment_index"].groupby(df_rank_industry_week["name"]).mean().reset_index()
    df_industry_index_week=df_industry_index_week.sort_values(by='setiment_index',ascending=False)
    
        
    l_top_industry=df_industry_index_week["name"].values.tolist()[:10]
    
    df_industry_sub=df_rank_industry_week[df_rank_industry_week["name"].isin(l_top_industry)]
           
    l_date_industry=df_industry_sub["p_date"].unique().tolist()
    
    dic_top_industry={}
    for date in l_date_industry:
        df_sub=df_industry_sub[df_industry_sub["p_date"]==date]
        dic_industry={}
        for industry in l_top_industry:
            df_temp=df_sub[df_sub["name"]==industry]
            mean=df_temp["setiment_index"].mean()
            try:
                str(int(mean)).isnumeric()
                dic_industry[industry]=df_temp["setiment_index"].mean()
            except:
                dic_industry[industry]=0

        df_industry=pd.DataFrame([dic_industry]).T.reset_index()
        df_industry.columns=["name","setiment_index"] 
        dic_top_industry[date]=df_industry
    
    dic_top={}
    dic_top["outer_title"]={"text":"热门行业板块舆情"}
    dic_top["max"]=100
    dic_top["min"]=0
    dic_top["xAxis"]=[str(i) for i in list(dic_top_industry.keys())]
    dic_top["yAxis"]=list(list(dic_top_industry.values())[0]["name"])
    
    l_top_data=[]
    
    count=0
    for date in list(dic_top_industry.keys()):
        df=dic_top_industry[date]
        for index,row in df.iterrows():
            l_temp=[i for i in range(0,3)]
            l_temp[0]=index
            l_temp[1]=count
            l_temp[2]=row["setiment_index"] 
            l_top_data.append(l_temp)
        count=count+1
        l_top_data.sort()
    
    dic_top["data"]=l_top_data
            
    json_dump = json.dumps(dic_top)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_name="top_industry"
    redis_c.set(json_name,json_dump)
    
    '''
    http://47.100.219.4:7080/?topic=5&aris_data=top_industry
    '''


    
def main():
    
    yesterday = datetime.date.today()+datetime.timedelta(days=-1)
    week1=datetime.date.today()+datetime.timedelta(days=-14)
    week2=datetime.date.today()+datetime.timedelta(days=-7)
    
    df_stock_name=get_stock_name()
    df_rank_daily=get_month_data(yesterday)
    stock_list=df_rank_daily["stock_id"].unique().tolist()
    
    count=0
    logger.info("getting month_index_line") 
    for stock in stock_list:
        month_index_line(df_rank_daily,stock)
        count=count+1
        print("get month_index_line"+str(count))
        
    logger.info("getting week_summary") 
    df_weekly_rst=week_summary(df_rank_daily,week1,week2,yesterday)
    
    
    count=0
    logger.info("getting week_summary_json") 
    for stock in stock_list:
        week_summary_json(stock,df_weekly_rst)
        count=count+1
        print("get week_summary_json"+str(count))
        
    
    count=0
    logger.info("getting specific_line") 
    for stock in stock_list:
        get_specific_line(df_rank_daily,stock)
        count=count+1
        print("get specific_line"+str(count))
        
    df_specific_1= get_day3_summary(df_rank_daily)
    
    count=0
    logger.info("getting day3_summary_json") 
    for stock in stock_list:
        get_day3_summary_json(df_specific_1,stock)
        count=count+1
        print("get day3_summary_json"+str(count))
    
    df_specific_2=get_week_summary(df_rank_daily,week2)
     
    count=0
    logger.info("getting week_summary_json") 
    for stock in stock_list:
        get_week_summary_json(df_specific_2,stock)
        count=count+1
        print("get week_summary_json"+str(count))
    
    df_specific_3=get_yesterday_summary(df_rank_daily,yesterday)
      
    count=0
    logger.info("getting yesterday_summary_json") 
    for stock in stock_list:
        get_yesterday_summary_json(df_specific_3,stock)
        count=count+1
        print("get yesterday_summary_json"+str(count))
        
    logger.info("getting wordcloud_dic") 
    dic_word=get_wordcloud()
    
    count=0
    logger.info("getting wordcloud_json") 
    for stock in stock_list:
        wordcloud_json(dic_word)
        count=count+1
        print("get wordcloud_json"+str(count))
        
    #生成热度关键词TOP5
    dic_daily_word={}
    for stock in stock_list:
        l_word=[]
        if len(dic_word[stock])>=5:
            for i in range(0,5):
                word=dic_word[stock][i]["name"]
                l_word.append(word)
            word_1=','.join(l_word) 
        else:
            word_1=""
        dic_daily_word[stock]=word_1
    
    logger.info("getting heatmap")
    get_heatmap(df_rank_daily,df=get_stock_industry())
     
    logger.info("getting daily") 
    get_daily(df_rank_daily,yesterday,dic_daily_word)
    
    logger.info("getting industry") 
    get_industry(df_rank_daily,yesterday,df=get_stock_industry())
    
    logger.info("getting industry_stock") 
    get_industry_stock(df_rank_daily,yesterday,df=get_stock_industry())
    
    
main()



    


  
    
    
    
    
    
    
    
    

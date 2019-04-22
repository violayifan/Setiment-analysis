# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 15:05:57 2019

@author: viola
"""

'''
Part one 获取数据
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

logger = logging.getLogger('cloudword')  
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('cloudword.log')  
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  
fh.setFormatter(formatter)  
ch.setFormatter(formatter)  
logger.addHandler(fh)  
logger.addHandler(ch) 

'''
Part 1 获取雪球数据
'''
#取stock list

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

l_stock=df_stock_name["stock_id"].sort_values().tolist()

#sql取每个股票按时间排序取最近100条评论内容
def getdata(engine,sql):
    starttime = datetime.datetime.now()
    conn = engine.connect()
    getrst = pd.read_sql(sql,conn)
    conn.close()
    endtime = datetime.datetime.now()
    logger.info('......it took {} seconds to get rawdata.'.format((endtime - starttime).seconds))
    return getrst

def get_raw():
    engine_server_questions_ans = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/questions_ans?charset=utf8')
    sql_ans_xueqiu_comment = "select about,pub_time,comment_num,answer from xueqiu_comment where answer is not Null;"
    #sql = "select about,pub_time,comment_num,answer from xueqiu_comment where answer is not Null AND about={} ORDER BY pub_time DESC LIMIT 100;".format(each)
    raw_data = getdata(engine_server_questions_ans,sql_ans_xueqiu_comment)
    return raw_data

df_xueqiu=get_raw()

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
Part 3 生成云图
'''

import jieba.analyse

jieba.analyse.set_stop_words("../Desktop/stopwords.txt")

dic_word={}
count=0
for stock in l_answer_stock:
    data=jieba.analyse.extract_tags(dic_answer[stock], topK=50, withWeight=True, allowPOS=('nl','vn','vl','an','al','z'))
    l_data=[]
    for i in range(0,len(data)):
        dic_temp_word={}
        temp=data[i]
        dic_temp_word["name"]=temp[0]
        dic_temp_word["value"]=temp[1]
        l_data.append(dic_temp_word)
    dic_word[stock]=l_data
    count=count+1
    print(count)
        
'''
Part 4 上传redis
'''
import redis
count=0
for each in l_answer_stock:
    dic_data={}
    word=dic_word[each]
    dic_data["data"]=word
    json_dump = json.dumps(dic_data)
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
    json_name="word_cloud_"+each
    redis_c.set(json_name,json_dump)
    count=count+1
    print(count)
'''
http://47.100.219.4:7080/?topic=5&aris_data=word_cloud_"stock_code"
'''









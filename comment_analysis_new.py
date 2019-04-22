# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

"""

"""
Part one 收益率(T+n)和评论数量(T+0)的关系
"""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json
from urllib import parse,request
import datetime
import logging
import matplotlib.pyplot as plt


logger = logging.getLogger('comment')  
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('comment_analysis.log')  
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

def get_raw():
    engine_server_questions_ans = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/questions_ans?charset=utf8')
    sql_get_question_ans_xueqiu_comment = "select about,pub_time,comment_num,answer from xueqiu_comment where answer is not Null;"
    raw_data = getdata(engine_server_questions_ans,sql_get_question_ans_xueqiu_comment)
    return raw_data

def adjust_raw(raw_data):
    # pub_time 时间转成str 用于统计合并
    pub_time=raw_data["pub_time"]
    pub_time_date=[str(x)[:10] for x in pub_time]
    pub_time_filter = [str(x)[:4] for x in pub_time]
    raw_data['pub_time'] = pub_time_date
    raw_data['filter'] = pub_time_filter
    
    # 2017之前数据离散程度高，删除
    temp_l = []
    for i in pub_time_filter:
        try:
            i = int(i)
            if i <2017:
                temp_l.append(np.nan)
            else:
                temp_l.append(str(i))
        except:
            temp_l.append(np.nan)
    raw_data["filter"]=temp_l
    adjusted_data=raw_data.dropna(subset=["filter"])
    
    # 统计合并 groupby pub_time
    adjusted_data=adjusted_data[["about","comment_num","pub_time"]]
    adjusted_data_new=adjusted_data.groupby(["pub_time","about"])["comment_num"].sum()
    adjusted_data_new = adjusted_data_new.reset_index()
    
    # comment_num + 1
    comment_num=adjusted_data_new["comment_num"]+1
    adjusted_data_new["comment_num"]=comment_num
    
    # incorporate weight
    adjusted_data_new["weight"]=1
    adjusted_data_new["score"]=adjusted_data_new["weight"]*adjusted_data_new["comment_num"]
    
    # output columns
    adjusted_data_new.columns=["date","stock_code","comment_num","weight","score"]
    # date type change 
    temp_l = adjusted_data_new['date']
    temp_l = [datetime.datetime.strptime(x,"%Y-%m-%d") for x in temp_l]
    adjusted_data_new['date'] = temp_l
    return adjusted_data_new[["date","stock_code","score"]]

def get_stock_val(stock_l):
    '''
    get total stock vlue
    '''
    d_stock_price={}  # buffer memory
    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}
    def get_stock_price(code):
        """
        get price df for each stock
        """
        # buffer memory
        if code in d_stock_price:
            return d_stock_price[code]
        # get mkt
        if code[0]=='6':
            mkt='sh'
        else:
            mkt='sz'
        
        url="http://nujump.tigerobo.com/HB_Jumper/pd.ashx?id=%s|%s&type=k&rtype=1&style=top&num=556&js=(x)&at=fa"%(code, mkt)
        req = request.Request(url=url,headers=header_dict)
        res = request.urlopen(req)
        res = res.read()
        
        r=res.decode(encoding='utf-8')
        
        if r=='{stats:false}':
            return -1
        #d=json.loads(r,strict=False)
        
        r_l=r.split('\n')
        
        data_l=[]
        
        for r_i in r_l:
            r_i_t=r_i.split(',')
            
            data_l.append([datetime.datetime.strptime(r_i_t[0], "%Y-%m-%d"),float(r_i_t[1]), float(r_i_t[2])])
            
        if len(data_l)==0:
            return -1
        
        df=pd.DataFrame(data_l,columns=['date','open','close'])
        df['stock_code']=code
        # return 用open close计算
#        return_l = []
#        for each in range(len(df)):
#            if temp_l==0:
#                return_temp=np.nan
#            else:
#                temp_l = list(df.iloc[each])
#                return_temp = temp_l[2] / temp_l[1] - 1
#
#            return_l.append(return_temp)
#        df['return'] = return_l
        d_stock_price[code]=df
        
        return d_stock_price[code]
    
    count = 0 
    for each in stock_l:
        rst = get_stock_price(each)
        d_stock_price[each] = rst
        # count to observe process
        count += 1
        if count % 30 == 0:
            print(count)
    
    return d_stock_price

def get_multiple_return(df):
    '''
    次日开盘买入，之后n日卖出的return
    '''
    # t+1开盘买入，t+2开盘卖出
    df['r1']=(df['open'].shift(-2)/df['open'].shift(-1))-1
    # t+1开盘买入，t+3收盘卖出
    df['r2']=df['close'].shift(-3)/df['open'].shift(-1)-1
    # t+1开盘买入，t+5收盘卖出
    df['r3']=df['close'].shift(-5)/df['open'].shift(-1)-1
    # t+1开盘买入，t+7收盘卖出
    df['r4']=df['close'].shift(-7)/df['open'].shift(-1)-1
    return df

def get_merge(df1,df2):
    '''
    news_day in return_day: 一切正常
    news_day not in return_day: 日期向前找，因为周六的消息经过周日，在周一发酵，相当于周五发生消息，在周一买入，周二卖出
    '''
    df_output = pd.DataFrame()
    # 取出日期交集处理
    df_merge1 = pd.merge(df1, df2, how = 'inner', on = ['stock_code','date'])
    df_output = df_output.append(df_merge1)
    
    # 取出不交集的元素
    date_df1 = set(df1['date'])
    date_df2 = set(df2['date'])
    dif_date = list(date_df1 - date_df2)
    df_gap = df1.copy()
    
    count = 0 # 控制向前看几天
    while len(dif_date) >0 and count < 10:
        df_gap = df_gap[df_gap['date'].isin(dif_date)]
        # 找前一个交易日
        new_date = [x - datetime.timedelta(days = 1) for x in df_gap['date']]
        df_gap['date'] = new_date
        # merge
        df_merge2 = pd.merge(df_gap, df2, how = 'inner', on = ['stock_code','date'])
        df_output = df_output.append(df_merge2)
        # 取出不交集的元素
        date_df1 = set(df_gap['date'])
        dif_date = list(date_df1 - date_df2)
        
        count += 1
    print('以下交易日无数据\n {}'.format(dif_date))
    return df_output

'''
评论内容情绪标签
'''
#评论内容去除标签
import re

def cleanhtml(raw_html):
    
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    cleantext = re.sub(r"\s+", "", cleantext)
    return cleantext
l_answer=[]

raw_news=raw_news.reset_index(drop=True)
     
l_answer_new=[re.sub("\$.+?\$", "", i) for i in l_answer]
l_answer_new_1=[i.replace("&nbsp;","") for i in l_answer_new]
l_answer_new_2=[]
for i in l_answer_new_1:
    if i=="":
        i=np.nan
    l_answer_new_2.append(i)

l_answer_new_3=[]

for i in l_answer_new_2:
    if i is not np.nan and re.findall("^http",i):
        i=np.nan
    l_answer_new_3.append(i)
    

raw_news["answer_clean"]=l_answer_new_3
raw_news_1=raw_news.dropna(subset=["answer_clean"])


#判断情感正负面打分
l_answer=raw_news_1["answer_clean"].tolist()

def sentiment(answer):
    
    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}
    url='https://sentiment-internal.tigerobo.com/text'
    
    query_d={"content":answer}
    
    
    query_str=json.dumps(query_d)
    
    req = request.Request(url=url,data=query_str.encode(encoding = "utf-8"),headers=header_dict)
    res = request.urlopen(req)
    res = res.read().decode(encoding = "utf-8")
    d=json.loads(res)
    return d
list_rate=[]
i=0
for each in l_answer:
    dic=sentiment(each)
    list_rate.append(dic["data"]["rate"])
    i=i+1
    print(i)








'''
1 取数据
'''
# get news
raw_news = get_raw()
# adjust news date type(process comment_num)
adjusted_data_new = adjust_raw(raw_news)

#process answer content&score

stock_list = list(set(adjusted_data_new['stock_code'].tolist()))
# get stock return dic
stock_return_dic =  get_stock_val(stock_list)

'''
2 for循环 根据stock_code 匹配 生成df 计算收益率
'''

output_df = pd.DataFrame()
for each_stock in stock_list:
    # get data
    news_df = adjusted_data_new[adjusted_data_new['stock_code'] == each_stock]
    return_df = stock_return_dic[each_stock]  # 可能 == 1
    # merge
    if type(return_df) != int:
        # 计算多日收益率
        final_return_df = get_multiple_return(return_df)
        # 根据规则merge
        df_merge = get_merge(news_df, final_return_df)
        output_df = output_df.append(df_merge)


'''
3 十分组查看收益率
'''
#收益率和评论数量的关系

#统计分布
output_df["score"].describe()


#直方图
#plt.hist(output_df["score"], bins=100,range=(0,100))

#按统计分布十分组
def group_by():
    
    dic_group={}
    for i in range(1,10):
        df1=output_df[output_df["score"]==i]
        dic_group[i]=df1
    df2=output_df[output_df["score"]>=10]
    dic_group[10]=df2
    return(dic_group)

dic_group=group_by()

#取平均收益率
def group_mean_return():
    list_return_1=[]
    list_return_2=[]
    list_return_3=[]
    list_return_4=[]
    for i in range(1,11):
        group_mean=dic_group[i].mean()
        for x in range(1,5):
            index="r"+str(x)
            subgroup_mean=group_mean[index]
            if x==1:
                list_return_1.append(subgroup_mean)
            if x==2:
                list_return_2.append(subgroup_mean)
            if x==3:
                list_return_3.append(subgroup_mean)
            if x==4:
                list_return_4.append(subgroup_mean)
    return(list_return_1,list_return_2,list_return_3,list_return_4)
list_return=group_mean_return()
               

#作图
for i in range(0,4):
    plt.bar(range(len(list_return[i])), list_return[i]) 
    plt.show()
    



"""
Part Two 根据评论热度选股
"""
"""
选10分组中的第十组查看net value
"""

#交易日期列表
date_list=return_df["date"].drop_duplicates().reset_index(drop=True)
date_list=date_list.drop(index=[i for i in range(0,15)]).reset_index(drop=True)

#选股

dic_net_value={}
dic_net_value[date_list[0]]=1
dic_subchoice={}
dic_mean_return={}

#按照评论数前10选个股，并计算净值
net_value=1
for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice=output_df[output_df["date"]==str(date_open)]
    if len(df_subchoice)<10:
        df_subchoice_1=df_subchoice.sort_values(by="score",ascending=False)[:len(df_subchoice)+1]
    else:
        df_subchoice_1=df_subchoice.sort_values(by="score",ascending=False)[:10]
        dic_subchoice[date_open]=df_subchoice_1
        
        n=len(df_subchoice_1)
        mean_return=(df_subchoice_1["r4"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+7
        date_close=date_list[y]
        dic_net_value[date_close]=net_value
        dic_mean_return[date_close]=mean_return

#选第十分组的全部
df_choice=dic_group[10]
net_value_10=1
dic_net_value_10={}
dic_net_value_10[date_list[0]]=1
dic_subchoice_10={}
dic_mean_return_10={}
l_subchoice_none=[]
for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_10=df_choice[df_choice["date"]==str(date_open)]
    dic_subchoice_10[date_open]=df_subchoice_10

    if len(df_subchoice_10)==0:
        net_value_10=0
        l_subchoice_none.append(date_open)
    else:        
        n=len(df_subchoice_10)
        mean_return=(df_subchoice_1["r4"].mean())/n
        net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_10[date_close]=net_value
    dic_mean_return_10[date_close]=mean_return



#作图
df_rtn_10=pd.DataFrame([dic_mean_return_10]).T
df_net_value_10=pd.DataFrame([dic_net_value_10]).T

df_net_value_10.columns=["net_value"]
l_date=[i for i in df_net_value_10.index]
l_net_value =[i for i in df_net_value_10["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

"""
第十分组中再分5组看net value
"""

#按rank分组算nv
df_rank=dic_group[10].reset_index(drop=True)

#把交易日中样本小于5的取出来
df_rank_1=pd.DataFrame()
df_rank_2=pd.DataFrame()
df_rank_3=pd.DataFrame()
df_rank_4=pd.DataFrame()
df_rank_5=pd.DataFrame()
for x in range(0,len(date_list)):
    date=date_list[x]
    df_rank_date=df_rank[df_rank["date"]==str(date)]
    if len(df_rank_date)<5:
        if len(df_rank_date)==1:
            df_rank_1=df_rank_1.append(df_rank_date).reset_index(drop=True)
        if len(df_rank_date)==2:
            df_rank_2=df_rank_2.append(df_rank_date).reset_index(drop=True)
        if len(df_rank_date)==3:
            df_rank_3=df_rank_3.append(df_rank_date).reset_index(drop=True)
        if len(df_rank_date)==4:
            df_rank_4=df_rank_4.append(df_rank_date).reset_index(drop=True)
    else:
        df_rank_5=df_rank_5.append(df_rank_date).reset_index(drop=True)
#填充
l_date_1=df_rank_1["date"].tolist()
for date in l_date_1:
    i=1
    fill=df_rank_1[df_rank_1["date"]==str(date)]
    while i<5:
        df_rank_1=df_rank_1.append(fill)
        i=i+1

l_date_2=list(set(df_rank_2["date"]))
for date in l_date_2:
    select=df_rank_2[df_rank_2["date"]==str(date)]
    select_1=df_rank_2[df_rank_2["date"]==str(date)]["score"]
    if len(select_1.unique())==1:
        n=1
        select=select.reset_index(drop=True)
        while n<4:
            select_split=select.iloc[0]
            df_rank_2=df_rank_2.append(select_split)
            n=n+1
    else:
        select_3=select[select["score"]==max(select_1)]
        select_4=select[select["score"]==min(select_1)]
        df_rank_2=df_rank_2.append(select_4)
        i=1
        while i<3:
            df_rank_2=df_rank_2.append(select_3)
            i=i+1
        
l_date_3=list(set(df_rank_3["date"]))
for date in l_date_3:    
    select=df_rank_3[df_rank_3["date"]==str(date)]
    select_1=df_rank_3[df_rank_3["date"]==str(date)]["score"]
    if len(select_1.unique())==1 or len(select_1.unique())==2:
        n=1
        select=select.reset_index(drop=True)
        while n<3:
            select_split=select.iloc[2]
            df_rank_3=df_rank_3.append(select_split)
            n=n+1
    else:    
        select_2=select[select["score"]==max(select_1)]
        i=1
        while i<3:
            df_rank_3=df_rank_3.append(select_2)
            i=i+1

l_date_4=list(set(df_rank_4["date"]))
for date in l_date_4:    
    select=df_rank_4[df_rank_4["date"]==str(date)]
    select_1=df_rank_4[df_rank_4["date"]==str(date)]["score"]
    if len(select_1.unique())==1 or len(select_1.unique())==2 or len(select_1.unique())==3:
        select=select.reset_index(drop=True)
        select_split=select.iloc[3]
        df_rank_4=df_rank_4.append(select_split)
    else:    
        select_2=select[select["score"]==max(select_1)]    
        df_rank_4=df_rank_4.append(select_2)
# 分组
#(1)fill过的
        
df_rank_1=df_rank_1.sort_values(by="date").reset_index(drop=True)
df_rank_2=df_rank_2.sort_values(by="date").reset_index(drop=True)
df_rank_3=df_rank_3.sort_values(by="date").reset_index(drop=True)
df_rank_4=df_rank_4.sort_values(by="date").reset_index(drop=True)

list_rank=[i for i in range(1,6)]
df_rank_1_new=pd.DataFrame()
df_rank_2_new=pd.DataFrame()
df_rank_3_new=pd.DataFrame()
df_rank_4_new=pd.DataFrame()

for date in l_date_1:
    select=df_rank_1[df_rank_1["date"]==str(date)]
    select["group"]=list_rank
    df_rank_1_new=df_rank_1_new.append(select)

for date in l_date_2:
    select=df_rank_2[df_rank_2["date"]==str(date)]
    select=select.sort_values(by="score")
    select["group"]=list_rank
    df_rank_2_new=df_rank_2_new.append(select)

for date in l_date_3:
    select=df_rank_3[df_rank_3["date"]==str(date)]
    select=select.sort_values(by="score")
    select["group"]=list_rank
    df_rank_3_new=df_rank_3_new.append(select)

for date in l_date_4:
    select=df_rank_4[df_rank_4["date"]==str(date)]
    select=select.sort_values(by="score")
    select["group"]=list_rank
    df_rank_4_new=df_rank_4_new.append(select)

#(2)the rest
df_rank_5=df_rank_5.sort_values(by="date").reset_index(drop=True)
l_date_5=list(set(df_rank_5["date"]))

df_rank_5_new=pd.DataFrame()
#5分组
for date in l_date_5:
    select=df_rank_5[df_rank_5["date"]==str(date)]
    if len(select)==5:
        select=select.sort_values(by="score")
        select["group"]=list_rank
        df_rank_5_new=df_rank_5_new.append(select)
    else:       
        try:
            #按照rank百分比分组
            bins=np.percentile(select["score"],[0,20,40,60,80,100])
            select["group"]=pd.cut(select["score"],bins,labels=[1,2,3,4,5])
            df_rank_5_new=df_rank_5_new.append(select) 
        except:
            #按照长度等分
            select=select.sort_values(by="score")
            select=select.reset_index(drop=True)
            norm=int(len(select)/5)
            select_split_1=select.iloc[0:norm+1]
            select_split_1["group"]=1
            df_rank_5_new=df_rank_5_new.append(select_split_1)
            
            select_split_2=select.iloc[norm+1:norm*2+1]
            select_split_2["group"]=2
            df_rank_5_new=df_rank_5_new.append(select_split_2)
            
            select_split_3=select.iloc[norm*2+1:norm*3+1]
            select_split_3["group"]=3
            df_rank_5_new=df_rank_5_new.append(select_split_3)
            
            select_split_4=select.iloc[norm*3+1:norm*4+1]
            select_split_4["group"]=4
            df_rank_5_new=df_rank_5_new.append(select_split_4)
    
            select_split_5=select.iloc[norm*4+1:]
            select_split_5["group"]=5
            df_rank_5_new=df_rank_5_new.append(select_split_5)
    df_rank_5_new["group"].fillna(1,inplace=True)

#修正bug
df_rank_5_new=df_rank_5_new.sort_values(by="date")
df_rank_5_new=df_rank_5_new.reset_index(drop=True)
df_rank_5_new.loc[[662,668,671],["group"]]=2
        
        
        

#合并相同分组到同一个df中

df_bygroup=pd.concat([df_rank_1_new,df_rank_2_new,df_rank_3_new,df_rank_4_new,df_rank_5_new])
df_bygroup=df_bygroup.sort_values(by="date").reset_index(drop=True)

#第一组
choose_group1=df_bygroup[df_bygroup["group"]==1].sort_values(by="date").reset_index(drop=True)

#第二组
choose_group2=df_bygroup[df_bygroup["group"]==2].sort_values(by="date").reset_index(drop=True)


#第三组
choose_group3=df_bygroup[df_bygroup["group"]==3].sort_values(by="date").reset_index(drop=True)

#第四组
choose_group4=df_bygroup[df_bygroup["group"]==4].sort_values(by="date").reset_index(drop=True)

#第五组
choose_group5=df_bygroup[df_bygroup["group"]==5].sort_values(by="date").reset_index(drop=True)




#calculate net value

list_choose_group=[choose_group1,choose_group2,choose_group3,choose_group4,choose_group5]

def net_value_cal(n,df,r):                                     
    net_value=1
    dic_net_value={}
    dic_net_value[date_list[0]]=1
    dic_mean_return={}
    dic_sub={}
    for a in range(0,len(date_list)-n,n):                       
        date_open=date_list[a]
        df_sub=df[df["date"]==str(date_open)]        
        dic_sub[date_open]=df_sub
        x=len(df_sub)
        if len(df_sub)==0:
            pass
        else:
                mean_return=(df_sub[r].mean())/x
                net_value=net_value*(1+mean_return)
                b=a+n
                date_close=date_list[b]
                dic_net_value[date_close]=net_value
                dic_mean_return[date_close]=mean_return
    return dic_net_value,dic_mean_return,dic_sub


list_r4=[]
for i in range(0,5):
        df=list_choose_group[i]
        net_value_cal_r4=net_value_cal(7,df,"r4")
        list_r4.append(net_value_cal_r4)

dic_net_value_group1_r4=list_r4[0][0]
dic_mean_return_group1_r4=list_r4[0][1]
dic_subchoice_group1_r4=list_r4[0][2]

dic_net_value_group2_r4=list_r4[1][0]
dic_mean_return_group2_r4=list_r4[1][1]
dic_subchoice_group2_r4=list_r4[1][2]

dic_net_value_group3_r4=list_r4[2][0]
dic_mean_return_group3_r4=list_r4[2][1]
dic_subchoice_group3_r4=list_r4[2][2]

dic_net_value_group4_r4=list_r4[3][0]
dic_mean_return_group4_r4=list_r4[3][1]
dic_subchoice_group4_r4=list_r4[3][2]

dic_net_value_group5_r4=list_r4[4][0]
dic_mean_return_group5_r4=list_r4[4][1]
dic_subchoice_group5_r4=list_r4[4][2]




list_r3=[]
for i in range(0,5):
        df=list_choose_group[i]
        net_value_cal_r3=net_value_cal(5,df,"r3")
        list_r3.append(net_value_cal_r3)

dic_net_value_group1_r3=list_r3[0][0]
dic_mean_return_group1_r3=list_r3[0][1]
dic_subchoice_group1_r3=list_r3[0][2]

dic_net_value_group2_r3=list_r3[1][0]
dic_mean_return_group2_r3=list_r3[1][1]
dic_subchoice_group2_r3=list_r3[1][2]

dic_net_value_group3_r3=list_r3[2][0]
dic_mean_return_group3_r3=list_r3[2][1]
dic_subchoice_group3_r3=list_r3[2][2]

dic_net_value_group4_r3=list_r3[3][0]
dic_mean_return_group4_r3=list_r3[3][1]
dic_subchoice_group4_r3=list_r3[3][2]

dic_net_value_group5_r3=list_r3[4][0]
dic_mean_return_group5_r3=list_r3[4][1]
dic_subchoice_group5_r3=list_r3[4][2]


list_r2=[]
for i in range(0,5):
        df=list_choose_group[i]
        net_value_cal_r2=net_value_cal(3,df,"r2")
        list_r2.append(net_value_cal_r2)

dic_net_value_group1_r2=list_r2[0][0]
dic_mean_return_group1_r2=list_r2[0][1]
dic_subchoice_group1_r2=list_r2[0][2]

dic_net_value_group2_r2=list_r2[1][0]
dic_mean_return_group2_r2=list_r2[1][1]
dic_subchoice_group2_r2=list_r2[1][2]

dic_net_value_group3_r2=list_r2[2][0]
dic_mean_return_group3_r2=list_r2[2][1]
dic_subchoice_group3_r2=list_r2[2][2]

dic_net_value_group4_r2=list_r2[3][0]
dic_mean_return_group4_r2=list_r2[3][1]
dic_subchoice_group4_r2=list_r2[3][2]

dic_net_value_group5_r2=list_r2[4][0]
dic_mean_return_group5_r2=list_r2[4][1]
dic_subchoice_group5_r2=list_r2[4][2]



list_r1=[]
for i in range(0,5):
        df=list_choose_group[i]
        net_value_cal_r1=net_value_cal(2,df,"r1")
        list_r1.append(net_value_cal_r1)

dic_net_value_group1_r1=list_r1[0][0]
dic_mean_return_group1_r1=list_r1[0][1]
dic_subchoice_group1_r1=list_r1[0][2]

dic_net_value_group2_r1=list_r1[1][0]
dic_mean_return_group2_r1=list_r1[1][1]
dic_subchoice_group2_r1=list_r1[1][2]

dic_net_value_group3_r1=list_r1[2][0]
dic_mean_return_group3_r1=list_r1[2][1]
dic_subchoice_group3_r1=list_r1[2][2]

dic_net_value_group4_r1=list_r1[3][0]
dic_mean_return_group4_r1=list_r1[3][1]
dic_subchoice_group4_r1=list_r1[3][2]

dic_net_value_group5_r1=list_r1[4][0]
dic_mean_return_group5_r1=list_r1[4][1]
dic_subchoice_group5_r1=list_r1[4][2]


# plot   
 
list_group_r1=[dic_net_value_group1_r1,dic_net_value_group2_r1,dic_net_value_group3_r1,dic_net_value_group4_r1,dic_net_value_group5_r1]
list_group_r2=[dic_net_value_group1_r2,dic_net_value_group2_r2,dic_net_value_group3_r2,dic_net_value_group4_r2,dic_net_value_group5_r2]
list_group_r3=[dic_net_value_group1_r3,dic_net_value_group2_r3,dic_net_value_group3_r3,dic_net_value_group4_r3,dic_net_value_group5_r3]
list_group_r4=[dic_net_value_group1_r4,dic_net_value_group2_r4,dic_net_value_group3_r4,dic_net_value_group4_r4,dic_net_value_group5_r4]

def plot_net_value(list_group,df_sh,r):
    l_date=[]
    l_net_value=[]
    for x in range(0,5):
        df=pd.DataFrame([list_group[x]]).T
        df.columns=["net_value"]
        l_date_sub=[i for i in df.index]
        l_date.append(l_date_sub)
        l_net_value_sub =[i for i in df["net_value"].values]
        l_net_value.append(l_net_value_sub)
    plt.figure(figsize=(12,6))
    #plt.xlim(datetime.datetime.now()+datetime.timedelta(days=-365),datetime.datetime.now())
    plt.plot(l_date[0],l_net_value[0],label="group1")
    plt.plot(l_date[1],l_net_value[1],label="group2")
    plt.plot(l_date[2],l_net_value[2],label="group3")
    plt.plot(l_date[3],l_net_value[3],label="group4")
    plt.plot(l_date[4],l_net_value[4],label="group5")

    date=[i for i in df_sh["date"].values]
    return_sh=[i for i in df_sh[r].values]
    plt.plot(date,return_sh,label="sh_exchange")

    plt.legend()
    plt.show()

   

plot_net_value(list_group_r1,df_sh_r1,"r1")
plot_net_value(list_group_r2,df_sh_r2,"r2")
plot_net_value(list_group_r3,df_sh_r3,"r3")
plot_net_value(list_group_r4,df_sh_r4,"r4")




def get_sh_exchange():
    code = '000001.sh'
    
    
    url="http://nujump.tigerobo.com/HB_Jumper/pd.ashx?id=000001|sh&type=k&rtype=1&style=top&num=2400&js=(x)&at=fa"
    
    
    req = request.Request(url=url)
    res=''
    while res=='':
        try:
            res = request.urlopen(req)
        except:
            print(res,type(res))
            time.sleep(2)
    
    res = res.read()
    
    r=res.decode(encoding='utf-8')
    
    if r=='{stats:false}':
        return -1
    #d=json.loads(r,strict=False)
    
    r_l=r.split('\n')
    
    data_l=[]
    
    
    for r_i in r_l:
        r_i_t=r_i.split(',')
        
        data_l.append([datetime.datetime.strptime(r_i_t[0], "%Y-%m-%d"),float(r_i_t[1]), float(r_i_t[2])])

    if len(data_l)==0:
        return -1

    df_sh=pd.DataFrame(data_l,columns=['date','open','close'])
    df_sh['code']=code
    
    
    return df_sh

df_sh=get_sh_exchange()
df_sh=df_sh[df_sh["date"]>=str(date_list[0])].reset_index(drop=True)
df_sh=get_multiple_return(df_sh)

def net_value(n,df,r):
    date_list=df_sh["date"]                                 
    net_value=1
    dic_net_value={}
    dic_net_value[date_list[0]]=1
    for a in range(0,len(date_list)-n,n):                       
        date_open=date_list[a]
        df_sub=df[df["date"]==str(date_open)]  
        net_value_1=float(df_sub[r].values)
        net_value=net_value*(1+net_value_1)
        b=a+n
        date_close=date_list[b]
        dic_net_value[date_close]=net_value
    return dic_net_value

dic_net_value_sh_r1=net_value(2,df_sh,"r1")
df_sh_r1=pd.DataFrame([dic_net_value_sh_r1]).T.reset_index()
df_sh_r1.columns=["date","r1"]

dic_net_value_sh_r2=net_value(3,df_sh,"r2")
df_sh_r2=pd.DataFrame([dic_net_value_sh_r2]).T.reset_index()
df_sh_r2.columns=["date","r2"]

dic_net_value_sh_r3=net_value(5,df_sh,"r3")
df_sh_r3=pd.DataFrame([dic_net_value_sh_r3]).T.reset_index()
df_sh_r3.columns=["date","r3"]

dic_net_value_sh_r4=net_value(7,df_sh,"r4")
df_sh_r4=pd.DataFrame([dic_net_value_sh_r4]).T.reset_index()
df_sh_r4.columns=["date","r4"]






















'''

'''
r4
'''
net_value=1
dic_net_value_group1_r4={}
dic_net_value_group1_r4[date_list[0]]=1
dic_subchoice_group1_r4={}
dic_mean_return_group1_r4={}

#group1 net value
for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_group1_r4=choose_group1[choose_group1["date"]==str(date_open)]
    dic_subchoice_group1_r4[date_open]=df_subchoice_group1_r4    
    n=len(df_subchoice_group1_r4)
    mean_return=(df_subchoice_group1_r4["r4"].mean())/n
    net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_group1_r4[date_close]=net_value
    dic_mean_return_group1_r4[date_close]=mean_return
    
#group2 net value
net_value=1
dic_net_value_group2_r4={}
dic_net_value_group2_r4[date_list[0]]=1
dic_subchoice_group2_r4={}
dic_mean_return_group2_r4={}
list_none=[]

for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_group2_r4=choose_group2[choose_group2["date"]==str(date_open)]
    dic_subchoice_group2_r4[date_open]=df_subchoice_group2_r4  
    n=len(df_subchoice_group2_r4)
    mean_return=(df_subchoice_group2_r4["r4"].mean())/n
    net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_group2_r4[date_close]=net_value
    dic_mean_return_group2_r4[date_close]=mean_return
#group3 net value
net_value=1
dic_net_value_group3_r4={}
dic_net_value_group3_r4[date_list[0]]=1
dic_subchoice_group3_r4={}
dic_mean_return_group3_r4={} 

for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_group3_r4=choose_group3[choose_group3["date"]==str(date_open)]
    dic_subchoice_group3_r4[date_open]=df_subchoice_group3_r4   
    n=len(df_subchoice_group3_r4)
    mean_return=(df_subchoice_group3_r4["r4"].mean())/n
    net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_group3_r4[date_close]=net_value
    dic_mean_return_group3_r4[date_close]=mean_return    
#group4 net value
net_value=1
dic_net_value_group4_r4={}
dic_net_value_group4_r4[date_list[0]]=1
dic_subchoice_group4_r4={}
dic_mean_return_group4_r4={} 

for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_group4_r4=choose_group4[choose_group4["date"]==str(date_open)]
    dic_subchoice_group4_r4[date_open]=df_subchoice_group4_r4   
    n=len(df_subchoice_group4_r4)
    mean_return=(df_subchoice_group4_r4["r4"].mean())/n
    net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_group4_r4[date_close]=net_value
    dic_mean_return_group4_r4[date_close]=mean_return
#group5 net value
net_value=1
dic_net_value_group5_r4={}
dic_net_value_group5_r4[date_list[0]]=1
dic_subchoice_group5_r4={}
dic_mean_return_group5_r4={} 

for x in range(0,len(date_list)-7,7):
    date_open=date_list[x]
    df_subchoice_group5_r4=choose_group5[choose_group5["date"]==str(date_open)]
    dic_subchoice_group5_r4[date_open]=df_subchoice_group5_r4  
    n=len(df_subchoice_group5_r4)
    mean_return=(df_subchoice_group5_r4["r4"].mean())/n
    net_value=net_value*(1+mean_return)
    y=x+7
    date_close=date_list[y]
    dic_net_value_group5_r4[date_close]=net_value
    dic_mean_return_group5_r4[date_close]=mean_return

#作图 r4
list_group_r4=[dic_net_value_group1_r4,dic_net_value_group2_r4,dic_net_value_group3_r4,dic_net_value_group4_r4,dic_net_value_group5_r4]
for x in range(0,5):
    df=pd.DataFrame([list_group_r4[x]]).T
    df.columns=["net_value"]
    l_date=[i for i in df.index]
    l_net_value =[i for i in df["net_value"].values]
    plt.figure(figsize=(12,6))
    plt.plot(l_date,l_net_value)
    plt.show()
    
    
'''
'''
   
df_rtn_1_r4=pd.DataFrame([dic_net_value_group1_r4]).T
df_net_value_group1_r4=pd.DataFrame([dic_net_value_group1_r4]).T
df_net_value_group1_r4.columns=["net_value"]
l_date=[i for i in df_net_value_group1_r4.index]
l_net_value =[i for i in df_net_value_group1_r4["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_2_r4=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2.columns=["net_value"]
l_date=[i for i in df_net_value_group2.index]
l_net_value =[i for i in df_net_value_group2["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_3=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group3=pd.DataFrame([dic_net_value_group3]).T
df_net_value_group3.columns=["net_value"]
l_date=[i for i in df_net_value_group3.index]
l_net_value =[i for i in df_net_value_group3["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4.columns=["net_value"]
l_date=[i for i in df_net_value_group4.index]
l_net_value =[i for i in df_net_value_group4["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5.columns=["net_value"]
l_date=[i for i in df_net_value_group5.index]
l_net_value =[i for i in df_net_value_group5["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()
'''
'''
r3
'''
'''
net_value=1
dic_net_value_group1_r3={}
dic_net_value_group1_r3[date_list[0]]=1
dic_subchoice_group1_r3={}
dic_mean_return_group1_r3={}

#group1 net value
for x in range(0,len(date_list)-5,5):
    date_open=date_list[x]
    df_subchoice_group1_r3=choose_group1[choose_group1["date"]==str(date_open)]
    dic_subchoice_group1_r3[date_open]=df_subchoice_group1_r3    
    n=len(df_subchoice_group1_r3)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group1_r3["r3"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+5
        date_close=date_list[y]
        dic_net_value_group1_r3[date_close]=net_value
        dic_mean_return_group1_r3[date_close]=mean_return
    
#group2 net value
net_value=1
dic_net_value_group2_r3={}
dic_net_value_group2_r3[date_list[0]]=1
dic_subchoice_group2_r3={}
dic_mean_return_group2_r3={}

for x in range(0,len(date_list)-5,5):
    date_open=date_list[x]
    df_subchoice_group2_r3=choose_group2[choose_group2["date"]==str(date_open)]
    dic_subchoice_group2_r3[date_open]=df_subchoice_group2_r3    
    n=len(df_subchoice_group2_r3)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group2_r3["r3"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+5
        date_close=date_list[y]
        dic_net_value_group2_r3[date_close]=net_value
        dic_mean_return_group2_r3[date_close]=mean_return


#group3 net value
net_value=1
dic_net_value_group3_r3={}
dic_net_value_group3_r3[date_list[0]]=1
dic_subchoice_group3_r3={}
dic_mean_return_group3_r3={}

for x in range(0,len(date_list)-5,5):
    date_open=date_list[x]
    df_subchoice_group3_r3=choose_group3[choose_group3["date"]==str(date_open)]
    dic_subchoice_group3_r3[date_open]=df_subchoice_group3_r3    
    n=len(df_subchoice_group3_r3)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group3_r3["r3"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+5
        date_close=date_list[y]
        dic_net_value_group3_r3[date_close]=net_value
        dic_mean_return_group3_r3[date_close]=mean_return

#group4 net value
net_value=1
dic_net_value_group4_r3={}
dic_net_value_group4_r3[date_list[0]]=1
dic_subchoice_group4_r3={}
dic_mean_return_group4_r3={}

for x in range(0,len(date_list)-5,5):
    date_open=date_list[x]
    df_subchoice_group4_r3=choose_group4[choose_group4["date"]==str(date_open)]
    dic_subchoice_group4_r3[date_open]=df_subchoice_group4_r3    
    n=len(df_subchoice_group3_r3)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group4_r3["r3"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+5
        date_close=date_list[y]
        dic_net_value_group4_r3[date_close]=net_value
        dic_mean_return_group4_r3[date_close]=mean_return

#group5 net value
net_value=1
dic_net_value_group5_r3={}
dic_net_value_group5_r3[date_list[0]]=1
dic_subchoice_group5_r3={}
dic_mean_return_group5_r3={}

for x in range(0,len(date_list)-5,5):
    date_open=date_list[x]
    df_subchoice_group5_r3=choose_group5[choose_group5["date"]==str(date_open)]
    dic_subchoice_group5_r3[date_open]=df_subchoice_group5_r3    
    n=len(df_subchoice_group5_r3)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group5_r3["r3"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+5
        date_close=date_list[y]
        dic_net_value_group5_r3[date_close]=net_value
        dic_mean_return_group5_r3[date_close]=mean_return


#作图
list_group_r3=[dic_net_value_group1_r3,dic_net_value_group2_r3,dic_net_value_group3_r3,dic_net_value_group4_r3,dic_net_value_group5_r3]
for x in range(0,5):
    df=pd.DataFrame([list_group_r3[x]]).T
    df.columns=["net_value"]
    l_date=[i for i in df.index]
    l_net_value =[i for i in df["net_value"].values]
    plt.figure(figsize=(12,6))
    plt.plot(l_date,l_net_value)
    plt.show()
''       
'''
 '''   
df_rtn_1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1.columns=["net_value"]
l_date=[i for i in df_net_value_group1.index]
l_net_value =[i for i in df_net_value_group1["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2.columns=["net_value"]
l_date=[i for i in df_net_value_group2.index]
l_net_value =[i for i in df_net_value_group2["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_3=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group3=pd.DataFrame([dic_net_value_group3]).T
df_net_value_group3.columns=["net_value"]
l_date=[i for i in df_net_value_group3.index]
l_net_value =[i for i in df_net_value_group3["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4.columns=["net_value"]
l_date=[i for i in df_net_value_group4.index]
l_net_value =[i for i in df_net_value_group4["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5.columns=["net_value"]
l_date=[i for i in df_net_value_group5.index]
l_net_value =[i for i in df_net_value_group5["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()
'''
'''
r2
'''
'''
#group1 net value
net_value=1
dic_net_value_group1_r2={}
dic_net_value_group1_r2[date_list[0]]=1
dic_subchoice_group1_r2={}
dic_mean_return_group1_r2={}

for x in range(0,len(date_list)-3,3):
    date_open=date_list[x]
    df_subchoice_group1_r2=choose_group1[choose_group1["date"]==str(date_open)]
    dic_subchoice_group1_r2[date_open]=df_subchoice_group1_r2    
    n=len(df_subchoice_group1_r2)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group1_r2["r2"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+3
        date_close=date_list[y]
        dic_net_value_group1_r2[date_close]=net_value
        dic_mean_return_group1_r2[date_close]=mean_return
    
#group2 net value
net_value=1
dic_net_value_group2_r2={}
dic_net_value_group2_r2[date_list[0]]=1
dic_subchoice_group2_r2={}
dic_mean_return_group2_r2={}

for x in range(0,len(date_list)-3,3):
    date_open=date_list[x]
    df_subchoice_group2_r2=choose_group2[choose_group2["date"]==str(date_open)]
    dic_subchoice_group2_r2[date_open]=df_subchoice_group2_r2    
    n=len(df_subchoice_group2_r2)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group2_r2["r2"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+3
        date_close=date_list[y]
        dic_net_value_group2_r2[date_close]=net_value
        dic_mean_return_group2_r2[date_close]=mean_return

#group3 net value
net_value=1
dic_net_value_group3_r2={}
dic_net_value_group3_r2[date_list[0]]=1
dic_subchoice_group3_r2={}
dic_mean_return_group3_r2={}

for x in range(0,len(date_list)-3,3):
    date_open=date_list[x]
    df_subchoice_group3_r2=choose_group2[choose_group3["date"]==str(date_open)]
    dic_subchoice_group3_r2[date_open]=df_subchoice_group3_r2    
    n=len(df_subchoice_group3_r2)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group3_r2["r2"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+3
        date_close=date_list[y]
        dic_net_value_group3_r2[date_close]=net_value
        dic_mean_return_group3_r2[date_close]=mean_return
   
#group4 net value
net_value=1
dic_net_value_group4_r2={}
dic_net_value_group4_r2[date_list[0]]=1
dic_subchoice_group4_r2={}
dic_mean_return_group4_r2={}

for x in range(0,len(date_list)-3,3):
    date_open=date_list[x]
    df_subchoice_group4_r2=choose_group2[choose_group4["date"]==str(date_open)]
    dic_subchoice_group4_r2[date_open]=df_subchoice_group4_r2    
    n=len(df_subchoice_group4_r2)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group4_r2["r2"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+3
        date_close=date_list[y]
        dic_net_value_group4_r2[date_close]=net_value
        dic_mean_return_group4_r2[date_close]=mean_return

#group5 net value
net_value=1
dic_net_value_group5_r2={}
dic_net_value_group5_r2[date_list[0]]=1
dic_subchoice_group5_r2={}
dic_mean_return_group5_r2={}

for x in range(0,len(date_list)-3,3):
    date_open=date_list[x]
    df_subchoice_group5_r2=choose_group2[choose_group5["date"]==str(date_open)]
    dic_subchoice_group5_r2[date_open]=df_subchoice_group5_r2    
    n=len(df_subchoice_group5_r2)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group5_r2["r2"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+3
        date_close=date_list[y]
        dic_net_value_group5_r2[date_close]=net_value
        dic_mean_return_group5_r2[date_close]=mean_return


#作图
list_group_r2=[dic_net_value_group1_r2,dic_net_value_group2_r2,dic_net_value_group3_r2,dic_net_value_group4_r2,dic_net_value_group5_r2]
for x in range(0,5):
    df=pd.DataFrame([list_group_r2[x]]).T
    df.columns=["net_value"]
    l_date=[i for i in df.index]
    l_net_value =[i for i in df["net_value"].values]
    plt.figure(figsize=(12,6))
    plt.plot(l_date,l_net_value)
    plt.show()
       
'''
'''    
df_rtn_1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1.columns=["net_value"]
l_date=[i for i in df_net_value_group1.index]
l_net_value =[i for i in df_net_value_group1["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2.columns=["net_value"]
l_date=[i for i in df_net_value_group2.index]
l_net_value =[i for i in df_net_value_group2["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_3=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group3=pd.DataFrame([dic_net_value_group3]).T
df_net_value_group3.columns=["net_value"]
l_date=[i for i in df_net_value_group3.index]
l_net_value =[i for i in df_net_value_group3["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4.columns=["net_value"]
l_date=[i for i in df_net_value_group4.index]
l_net_value =[i for i in df_net_value_group4["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5.columns=["net_value"]
l_date=[i for i in df_net_value_group5.index]
l_net_value =[i for i in df_net_value_group5["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()
'''
'''
r1
'''
'''
#group1 net value

net_value=1
dic_net_value_group1_r1={}
dic_net_value_group1_r1[date_list[0]]=1
dic_subchoice_group1_r1={}
dic_mean_return_group1_r1={}

for x in range(0,len(date_list)-2,2):
    date_open=date_list[x]
    df_subchoice_group1_r1=choose_group1[choose_group1["date"]==str(date_open)]
    dic_subchoice_group1_r1[date_open]=df_subchoice_group1_r1    
    n=len(df_subchoice_group1_r1)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group1_r1["r1"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+1
        date_close=date_list[y]
        dic_net_value_group1_r1[date_close]=net_value
        dic_mean_return_group1_r1[date_close]=mean_return
    
#group2 net value
net_value=1
dic_net_value_group2_r1={}
dic_net_value_group2_r1[date_list[0]]=1
dic_subchoice_group2_r1={}
dic_mean_return_group2_r1={}

for x in range(0,len(date_list)-2,2):
    date_open=date_list[x]
    df_subchoice_group2_r1=choose_group2[choose_group2["date"]==str(date_open)]
    dic_subchoice_group2_r1[date_open]=df_subchoice_group2_r1    
    n=len(df_subchoice_group2_r1)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group2_r1["r1"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+1
        date_close=date_list[y]
        dic_net_value_group2_r1[date_close]=net_value
        dic_mean_return_group2_r1[date_close]=mean_return

#group3 net value
net_value=1
dic_net_value_group3_r1={}
dic_net_value_group3_r1[date_list[0]]=1
dic_subchoice_group3_r1={}
dic_mean_return_group3_r1={}

for x in range(0,len(date_list)-2,2):
    date_open=date_list[x]
    df_subchoice_group3_r1=choose_group3[choose_group3["date"]==str(date_open)]
    dic_subchoice_group3_r1[date_open]=df_subchoice_group3_r1    
    n=len(df_subchoice_group3_r1)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group3_r1["r1"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+1
        date_close=date_list[y]
        dic_net_value_group3_r1[date_close]=net_value
        dic_mean_return_group3_r1[date_close]=mean_return
    
#group4 net value
net_value=1
dic_net_value_group4_r1={}
dic_net_value_group4_r1[date_list[0]]=1
dic_subchoice_group4_r1={}
dic_mean_return_group4_r1={}

for x in range(0,len(date_list)-2,2):
    date_open=date_list[x]
    df_subchoice_group4_r1=choose_group4[choose_group4["date"]==str(date_open)]
    dic_subchoice_group4_r1[date_open]=df_subchoice_group4_r1    
    n=len(df_subchoice_group4_r1)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group4_r1["r1"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+1
        date_close=date_list[y]
        dic_net_value_group4_r1[date_close]=net_value
        dic_mean_return_group4_r1[date_close]=mean_return

#group5 net value
net_value=1
dic_net_value_group5_r1={}
dic_net_value_group5_r1[date_list[0]]=1
dic_subchoice_group5_r1={}
dic_mean_return_group5_r1={}

for x in range(0,len(date_list)-2,2):
    date_open=date_list[x]
    df_subchoice_group5_r1=choose_group5[choose_group5["date"]==str(date_open)]
    dic_subchoice_group5_r1[date_open]=df_subchoice_group5_r1    
    n=len(df_subchoice_group5_r1)
    if n==0:
        pass
    else:
        mean_return=(df_subchoice_group5_r1["r1"].mean())/n
        net_value=net_value*(1+mean_return)
        y=x+1
        date_close=date_list[y]
        dic_net_value_group5_r1[date_close]=net_value
        dic_mean_return_group5_r1[date_close]=mean_return


#作图
list_group_r1=[dic_net_value_group1_r1,dic_net_value_group2_r1,dic_net_value_group3_r1,dic_net_value_group4_r1,dic_net_value_group5_r1]
for x in range(0,5):
    df=pd.DataFrame([list_group_r1[x]]).T
    df.columns=["net_value"]
    l_date=[i for i in df.index]
    l_net_value =[i for i in df["net_value"].values]
    plt.figure(figsize=(12,6))
    plt.plot(l_date,l_net_value)
    plt.show()



''' 
''' 
df_rtn_1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group1.columns=["net_value"]
l_date=[i for i in df_net_value_group1.index]
l_net_value =[i for i in df_net_value_group1["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2=pd.DataFrame([dic_net_value_group2]).T
df_net_value_group2.columns=["net_value"]
l_date=[i for i in df_net_value_group2.index]
l_net_value =[i for i in df_net_value_group2["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_3=pd.DataFrame([dic_net_value_group1]).T
df_net_value_group3=pd.DataFrame([dic_net_value_group3]).T
df_net_value_group3.columns=["net_value"]
l_date=[i for i in df_net_value_group3.index]
l_net_value =[i for i in df_net_value_group3["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4=pd.DataFrame([dic_net_value_group4]).T
df_net_value_group4.columns=["net_value"]
l_date=[i for i in df_net_value_group4.index]
l_net_value =[i for i in df_net_value_group4["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()

df_rtn_5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5=pd.DataFrame([dic_net_value_group5]).T
df_net_value_group5.columns=["net_value"]
l_date=[i for i in df_net_value_group5.index]
l_net_value =[i for i in df_net_value_group5["net_value"].values]
plt.figure(figsize=(12,6))
plt.plot(l_date,l_net_value)
plt.show()
'''

'''
label
'''

#获取前7天所有个股评论数


df_label=pd.DataFrame()

for i in range(1,8):
    date=datetime.datetime.now()+datetime.timedelta(days=-i)
    df_label_sub=adjusted_data_new[adjusted_data_new["date"]==str(date)[:10]]
    df_label=df_label.append(df_label_sub)





#按照前一天评论数量的百分位分组

date_before=datetime.datetime.now()+datetime.timedelta(days=-1)
df_label_before=df_label[df_label["date"]==str(date_before)[:10]]
df_label_before=df_label_before.sort_values(by="stock_code").reset_index(drop=True)
bins=np.percentile(df_label_before["score"],[0,30,60,100])
df_label_before["昨日评论总数"]=pd.cut(df_label_before["score"],bins,labels=["雪球昨日评论数少","雪球昨日评论数居中","雪球昨日评论多"])
df_label_before["昨日评论总数"].fillna("雪球昨日评论数少",inplace=True)


#七天累计评论数量按百分位分组

df_label_sum=pd.DataFrame(df_label.groupby(["stock_code"])["score"].sum())
bins=np.percentile(df_label_sum["score"],[0,30,60,100])
df_label_sum["一周累计评论数"]=pd.cut(df_label_sum["score"],bins,labels=["雪球一周累计评论少","雪球一周累计评论居中","雪球一周累计评论多"])
df_label_sum["一周累计评论数"].fillna("雪球一周累计评论少",inplace=True)
df_label_sum.index.name="stock_code"
df_label_sum=df_label_sum.reset_index()


#七天评论累计增长数按百分位分组
df_label_growth=pd.DataFrame()
dic_growth={}
df_label=df_label.sort_values(by="stock_code")
date_adj=df_label["date"]
date=[str(x)[:10] for x in date_adj]
df_label['date'] = date
stock_list_growth=list(df_label["stock_code"].unique())
for stock in stock_list:
    df_growth=df_label[df_label["stock_code"]==stock]
    if len(df_growth)==0:
        pass
    elif len(df_growth)==1:
        dic_growth[stock]=0
    elif len(df_growth)==2:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=df_growth_list[1]-df_growth_list[0]
        dic_growth[stock]=growth
    elif len(df_growth)==3:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=(df_growth_list[1]-df_growth_list[0])+(df_growth_list[2]-df_growth_list[1])
        dic_growth[stock]=growth       
    elif len(df_growth)==4:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=(df_growth_list[1]-df_growth_list[0])+(df_growth_list[2]-df_growth_list[1])+(df_growth_list[3]-df_growth_list[2])
        dic_growth[stock]=growth       
    elif len(df_growth)==5:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=(df_growth_list[1]-df_growth_list[0])+(df_growth_list[2]-df_growth_list[1])+(df_growth_list[3]-df_growth_list[2])+(df_growth_list[4]-df_growth_list[3])
        dic_growth[stock]=growth               
    elif len(df_growth)==6:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=(df_growth_list[1]-df_growth_list[0])+(df_growth_list[2]-df_growth_list[1])+(df_growth_list[3]-df_growth_list[2])+(df_growth_list[4]-df_growth_list[3])+(df_growth_list[5]-df_growth_list[4])
        dic_growth[stock]=growth                      
    else:
        df_growth=df_growth.sort_values(by="date").reset_index(drop=True)
        df_growth_list=list(df_growth["score"])
        growth=(df_growth_list[1]-df_growth_list[0])+(df_growth_list[2]-df_growth_list[1])+(df_growth_list[3]-df_growth_list[2])+(df_growth_list[4]-df_growth_list[3])+(df_growth_list[5]-df_growth_list[4])+(df_growth_list[6]-df_growth_list[5])
        dic_growth[stock]=growth                      
df_label_growth=pd.DataFrame([dic_growth]).T
df_label_growth.columns=["growth"]

##分组
bins=np.percentile(df_label_growth["growth"],[0,30,60,100])
df_label_growth["一周评论累计增长数"]=pd.cut(df_label_growth["growth"],bins,labels=["雪球一周累计评论增长少","雪球一周累计评论增长居中","雪球一周累计评论增长多"])
df_label_growth["一周评论累计增长数"].fillna("雪球一周累计评论增长少",inplace=True)
df_label_growth.index.name="stock_code"
df_label_growth=df_label_growth.reset_index()

#生成字典label,把标签放进list里
label={}
stock_list_comment=list(df_label["stock_code"])
for stock in stock_list_comment:    
    df_label_before_1=df_label_before[df_label_before["stock_code"]==stock] 
    if len(df_label_before_1)==0:
        label[stock]=["雪球昨日无评论"]
    else:
        value=df_label_before_1["昨日评论总数"].values[0]
        label[stock]=[value]
   
    df_label_sum_1=df_label_sum[df_label_sum["stock_code"]==stock] 
    if len(df_label_sum_1)==0:
        pass
    else:
        value=df_label_sum["一周累计评论数"].values[0]
        label[stock].append(value)
      
    df_label_growth_1=df_label_growth[df_label_growth["stock_code"]==stock]
    if len(df_label_growth_1)==0:
        pass
    else:
        value=df_label_growth["一周评论累计增长数"].values[0]
        label[stock].append(value)

#上传到redis
import redis
redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=5)
json_comment=label
json_comment_dump=json.dumps(json_comment)
json_comment_name="label"
redis_c.set(json_comment_name,json_comment_dump)   

'''
comment label http://47.100.219.4:7080/?topic=5&aris_data=label

'''

"""    
'''
output_df['Percentile_rank']=output_df["score"].rank(ascending = False, pct = True) 

output_df_sort=output_df.sort_values(by="Percentile_rank")
output_df_sort=output_df_sort.reset_index(drop=True)

''''
'''


dic_df = dict()
n = 10
i = 1

score_list = list(set(output_df_sort['score']))

length = len(output_df_sort)

# 若单元素超过df长度10% 则领出来
count_max = 0
#count_dic = dict()
c = output_df_sort['score'].tolist()
for each in score_list:
    count = c.count(each)
#    count_dic[str(each)] == count
    if count > count_max:
        count_max = count
        value_max = each
df_rest = output_df_sort.copy()

while count_max/length > 1/n and n >= 1:
    # 超出量存储
    df_temp = df_rest[df_rest['score'] == value_max]
    groupname = 'group' + str(i)
    dic_df[groupname] = df_temp
    # 剩余df
    df_rest = df_rest[df_rest['score'] != value_max]
    
    score_list = list(set(df_rest['score']))
    count_max = 0
    # 准备循环
    length = len(df_rest)
    c = df_rest['score'].tolist()
    for each in score_list:
        count = c.count(each)
#        count_dic[each] == count
        if count > count_max:
            count_max = count
            value_max = each
            
    n = n-1
    i += 1

# 开始n分组
    
def get_grop_cut(n, start):
    '''
    根据指定的n返回用于做n分组的区间组和组名
    '''
    i = 0
    label = []
    l = []
    gap_temp = 100/n
    while i < n:
        label.append('group' + str(i+start))
        l.append( gap_temp * i )
        i += 1
    l.append(100)
    return label, l

label, l = get_grop_cut(n, i)
bins=np.percentile(df_rest["score"],l)
df_rest['group'] = pd.cut(df_rest["score"], bins,labels=label,include_lowest = True)
df_check = df_rest.copy()
e = list(dic_df.keys())

for each in e:
    temp_df = dic_df[each]
    temp_df['group'] = each
    df_rest = df_rest.append(temp_df, sort=True)


#t = list(set(df_rest['group'].tolist()))
#t = [ 'group1',
# 'group2',
# 'group3',
# 'group4',
# 'group 5',
# 'group 6',
# 'group 7',
# 'group 8',
# 'group 9',
# 'group 10']


#return_mean_bygroup=[]
#for each in t:
    #temp_df = df_rest[df_rest['group'] == each]
    #temp = temp_df["r1"].mean()
    #return_mean_bygroup.append(temp)

#plt.bar(range(len(return_mean_bygroup)), return_mean_bygroup)  
#plt.show()    
   
"""














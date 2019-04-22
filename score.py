# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json
from urllib import parse,request
import datetime


    
def getdata(engine ,sql):
#    starttime = datetime.datetime.now()
    conn = engine.connect()
    try:
        getrst = pd.read_sql(sql,conn)
    except Exception as e:
        print('getdata error: {} \n engine and sql as below: \n {} \n {}'.format(repr(e),engine,sql))
    finally:
        conn.close()
#    endtime = datetime.datetime.now()
#    logger.info('......it took {} seconds to get rawdata.'.format((endtime - starttime).seconds))
    return getrst
#engine_server_news = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/news?charset=utf8')
#sql_get_news_stock_day_count = "select * from stock_day_count_xueqiu"
#temp_TotalRatio_df = getdata(engine_server_news,sql_get_news_stock_day_count)
def get_raw():
    engine_server_questions_ans = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/questions_ans?charset=utf8')
    sql_get_question_ans_xueqiu_comment = "select about,pub_time,comment_num from xueqiu_comment"
    raw_data = getdata(engine_server_questions_ans,sql_get_question_ans_xueqiu_comment)
    return raw_data

def adjust_raw(raw_data):
    #update pub_time的时间
    pub_time=raw_data["pub_time"]
    pub_time_date=[str(x)[:10] for x in pub_time]
    pub_time_filter = [str(x)[:4] for x in pub_time]
    raw_data['pub_time'] = pub_time_date
    raw_data['filter'] = pub_time_filter
    
    # 删除
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
    
    # groupby pub_time
    adjusted_data=adjusted_data[["about","comment_num","pub_time"]]
    
    adjusted_data_new=adjusted_data.groupby(["pub_time","about"])["comment_num"].sum()
    adjusted_data_new = adjusted_data_new.reset_index()
    
    #comment_num+1
    comment_num=adjusted_data_new["comment_num"]+1
    adjusted_data_new["comment_num"]=comment_num
    
    adjusted_data_new["weight"]=1
    adjusted_data_new["score"]=adjusted_data_new["weight"]*adjusted_data_new["comment_num"]
    
    adjusted_data_new.columns=["date","stock_code","comment_num","weight","score"]
    temp_l = adjusted_data_new['date']
    temp_l = [datetime.datetime.strptime(x, "%Y-%m-%d") for x in temp_l]
    adjusted_data_new['date'] = temp_l
    
    return adjusted_data_new[["date","stock_code","score"]]



# 
raw_data = get_raw()
# 
adjusted_data_new = adjust_raw(raw_data)


# 

#
def get_stock_val(stock_l):
    '''
    get total stock vlue
    '''
    d_stock_price={}
    header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}

    def get_stock_price(code):
        """
        """
    
        if code in d_stock_price:
            return d_stock_price[code]
        
        if code[0]=='6':
            mkt='sh'
        else:
            mkt='sz'
        
        url="http://nujump.tigerobo.com/HB_Jumper/pd.ashx?id=%s|%s&type=k&rtype=1&style=top&num=526&js=(x)&at=fa"%(code, mkt)
        
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
        df['code']=code
        return_l = []
        for each in range(len(df)):
            if temp_l==0:
                return_temp=np.nan
            else:
                temp_l = list(df.iloc[each])
                return_temp = temp_l[2] / temp_l[1] - 1

            return_l.append(return_temp)
        df['return'] = return_l
        d_stock_price[code]=df
        
        return d_stock_price[code]
    count = 0 
    for each in stock_l:
        rst = get_stock_price(each)
        d_stock_price[each] = rst
        count += 1
        if count % 10 == 0:A
            print(count)
    
    return d_stock_price

from matplotlib import pyplot as plt 

#def get_scatter(df):
    column = df.columns.tolist()
    
    #绘制散点图
    plt.scatter(df[column[0]],df[column[1]],color = 'b',label = "Exam Data")
     
    #添加图的标签（x轴，y轴）
    plt.xlabel(column[0])
    plt.ylabel(column[1])
    #显示图像
    plt.show()
    return


df = adjusted_data_new  # total数据

# 在原df上增加收益率
# 建立不重复的stock列表
stockl = list(set(df['stock_code']))
# 根据stock——code获取收益率dic（包含df，-1）
d_stock_price = get_stock_val(stockl)
# 建立用于合并收益率和score的df用于分析
df_merge = pd.DataFrame()
#根据每个股票合并
for each_stock in stockl:
    # each comment df
    df_temp = df[df['stock_code'] == each_stock]
    # each return df
    df_return = d_stock_price[each_stock]# ==-1
    if type(df_return)!= int:
        df_return.rename(columns={'code':'stock_code'},inplace=True)
        # merge
        rst = pd.merge(df_temp, df_return[['stock_code','date','return']], how = 'left', on = ['stock_code','date'])
        # append在输出df上
        df_merge = df_merge.append(rst, sort = True)
   

# df分组



''' 回归 ''' 
import matplotlib.pyplot as plt

df_merge_1=df_merge.dropna(subset=["return"])


df=df_return.copy()
df.set_index(['date','stock_code'],drop = True).shift(1)
#
#
#
#
#
df1 = df_merge_1.copy()[df_merge["score"]==1]
df2 = df_merge.copy()[df_merge["score"]==2]
df3 = df_merge.copy()[df_merge["score"]==3]
df4 = df_merge.copy()[df_merge["score"]==4]

df1

dfi = df_merge.copy()[(df_merge["score"]!=1)&(df_merge["score"]!=2)&(df_merge["score"]!=3)&(df_merge["score"]!=4)]

l = []
x = 6
i = 0
temp = 100/x
while i < (100-x+1):
    l.append(i)
    i += temp
l.append(100)
    
    
bins=np.percentile(dfi["score"],l)
dfi["percentage"]=pd.cut(dfi["score"],bins,include_lowest = True)

return_mean_bygroup=[]

a=df1["return"].mean()
b=df2["return"].mean()
c=df3["return"].mean()
d=df4["return"].mean()
e=dfi.groupby(["percentage"]).mean()["return"].values

return_mean_bygroup.append(a)
return_mean_bygroup.append(b)
return_mean_bygroup.append(c)
return_mean_bygroup.append(d)
for x in e:
    return_mean_bygroup.append(x)

plt.bar(range(len(return_mean_bygroup)), return_mean_bygroup)  
plt.show()  



dfi_1=dfi.copy()[dfi["score"]>30]
del dfi_1["percentage"]

bins_1=np.percentile(dfi_1["score"],[0,20,40,60,80,100])
dfi_1["percentage"]=pd.cut(dfi_1["score"],bins_1,include_lowest = True)

f=dfi_1.groupby(["percentage"]).mean()["return"].values

return_mean_bygroup_last=[]
for x in e:
    return_mean_bygroup_last.append(x)

plt.bar(range(len(return_mean_bygroup_last)), return_mean_bygroup_last)  
plt.show()  






a=df_merge["score"].values
a.max()
b=df_merge["return"].tolist()

plt.plot(a,b,'bo')
plt.show()


plt.hist(a,bins=10,range = (0,100))
plt.show()

import statsmodels.api as sm
import statsmodels.formula.api as smf
data=df_merge[["score","return"]]
results = smf.ols('y ~ x', data=data).fit()
print(results.summary())
#
#
#
#
#
#
#
'''
1 取数据
'''
dict = ?
df = ?


'''
2 for循环 根据stock_code 匹配 生成df 计算收益率
'''
stock_list = ??
output_df = pd.DataFrame()
for each_stock in stock_list:
    # get data
    news_df = ?
    return_df = ?
    # 计算多日收益率
    final_return_df = get_multiple_return(return_df)
    # 根据规则merge
    merge_df = get_merge(news_df, final_return_df)
    output_df = output_df.append(merge_df)

'''
3 十分组查看收益率
'''
# N分组
df = input()
def get_group_mark(dff,n, start = 0):
    '''
    根据df，n分组返回分组情况
    n:      分组数
    start:  group名字开始数
    '''
    def get_grop_cut(n, start):
        '''
        根据指定的n返回用于做n分组的区间组和组名
        '''
        i = 0
        label = []
        l = []
        gap_temp = 100/n
        while i < n:
            label.append('group ' + str(i+start+1))
            l.append( gap_temp * i )
            i += 1
        l.append(100)
        return label, l
    
    check_start = start
    if len(dff) < n:
        print('df 长度不足{}，请调整参数'.format(n))
        return -1
    df = dff.copy()
    # prepare    
    label, l = get_grop_cut(10, check_start)    
    # 分组mark
    bins=np.percentile(df["score"],l)
    
    # 单个数据频次特高则归为单组
    if len(bins) > len(set(bins)):
        # 提取高频数据
        duplicate = []
        for x in bins:
            if list(bins).count(x) > 1:
                duplicate.append(x)
        duplicate = list(set(duplicate))
        duplicate.sort()
        print('超量数据值:{},删除{}'.format(duplicate, duplicate[0]))
        # 删除找到的第一个超量数据
        each = duplicate[0]
        bins_1=bins.tolist()
        index = bins_1.index(each)
        count = bins_1.count(each)
        
        if index == 0:
            # 头部重复
            group_name = label[index]
            df_temp = df[df['score'] > each]
            df_append = get_group_mark(df_temp, n-1, start = check_start + 1)
            df_reserve = df[df['score'] == each]
            df_reserve['group'] = group_name
            df = df_reserve.append(df_append)
            
        elif index + count == len(bins):
            # 尾部重复
            group_name = label[-1]
            df_temp = df[df['score'] < each]
            df_append = get_group_mark(df_temp, n-1, start = check_start)
            df_reserve = df[df['score'] == each]
            df_reserve['group'] = group_name
            df = df_reserve.append(df_append)

        else:
            # 中间重复
            index = index + count // 2
            # 前
            count_1 = index
            df_temp_1 = df[df['score'] < each]
            df_append_1 = get_group_mark(df_temp_1, count_1 , start = check_start)
            
            # 后
            count_2 = n - count_1 - 1
            df_temp_2 = df[df['score'] > each]
            df_append_2 = get_group_mark(df_temp_2, count_2 , start = check_start + index + 1)
            
            # 中
            df_reserve = df[df['score'] == each]
            group_name = label[index + 1]
            df_reserve['group'] = group_name
            
            df = df_reserve.append(df_append_1)
            df = df.append(df_append_2)
                    
    else:
        # 根据bins切组
        df['group'] = pd.cut(df["score"], bins,labels=label,include_lowest = True)
    
    return df




















# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 16:14:08 2019

@author: P7XXTMX-G
"""

import json
from urllib import parse,request
import datetime
import numpy as np
import pandas as pd

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
            temp_l = list(df.iloc[each])
            return_temp = temp_l[2] / temp_l[1] - 1
            return_l.append(return_temp)
        df['return'] = return_l
        d_stock_price[code]=df
        
        return d_stock_price[code]
    
    for each in stock_l:
        rst = get_stock_price(each)
        d_stock_price[each] = rst
    
    return d_stock_price

from matplotlib import pyplot as plt 

def get_scatter(df):
    column = df.columns.tolist()
    
    #绘制散点图
    plt.scatter(df[column[0]],df[column[1]],color = 'b',label = "Exam Data")
     
    #添加图的标签（x轴，y轴）
    plt.xlabel(column[0])
    plt.ylabel(column[1])
    #显示图像
    plt.show()
    return


df = input()  # total数据
# 在原df上增加收益率
stockl = list(set(df['stock_code']))
d_stock_price = get_stock_val(stockl)
df_merge = pd.DataFrame()
for each_stock in stockl:
    df_temp = df[df['stock_code'] == each_stock]
    df_return = d_stock_price[each_stock]
    rst = pd.merge(df_temp, df_return[['stock_code','return']], how = 'left', on = ['stock_code','date'])
    df_merge = df_merge.append(rst, sort = True)

# df分组



''' 回归 ''' 













































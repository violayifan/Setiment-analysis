# -*- coding: utf-8 -*-
"""
Created on Thu Jul  4 09:58:24 2019

@author: viola
"""

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import json
from urllib import parse,request
import datetime
import logging
import re
import math
import requests
import redis

logger = logging.getLogger('yuqing_label_updated')  
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('yuqing_label_updated.log')  
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  
fh.setFormatter(formatter)  
ch.setFormatter(formatter)  
logger.addHandler(fh)  
logger.addHandler(ch)  


def getdata(engine ,sql):
    
    """
    用于连接数据库，报告数据提取时间以及是否出现连接数据库错误
    
    参数说明：
        engine：登录数据库地址,用户名,密码信息
        sql：sql数据库子表位置和选取字表的列名
        
    Raises：
        数据库连接错误
    """  
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

def get_raw(sql):
    """
    用于提取原始数据
    
    参数说明：
        sql：sql数据库子表位置和选取字表的列名
    
    返回:
        返回结果为一个dataframe
    """
    engine_server = create_engine('mysql+pymysql://zhuzhizhong:Intern@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/news?charset=utf8')
    raw_data = getdata(engine_server,sql)
    return raw_data

class yuqing_raw_data_getter(object):  
    
    """
    用于提取一段间隔时间区间的三个不同来源的数据(雪球，微博和新闻)
    
    参数说明：
        date1：开始时间点
        date2：结束时间点
        
    返回值：
        返回结果一个dataframe
        包含三列：stock_id(股票代码),p_date(发布日期),count(数量)    
    """
   
    def xueqiu(self,date1,date2):
        '''
        提取雪球评论数量
        '''
        sql_get_xueqiu= "select stock_id, p_date, count from stock_day_count_xueqiu WHERE p_date>='{}'and p_date<='{}' ORDER BY p_date ASC;".format(date1,date2)
        df_xueqiu=get_raw(sql_get_xueqiu)    
        return df_xueqiu

    def weibo(self,date1,date2):
        """
        提取微博评论数量
        """
        sql_get_weibo= "select stock_id, p_date, count from stock_day_count_weibo WHERE p_date>='{}'and p_date<='{}' ORDER BY p_date ASC;".format(date1,date2)
        df_weibo=get_raw(sql_get_weibo)
        return df_weibo
    
    def news(self,date1,date2):
        """
        提取新闻数量
        """
        sql_get_news= "select stock_id, p_date, count from stock_day_count_news  WHERE p_date>='{}'and p_date<='{}' ORDER BY p_date ASC;".format(date1,date2)
        df_news=get_raw(sql_get_news)
        df_news["stock_id"]=[i[:6] for i in df_news["stock_id"]]
        df_news=df_news.copy()
        return df_news

class stockGetter(object):
    
    """
    用于获取股票代码-名称信息
    
    return df_stock_name 
    包含三列：stock_id(股票代码,有后缀.SH/.SZ),stock_code(股票代码,无后缀.SH/.SZ),stock_name(股票中文简称)
    """
    
    def get_stock_name(self):
        
        header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}
        
        url_getname = 'https://stkcode.tigerobo.com/HB_IDCenterWeb/JS.ashx?type=bk&cmd=901&rtntype=1&' \
                    'tkn=3e715abf133fa24da68e663c5ab98857'
        req = request.Request(url=url_getname, headers=header_dict)
        res = request.urlopen(req)
        res = res.read()
        r = json.loads(res.decode(encoding='utf-8'))
        df_stock_name=pd.DataFrame()
        for data in r:
            if data["ShowMkt"] == "SZ" or data["ShowMkt"] == "SH":
                res_dict = dict()
                stockName = data["Name"]
                stockName = re.sub("\s*", '', stockName)
                res_dict["stock_id"] =data["Code"]+"."+data["ShowMkt"]
                res_dict["stock_code"]=data["Code"]
                res_dict["stock_name"]=data["Name"]
                df_temp=pd.DataFrame([res_dict])
                df_stock_name=df_stock_name.append(df_temp)
        
        df_stock_name=df_stock_name.reset_index(drop=True)

        return df_stock_name 

class yuqing_calculate_getter(object):
        
   '''
   用于计算舆情指数，不同维度的舆情指数变化量和变化率，以及定义舆情正面和负面标签的标准值
   '''
   def get_yuqing_score(self,date1,date2):
        """
        用于计算一段时间(一周)的舆情指数平均值
        
        参数说明：
            date1：开始时间点
            date2：结束时间点
        
        返回值：
            返回df_yuqing_score
            包含五列：stock_code(股票代码),weibo(微博舆情分数),xueqiu(雪球舆情分数),news(新闻舆情分数),total(舆情总分)      
        """                  
        
        def yuqing_index(t,a):
            
            """
            算舆情指数的公式
            
            参数说明：
                a：设定的初始参数(xueqiu_para_a,weibo_para_a,news_para_a)
                t：输入的数值(例：雪球评论数,新闻数，微博数)，在输出转化为相应舆情分数
                
            返回值：
                舆情分数
            """
            y=(2/(1+math.pow(a,-t))-1)*100            
            return y

        #舆情指数参数para_a(一年调整)    
        xueqiu_para_a=1.63
        weibo_para_a=1.43
        news_para_a=1.05
    
        getter_raw_data = yuqing_raw_data_getter()
        #获取雪球评论数量
        df_xueqiu = getter_raw_data.xueqiu(date1,date2)
        #获取微博评论数量
        df_weibo=getter_raw_data.weibo(date1,date2)
        #获取新闻数量
        df_news=getter_raw_data.news(date1,date2)
    
        # 计算三个不同维度的舆情分数(微博，新闻和雪球)
        df_weibo["weibo_score"]=df_weibo["count"].transform(lambda x:yuqing_index(x,weibo_para_a))
        df_news["news_score"]=df_news["count"].transform(lambda x:yuqing_index(x,news_para_a))
        df_xueqiu["xueqiu_score"]=df_xueqiu["count"].transform(lambda x:yuqing_index(x,xueqiu_para_a))
        
        #合并三个维度的数据，为计算总舆情指数做数据准备，对缺失值用零填充
        df_combine=pd.DataFrame()
        df_combine=pd.merge(df_weibo,df_xueqiu,on=["stock_id","p_date"],how="outer")
        df_combine=pd.merge(df_combine,df_news,on=["stock_id","p_date"],how="outer")
        df_combine["xueqiu_score"].fillna(yuqing_index(0,xueqiu_para_a),inplace=True)
        df_combine["weibo_score"].fillna(yuqing_index(0,weibo_para_a),inplace=True)
        df_combine["news_score"].fillna(yuqing_index(0,news_para_a),inplace=True)        
        df_combine["count_x"].fillna(0,inplace=True)
        df_combine["count_y"].fillna(0,inplace=True)
        df_combine["count"].fillna(0,inplace=True)
        df_yuqing=df_combine
        
        # 计算总舆情指数，三个维度的分数加权 total_score=0.2*yuqin(weibo)+0.6*yuqin(xueqiu)+0.2*yuqin(news)
        df_yuqing["total"]=0.2*df_yuqing["weibo_score"]+0.6*df_yuqing["xueqiu_score"]+0.2*df_yuqing["news_score"]
            
        df_yuqing_score=pd.DataFrame()
        
        Getter_stock=stockGetter()
        df_stock_name=Getter_stock.get_stock_name()
        
        # 处理个别股票缺失原始数据的特殊情况,赋值为0
        for stock in df_stock_name["stock_code"].tolist():
            df_temp=df_yuqing[df_yuqing["stock_id"]==stock]
            if df_temp.empty==True:
                total=0
                weibo=0
                xueqiu=0
                news=0
        # 计算个股的不同维度的舆情分数
            else:
                total=df_temp["total"].mean()
                weibo=df_temp["count_x"].sum()
                xueqiu=df_temp["count_y"].sum()
                news=df_temp["count"].sum()
              
            dic_yuqing_score={}
            dic_yuqing_score["stock_code"]=stock
            dic_yuqing_score["total"]=total
            dic_yuqing_score["weibo"]=weibo
            dic_yuqing_score["xueqiu"]=xueqiu
            dic_yuqing_score["news"]=news
    
            df_temp_1=pd.DataFrame([dic_yuqing_score])        
            df_yuqing_score=df_yuqing_score.append(df_temp_1)
        
        return df_yuqing_score
    
 
   def get_change_caculation(self,df1,df2,typ):

       """
       用于分别计算不同维度的两个不同时间区间的舆情变化量和变化率
       
       参数说明：
           df1：前一个时间维度的全部舆情信息
           df2：后一个时间维度的全部舆情信息
           typ：控制计算不同维度的舆情值(weibo,xueqiu,news,total)
       
       返回值：
           df_yuqing_change
           包含四列：stock_code(股票代码),change(变化值),change_ratio(变化率),rank(针对变化值的排名百分比数)

       """
       Getter_stock=stockGetter()
       df_stock_name=Getter_stock.get_stock_name()
      
       df_yuqing_change=pd.DataFrame()
       
       for stock in df_stock_name["stock_code"].tolist():
    
            df_temp_1=df1[df1["stock_code"]==stock]
            df_temp_2=df2[df2["stock_code"]==stock]
            
            dic_yuqing={}
            change=df_temp_2[typ].values[0]-df_temp_1[typ].values[0]
            if df_temp_1[typ].values[0]==0:
               change_ratio="/"
            else:   
                 change_ratio=(df_temp_2[typ].values[0])/(df_temp_1[typ].values[0])-1
                 
            dic_yuqing["stock_code"]=stock
            dic_yuqing["{}_change".format(typ)]=change
            dic_yuqing["{}_change_ratio".format(typ)]=change_ratio
            df_temp=pd.DataFrame([dic_yuqing])
            df_yuqing_change=df_yuqing_change.append([df_temp])

       
       df_yuqing_change["rank"]=df_yuqing_change["{}_change".format(typ)].rank(pct=True)       
    
       return df_yuqing_change
    
   
   def get_yuqing_label(self,df_change,typ,threshold):
       
       """
       用于计算定义舆情正面和负面标签的标准值
       
       参数说明：
           df_change：包含一个舆情维度的变化量变化率数据的df(函数get_change_caculation返回的结果)
           typ：控制计算不同维度的定义舆情正面和负面标签的标准值(weibo,xueqiu,news,total)
           threshold：控制上限和下限的百分比数
                     上限：>=上限百分位数，打正面标签
                     下限：<=下限百分位数，打负面标签       
       返回值：
           value：返回正面/负面标签的标准值                 
       """
        
       df_change=df_change[df_change["{}_change".format(typ)]!=0]    
       
       if threshold>=0.5:
          value=df_change[df_change["rank"]>=threshold].sort_values(by="{}_change".format(typ))["{}_change".format(typ)].values[0]

       if threshold<0.5:
          value=df_change[df_change["rank"]<threshold].sort_values(by="{}_change".format(typ),ascending=False)["{}_change".format(typ)].values[0]
       
       return value


class stock_label(object):
    
    """
    根据舆情分数，给每个个股赋特定属性，打标签，生成标签的详细说明
    
    stock对象属性：
        stock_code：股票代码
        change：股票舆情分数变化值
        df_change：用于储存提取的股票在一个维度上舆情变化的数据(包含变化量,变化率,变化量的百分位数)
        sign：股票标签的类别(positive正面标签,negative负面标签,neutral中立不打标签)
    
    """
    
    def __init__(self,stock_code,df_change,typ):
        
        """
        用于给创建类的实例，给个股赋予初始属性
        
        参数说明：
            stock_code：股票代码
            df_change：包含一个舆情维度的变化量变化率数据的df(函数get_change_caculation返回的结果)
            typ：控制输入不同维度的舆情数据(weibo,xueqiu,news,total)
        """
        
        self.stock_code=stock_code
        self.change=df_change[df_change["stock_code"]==stock_code]["{}_change".format(typ)].values[0]
        self.df_change=df_change[df_change["stock_code"]==stock_code]
    
    def stock_label_check(self,typ,value_up,value_down):
        
        """
        根据个股的舆情变化数值，给股票赋予sign的属性
        大于等于变化上限值的为positive
        小于等于变化下限值的为negative
        处于中位的为neutral
        
        参数说明：
            typ：控制输入不同维度的舆情数据(weibo,xueqiu,news,total)
            value_up：上限值 (函数get_yuqing_label的返回值)
            value_down：下限值(函数get_yuqing_label的返回值)   
        """
        
        
        if self.change>=value_up:
            
            self.sign="positive"
        
        elif self.change<=value_down:
            
            self.sign="negative"
        else:
            self.sign="neutral"
        
    def stock_label_generate(self,typ): 
        """
        用于生成包含标签的名称,说明,detail和redis_code的字典
        
        参数说明：
            typ：控制输入不同维度的舆情数据(weibo,xueqiu,news,total)
            
        返回值：
            df_label
            包含五列：stock_code(股票代码),label(包含标签信息的字典),change(变化值),change_ratio(变化率),rank(针对变化值的排名百分比数)
        """
        
        
        df_label=self.df_change
                
        def detail_text(typ,df_label):
            name_dic = {"weibo":"微博数","xueqiu":"雪球评论数","news":"新闻数","total":"舆情指数"}
            unit_dic = {"weibo":"条","xueqiu":"条","news":"条","total":"分"}
            
            if float(df_label["{}_change".format(typ)].values[0])>0 and df_label["{}_change_ratio".format(typ)].values[0]!="/":
                
                text="{}上升{}{},上升幅度{:.2%}".format(name_dic[typ],int(df_label["{}_change".format(typ)].values[0]),unit_dic[typ],df_label["{}_change_ratio".format(typ)].values[0])
        
            if float(df_label["{}_change".format(typ)].values[0])<0 and df_label["{}_change_ratio".format(typ)].values[0]!="/":
                text="{}下降{}{},下降幅度{:.2%}".format(name_dic[typ],abs(int(df_label["{}_change".format(typ)].values[0])),unit_dic[typ],abs(df_label["{}_change_ratio".format(typ)].values[0]))

            if float(df_label["{}_change".format(typ)].values[0])>0 and df_label["{}_change_ratio".format(typ)].values[0]=="/":
                text="{}上升{}{}".format(name_dic[typ],int(df_label["{}_change".format(typ)].values[0]),unit_dic[typ])
            
            if float(df_label["{}_change".format(typ)].values[0])<0 and df_label["{}_change_ratio".format(typ)].values[0]=="/":
                text="{}下降{}{}".format(name_dic[typ],abs(int(df_label["{}_change".format(typ)].values[0])),unit_dic[typ])
            return text

        dic_label_name={"weibo":"微博关注度","xueqiu":"雪球关注度","news":"新闻关注度","total":"关注度"}
        dic_label={}
        text=detail_text(typ,df_label)
        
        if self.sign=="positive":           
            text_type="大幅上升"            
        if self.sign=="negative":            
            text_type="大幅下降"
            
        dic_label["label"]=dic_label_name[typ]+text_type
        dic_label["desText"]=text
        dic_label["detail"]=[{"change":df_label["{}_change".format(typ)].values[0],"change_ratio":df_label["{}_change_ratio".format(typ)].values[0]}]
        dic_label["redis_code"]="yuqing_{}_label".format(typ)
        redis_name="tg_self_yuqing_{}_{}".format(typ,self.sign)
        df_label[redis_name]=[dic_label]
        
        return df_label


        
def main(): 
    """
    提取数据并生成不同维度的舆情数据对应的正负面标签
    
    返回值：
        dic_label_store
        字典的key：weibo,xueqiu,news,total(对应四个维度的舆情数据)
        字典的value：包含正负面标签结果的label_dict
                    label_dict: key:两个维度(positive&negative),value:包含股票和对应标签信息的stock_dict                    stock_dict: key：stocl_id,value：标签信息(个股对应的上传数据的dict)
    """
    
    #设置时间间隔
    date1 = datetime.date.today()+datetime.timedelta(days=-14)
    date2 = datetime.date.today()+datetime.timedelta(days=-7)
    date3 = datetime.date.today()+datetime.timedelta(days=-1)

    #分别提取前两周和前一周的不同维度的舆情分数汇总
    yuqing_calculate=yuqing_calculate_getter()
    df_yuqing_score_week1= yuqing_calculate.get_yuqing_score(date1,date2)
    df_yuqing_score_week2= yuqing_calculate.get_yuqing_score(date2,date3)

    #获取stock_code(无后缀.SH/.SZ)和stock_id(有后缀.SH/.SZ)对应的字典
    Getter_stock=stockGetter()
    df_stock_name=Getter_stock.get_stock_name()[["stock_code","stock_id"]].set_index("stock_code")
    dic_stock_name=df_stock_name.T.to_dict()
    
    #创建四个不同维度type数据的列表
    list_tpye=["weibo","xueqiu","news","total"]
    
    #创建用于储存四个不同维度数据的结果
    dic_label_store={}
    for typ in list_tpye:
        
        logger.info("processing {} data".format(typ))
        
        #获取单个维度的变化数据
        df_change=yuqing_calculate.get_change_caculation(df_yuqing_score_week1,df_yuqing_score_week2,typ)
        #计算单个维度打正负标签的标准值；0.9为90%分位数值,0.1为10%分位数值
        #即变化值大于等于90%分位数值的股票打正面标签，变化值小于等于10%分位数值的股票打负面标签
        value_up=yuqing_calculate.get_yuqing_label(df_change,typ,0.9)
        value_down=yuqing_calculate.get_yuqing_label(df_change,typ,0.1)
        
                
        df_label_each=pd.DataFrame()
        count=0
        #根据全部股票列表循环
        for stock in list(dic_stock_name.keys()):
            count=count+1
            logger.info("processing stock {}/{}".format(count,len(dic_stock_name)))
            
            #创建股票实例
            stock_temp=stock_label(stock,df_change,typ)
            #根据股票的舆情变化值,赋予属性(positive/negative/neutral)
            stock_temp.stock_label_check(typ,value_up,value_down)
            
            #如果股票是positive/negative,生成正负面标签
            if stock_temp.sign=="positive" or stock_temp.sign=="negative":
                column_name="tg_self_yuqing_{}_{}".format(typ,stock_temp.sign)
                df_each=stock_temp.stock_label_generate(typ)[["stock_code",column_name]]
                stock_id=dic_stock_name[stock]["stock_id"]
                df_each['stock_id'] = [stock_id]
                df_each=df_each.drop(["stock_code"],axis=1) 
                df_each=df_each.set_index("stock_id")
                df_label_each=df_label_each.append(df_each)
                        
        dic_label=df_label_each.to_dict()
        #对生成的两个标签(正面/负面)循环,去除空值
        for label in list(dic_label.keys()):          
            label_sub=dic_label[label]
            for stock in list(label_sub.keys()):
                if label_sub[stock] is np.nan:
                    del label_sub[stock]
        
        dic_label_store[typ]=dic_label
        
    return dic_label_store
    

def set_redis(dic_label_store):
    """
    对四个维度的舆情数据分别上传redis
    
    参数说明：
        dic_label_store：包含四个维度的正负面标签数据(main函数的返回值)
    """
    
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=100)

    #对四个维度的舆情数据标签循环，上传标签    
    for key in list(dic_label_store.keys()):
        #对每个维度下的两类标签循环(正负面)
        for label in list(dic_label_store[key].keys()):            
            redis_name=label
            logger.info("processing redis {}".format(redis_name))
            #获取上一期上传的有该类标签的股票列表
            origional_l = list(redis_c.hgetall(redis_name).keys())
            #当期需要上传的该类标签的股票列表
            stock_list=list(dic_label_store[key][label].keys())
            
            count=0           
            #生成有标签的每个个股的redis
            for stock in stock_list:
                stock_label=dic_label_store[key][label][stock]
                json_dump=json.dumps(stock_label)
                redis_c.hset(redis_name,stock,json_dump)
                count=count+1
                logger.info("processing redis {},{}/{}".format(redis_name,count,len(stock_list)))
           
            #应该删除该类标签的股票列表
            del_l = list(set(origional_l) - set(stock_list))
            if len (del_l)==0:
                logger.info("no stock tags need to be delected")
            else:
                logger.info("processing delect redis{}:total_number {}".format(redis_name,len(del_l)))
                for del_element in del_l:
                    #删除标签
                    redis_c.hdel(redis_name,del_element)
                    logger.info("processing delect redis{}:{}".format(redis_name,del_element))

def set_redis_hot_ranking():
    
    """
    分别生成按舆情总分和舆情变化率排序的股票舆情数据并上传redis
    """
    
    logger.info("processing set_redis_hot_ranking")
    
    #设置时间间隔    
    date1 = datetime.date.today()+datetime.timedelta(days=-14)
    date2 = datetime.date.today()+datetime.timedelta(days=-7)
    date3 = datetime.date.today()+datetime.timedelta(days=-1)
    
    #获取股票列表
    Getter_stock=stockGetter()
    df_stock_name=Getter_stock.get_stock_name()
   
    #提取舆情数据
    yuqing_calculate=yuqing_calculate_getter()
    df_yuqing_score_week1= yuqing_calculate.get_yuqing_score(date1,date2)
    df_yuqing_score_week2= yuqing_calculate.get_yuqing_score(date2,date3)
   
    #根据近一周舆情总分列表,排序,换列名(和前端配置名匹配)并加股票中文简称
    df_yuqing_score_week2_1= df_yuqing_score_week2.sort_values(by="total",ascending=False)    
    df_yuqing_score_week2_1.rename(columns = {"total":"score","weibo":"weibo_count","xueqiu":"xueqiu_count","news":"news_count"},inplace = True)
    df_yuqing_score_week2_1=pd.merge(df_yuqing_score_week2_1,df_stock_name)
    df_yuqing_score_week2_1.drop(["stock_code"],axis=1,inplace=True)
    df_yuqing_score_week2_1=df_yuqing_score_week2_1[["stock_id","stock_name","score","weibo_count","xueqiu_count","news_count"]]

    #提取舆情变化列表,排序,换列名(和前端配置名匹配)并加股票中文简称
    df_yuqing_change=yuqing_calculate.get_change_caculation(df_yuqing_score_week1,df_yuqing_score_week2,typ="total")    
    df_yuqing_change.rename(columns = {"total_change":"score_change","total_change_ratio":"score_change_ratio"},inplace = True)
    df_yuqing_change=df_yuqing_change[df_yuqing_change["score_change_ratio"]!="/"]
    df_yuqing_change=df_yuqing_change.sort_values(by="score_change_ratio",ascending=False)
    df_yuqing_change=pd.merge(df_yuqing_change,df_stock_name)
    df_yuqing_change.drop(["stock_code"],axis=1,inplace=True)
    df_yuqing_change=df_yuqing_change[["stock_id","stock_name","score_change_ratio","score_change"]]
        
    #分别上传redis
    redis_c = redis.Redis(host="r-uf6d06633ca14714.redis.rds.aliyuncs.com", port=6379, password="guvfCZCLgiL6",  decode_responses=True, db=100)
    logger.info("processing redis hot_stock_ranking")
    redis_c.set("hot_stock_ranking",df_yuqing_score_week2_1.to_json(orient='table'))
    
    logger.info("processing redis hot_stock_change_ranking")
    redis_c.set("hot_stock_change_ranking",df_yuqing_change.to_json(orient='table'))


              
    
dic_label_store=main()
set_redis(dic_label_store)
set_redis_hot_ranking()       
                





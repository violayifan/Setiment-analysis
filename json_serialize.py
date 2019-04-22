# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 18:16:17 2019

@author: 72669
"""
import json
from pypinyin import lazy_pinyin
import pandas as pd
def div(x,y):
    if x % y !=0:
        return x/y
    else:
        return x//y

def market(x):
    if x[0] == '6':
        return x + '.SH'
    else:
        return x + '.SZ'
    

def df2json1bar(df):
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit='',decimal=2,series=df.iloc[:,0].tolist(),calc = 0):
        #atom specification
        def atom_sp(x):
            assert type(x)==list,"Input data must be of type list"
            out = []
            for d in x:
                out.append({"dv":"{}{}".format(round(div(d,10**calc),decimal),unit), "value":d})
            return out
        data = atom_sp(series)
        fseries = []
        fseries.append({"name":legend,"data":data,"type":"bar"})
        json = {"outer_title":{"text":title},
                "legend":{"data":legend},
                "yAxis": {"type":y},
                "xAxis": {"data":x},
                "unit":unit,
                "decimal":decimal,
                "series":fseries
                }
        return json
    return specification

def df2jsontext(df):
    '''
    1条数据的df
    '''
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),units={},decimals={},calcs={},axiscord={}):
        c = df.columns.tolist()
        value = df[c[0]].tolist()[0]
        json = {'title':title, 'content': value}
        return json
    return specification

def df2jsonradar(df):
    '''
    index = x轴
    radar_x = 雷达图边界
    '''
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit='',decimal=2,series=df.iloc[:,0].tolist(),calc = 0, radar_max = 1):
        #atom specification
#        def atom_sp(x):
#            assert type(x)==list,"Input data must be of type list"
#            out = []
#            for d in x:
#                out.append({"dv":"{}{}".format(round(div(d,10**calc),decimal),unit), "value":d})
#            return out
        def indicator(x_list):
            assert type(x_list)==list,"Input data must be of type list"
            out = []
            for d in x_list:
                out.append({ 'name': d , 'max': radar_max})
            return out
        def radar_data(df,legend):
            data = []
            for each in legend:
                series = df[each].tolist()
                series = [round(d,decimal) for d in series]
                data.append({'value':series, 'name':each})
            return data
        data = radar_data(df,legend)
        fseries = []
        fseries.append({"name":legend,"data":data,"type":"radar"})
        json = {"outer_title":{"text":title},
                "legend":{"data":legend,'left':'10%'},
#                "yAxis": {"type":y},
#                "xAxis": {"data":x},
                "radar": {"indicator":indicator(x)},
#                "unit":unit,
#                "decimal":decimal,
                "series":fseries
                }
        return json
    return specification


def df2json1line(df):
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit='',decimal=2,series=df.iloc[:,0].tolist(),calc = 0):
        #atom specification
        def atom_sp(x):
            assert type(x)==list,"Input data must be of type list"
            out = []
            for d in x:
                try:
                    out.append({"dv":"{}{}".format(round(div(d,10**calc),decimal),unit), "value":d})
                except:
                    out.append({"dv":"N/A"})
            return out
        data = atom_sp(series)
        fseries = []
        fseries.append({"name":legend,"data":data,"type":"line"})
        json = {"outer_title":{"text":title},
                "legend":{"data":legend},
                "yAxis": {"type":y},
                "xAxis": {"data":x},
                "unit":unit,
                "decimal":decimal,
                "series":fseries
                }
        return json
    return specification

def multiline(df):
    series = []
    for c in df.columns:
        line = df[c].to_json(orient = 'split')
        line = json.loads(line)
        del line['index']
        line['type'] = 'line'
        series.append(line)

    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit='',decimal=2,series=series,calc = 0):
        '''atom_specification'''
        def atom_sp(x):
            assert type(x)==list,"Input data must be of type list"
            out = []
            for d in x:
                try:
                    out.append({"dv":"{}{}".format(round(div(d,10**calc),decimal),unit), "value":d})
                except:
                    out.append({"dv":"N/A"})
            return out
        fseries = []
        for s in series:
            s['data'] = atom_sp(s['data'])
            fseries.append(s)
        
        if type(x[0]) != str or type(x[0]) != int or type(x[0]) != float:
            x = [str(a) for a in x]
        json = {"outer_title":{"text":title},
        "legend":{"data":legend},
        "yAxis": {"type":y},
        "xAxis": {"data":x },
        "unit":unit,
        "decimal":decimal,
        "series":fseries
        }
        return json
    return specification    
    
def multibar(df):
    series = []
    for c in df.columns:
        line = df[c].to_json(orient = 'split')
        line = json.loads(line)
        del line['index']
        line['type'] = 'bar'
        series.append(line)

    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit='',decimal=2,series=series,calc = 0):
        '''atom_specification'''
        def atom_sp(x):
            assert type(x)==list,"Input data must be of type list"
            out = []
            for d in x:
                out.append({"dv":"{}{}".format(round(div(d,10**calc),decimal),unit), "value":d})
            return out
        fseries = []
        for s in series:
            s['data'] = atom_sp(s['data'])
            fseries.append(s)
        
        if type(x[0]) != str or type(x[0]) != int or type(x[0]) != float:
            x = [str(a) for a in x]
        json = {"outer_title":{"text":title},
        "legend":{"data":legend},
        "yAxis": {"type":y},
        "xAxis": {"type":"category","data":x },
        "unit":unit,
        "decimal":decimal,
        "series":fseries
        }
        return json
    return specification    



def table(df):
    for x in df.index:
        if type(x) != int:    
            df = df.reset_index()
            break
    def specification(df=df,count = len(df),detail = 0,title='默认标题',y="value",
                      legend=df.columns.tolist(),
                      x=df.index.tolist(),
                      unit='',decimal=2,calc = 0,
                      **kwarg):
        data = {}
        for c in df.columns:
            #如果是字符串形式的则不需要指定decimal,unit等
            if type(df[c].iloc[0]) != str:
                data[''.join(lazy_pinyin(c))+str((legend.index(c)))] = {"data":df[c].tolist(),"decimal":decimal,"unit":unit,"calc":calc}
            else:
                data[''.join(lazy_pinyin(c))+str((legend.index(c)))] = {"data":df[c].tolist()}

        json = {
                "data_type":"table",
                "outer_title":{"text":title},
                "headName":legend,
                "headCol":[''.join(lazy_pinyin(x))+str(legend.index(x)) for x in legend],
                "data":data,
                "count":count,
                "detail":detail
                }
        
        #跳转表格配置
        ##写死第一列为行业名称
        if detail == 2:
            json["data"]["detail_id"] = {"data":[kwarg['detail_id'][m] for m in df.iloc[:,0].tolist()]}
        
        
        return json
    return specification

def wide_table(df):
    for x in df.index:
        if type(x) != int:    
            df = df.reset_index()
            break
    def specification(df=df,title='默认标题',count = len(df),detail = 0,config = "not_found",y="value",legend=df.columns.tolist(),
                      units=dict(zip(df.columns.tolist(),['']*len(df.columns))),
                      decimals=dict(zip(df.columns.tolist(),[2]*len(df.columns))),
                      calcs = dict(zip(df.columns.tolist(),[0]*len(df.columns))),
                      **kwarg):
        data = {}
        #读取宽表配置
        if config != 'not_found':
            config = pd.read_excel(config)
            units = dict(zip(config['original'].tolist(),config['unit'].tolist()))
            calcs = dict(zip(config['original'].tolist(),config['calc'].tolist()))
            decimals = dict(zip(config['original'].tolist(),config['decimal'].tolist()))         
            
        for c in df.columns:
            #如果是字符串形式的则不需要指定decimal,unit等
            if type(df[c].iloc[0]) != str:
                data[''.join(lazy_pinyin(c))+str((legend.index(c)))] = {"data":df[c].tolist(),"decimal":decimals[c],"unit":units[c],"calc":calcs[c]}
            else:
                data[''.join(lazy_pinyin(c))+str((legend.index(c)))] = {"data":df[c].tolist()}
        json = {
                "data_type":"table",
                "outer_title":{"text":title},
                "headName":legend,
                "headCol":[''.join(lazy_pinyin(x))+str(legend.index(x)) for x in legend],
                "data":data,
                "count":count,
                "detail":detail
                }
        
        if detail == 2:
            json["data"]["detail_id"] = {"data":[kwarg['detail_id'][m] for m in df.iloc[:,0].tolist()]}
        return json
    return specification


def twoaxbar(df):
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit1='',unit2='',decimal1=2,decimal2=2,calc1 = 0,calc2=0):
        #字符串转化 object not json serializable
        if type(df.index.tolist()[0]) is pd.Timestamp:
            x = [m.strftime("%Y-%m-%d") for m in x]
        if type(x[0])!=str:
            x = [str(m) for m in x]            
        
        json = {
                "toolbox":{"axisPointer":{"type":"shadow"}},
                "outer_title":{"text":title},
                "legend":{"data":legend},
                "xAxis":{"type":"category","data":x},
                "yAxis":[{"type":"value","name":legend[0]},{"type":"value","name":legend[1]}],
                "series":[{"name":legend[0],"type":"bar","unit":unit1,"decimal":decimal1,"calc":calc1,"data":[{"dv":"{}{}".format(round(div(d,10**calc1),decimal1),unit1),"value":d} for d in df[legend[0]]]},
                           {"name":legend[1],"type":"bar","unit":unit2,"decimal":decimal2,"calc":calc2,"yAxisIndex":1,"data":[{"dv":"{}{}".format(round(div(d,10**calc2),decimal2),unit2),"value":d} for d in df[legend[1]]]}]
                }
        return json
    return specification

def barnline(df):
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),unit1='',unit2='',decimal1=2,decimal2=2,calc1 = 0,calc2=0):
        if type(df.index.tolist()[0]) is pd.Timestamp:
            x = [m.strftime("%Y-%m-%d") for m in x]
        if type(x[0])!=str:
            x = [str(m) for m in x]            
        
        json = {
                "toolbox":{"axisPointer":{"type":"shadow"}},
                "outer_title":{"text":title},
                "legend":{"data":legend},
                "xAxis":{"type":"category","data":x},
                "yAxis":[{"type":"value","name":legend[0]},{"type":"value","name":legend[1]}],
                "series":[{"name":legend[0],"type":"bar","unit":unit1,"decimal":decimal1,"calc":calc1,"data":[{"dv":"{}{}".format(round(div(d,10**calc1),decimal1),unit1),"value":d} for d in df[legend[0]]]},
                           {"name":legend[1],"type":"line","unit":unit2,"decimal":decimal2,"calc":calc2,"yAxisIndex":1,"data":[{"dv":"{}{}".format(round(div(d,10**calc2),decimal2),unit2),"value":d} for d in df[legend[1]]]}]
                }
        return json
    return specification

def twoaxline(df):
    def specification(df=df,title='默认标题',y="value",legend=df.columns.tolist(),x=df.index.tolist(),units={},decimals={},calcs={},axiscord={}):
        #字符串转化 object not json serializable
        if type(df.index.tolist()[0]) is pd.Timestamp:
            x = [m.strftime("%Y-%m-%d") for m in x]
        if type(x[0])!=str:
            x = [str(m) for m in x]            
        
        series = []
        for c in df.columns:
            entry = {}
            entry['name']=c
            entry["type"] = "line"
            entry['unit'] = units[c]
            entry['decimal'] = decimals[c]
            entry['calc'] = calcs[c]
            
            data = []
            for d in df[c]:
                try:
                    data.append({"dv":"{}{}".format(round(div(d,10**calcs[c]),decimals[c]),units[c]),"value":d})
                except:
                    data.append({"dv":'-',"value":None})
            entry['data'] = data
            entry['yAxisIndex'] = axiscord[c]
            series.append(entry)
        json = {
                "toolbox":{"axisPointer":{"type":"shadow"}},
                "outer_title":{"text":title},
                "legend":{"data":legend},
                "xAxis":{"data":x},
                "yAxis":[{"type":"value","name":legend[0]},{"type":"value","name":legend[-1]}],
                "series":series
                }
        return json
    return specification       

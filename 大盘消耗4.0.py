# -*- coding: utf-8 -*-
"""
huihong.hu

大盘数据图表
"""
import json
import requests
import time
from datetime import datetime, date, timedelta
import pandas as pd
from pyecharts import Line,Page,Grid,Timeline, EffectScatter, Overlap
import itertools

url = 'http://192.168.10.251:50002/query/statistic'

def date2ts(date):
    ts = time.strptime(date, "%Y-%m-%d")
    return int(time.mktime(ts))        
        
############################
meta_adx=pd.read_excel(r'C:\Users\Administrator\Desktop\工作\dsp数据\meta_adx.xls')
meta_product=pd.read_excel(r'C:\Users\Administrator\Desktop\工作\dsp数据\meta_product.xls')
meta_spot=pd.read_excel(r'C:\Users\Administrator\Desktop\工作\dsp数据\meta_spot.xls')
meta_dsp_user=pd.read_excel(r'C:\Users\Administrator\Desktop\工作\dsp数据\meta_dsp_user.xls')


begin_time = (date.today() + timedelta(days = -7)).strftime("%Y-%m-%d")
end_time = (date.today() + timedelta(days = -1)).strftime("%Y-%m-%d")
#%%
page = Page()
line1 = Line("","     图表中TOP消耗的入榜标准是昨日消耗最高的，涨幅TOP的入榜标准是昨日涨跌差值最大的"\
            ,width="1200px",height="100px",subtitle_color='#000',subtitle_text_size=17) 
page.add_chart(line1)

#%% 总消耗走势
querybody = {
        "begin_time": date2ts(begin_time),
        "end_time":   date2ts(end_time),
        "timeout": 300000,
        "keys": [
                "date"
            ],
        "dims": [],
        "query_type": "default",
        "metrics": [
            "cost_r",
            ],
        "orderby": [
            {"name": "date", "asc":True}
            ],
        "conds": {
            },
        "must_keys": 0,
        "in_keys_order": 0,
        "limit": 2000000,
        "use_item": 1,
        "use_realtime": 1,
        "debug": 1
        }
resp = requests.post(url, data=json.dumps(querybody))
resp.close()
rbody = resp.json()
if rbody['suc'] == 1 and rbody['is_suc']:
    df=pd.DataFrame(rbody['items'],columns=['date','cost_r'])
   
grid= Grid(width=1000,height=350)
line = Line('总消耗一周走势')
line.add('总消耗',df['date'].apply(lambda x:str(x)),df['cost_r'].apply(lambda x:int(x)),is_random=True) 
es = EffectScatter()
es.add("", df['date'].apply(lambda x:str(x)),df['cost_r'].apply(lambda x:int(x)),effect_scale=8)
overlap = Overlap()
overlap.add(line)
overlap.add(es)
page.add_chart(overlap)
#%%渠道、广告位消耗数据
querybody = {
        "begin_time": date2ts(begin_time),
        "end_time":   date2ts(end_time),
        "timeout": 300000,
        "keys": [
                "channel_id","date","spot_id"
            ],
        "dims": [],
        "query_type": "default",
        "metrics": [
            "cost_r",
            ],
        "orderby": [
            {"name": "date", "asc":True}
            ],
        "conds": {
            },
        "must_keys": 0,
        "in_keys_order": 0,
        "limit": 20000000,
        "use_item": 1,
        "use_realtime": 1,
        "debug": 1
        }
resp = requests.post(url, data=json.dumps(querybody))
resp.close()
rbody = resp.json()
if rbody['suc'] == 1 and rbody['is_suc']:
    df=pd.DataFrame(rbody['items'],columns=['channel_id','date','spot_id','cost_r'])

#%%几个关注的渠道
follow_list=[10108,10010,10167,10097,10008,10169]
follow_df=df[df['channel_id'].isin(follow_list)] #关注的渠道及其广告位
follow_cnt=follow_df.groupby(by=['channel_id','date'],as_index=False).sum()[['channel_id','date','cost_r']]
lists=[]
for i,j in itertools.product(follow_cnt['channel_id'].drop_duplicates(),df['date'].drop_duplicates()):
      lists.append([i,j])
cnt_foll=pd.DataFrame(lists,columns=['channel_id','date'])
cnt_foll=pd.merge(cnt_foll,follow_cnt,how='left',on=['channel_id','date'])
cnt_foll=pd.merge(cnt_foll,meta_adx,how='left',left_on='channel_id',right_on='adxid')  

line = Line('关注渠道消耗')
for i in cnt_foll['name'].drop_duplicates():
       line.add(i,cnt_foll[cnt_foll['name']==i]['date'].apply(lambda x:str(x)),cnt_foll[cnt_foll['name']==i]['cost_r'].apply(lambda x:int(x))) 



#TOP 8 渠道消耗
top_cnt=df.groupby(by=['channel_id','date'],as_index=False).sum()[['channel_id','date','cost_r']]
cnt=top_cnt[top_cnt['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='cost_r').tail(8)['channel_id']
top_cnt=top_cnt[top_cnt['channel_id'].isin(cnt)] #TOP渠道
lists=[]
for i,j in itertools.product(top_cnt['channel_id'].drop_duplicates(),df['date'].drop_duplicates()):
      lists.append([i,j])
lists=pd.DataFrame(lists,columns=['channel_id','date'])
top_cnt=pd.merge(lists,top_cnt,how='left',on=['channel_id','date'])
top_cnt=pd.merge(top_cnt,meta_adx,how='left',left_on='channel_id',right_on='adxid')  

line1 = Line('TOP渠道消耗')
for i in top_cnt['name'].drop_duplicates():
       line1.add(i,top_cnt[top_cnt['name']==i]['date'].apply(lambda x:str(x)),top_cnt[top_cnt['name']==i]['cost_r'].apply(lambda x:int(x))) 
    
#画图
grid= Grid(width=900,height=350)
timeline = Timeline(is_auto_play=False, timeline_bottom=0)
timeline.add(line, '关注渠道消耗')
timeline.add(line1, 'TOP渠道消耗')
grid.add(timeline)
#        grid= Grid(width=1200,height=350)
#        grid.add(line,grid_right="55%")
#        grid.add(line1,grid_left="55%")
page.add_chart(grid)

#%%广告位消耗
#TOP广告位
def spot_line(cnt,name):
    follow_spot=df[df['channel_id']==cnt]
    follow_spot=follow_spot.groupby(by=['spot_id','date'],as_index=False).sum()[['spot_id','date','cost_r']]
    spt=follow_spot[follow_spot['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='cost_r').tail(7)['spot_id']
    top_spt=follow_spot[follow_spot['spot_id'].isin(spt)] #TOP广告位
    lists=[]
    for i,j in itertools.product(top_spt['spot_id'].drop_duplicates(),df['date'].drop_duplicates()):
          lists.append([i,j])
    lists=pd.DataFrame(lists,columns=['spot_id','date'])
    top_spt=pd.merge(lists,top_spt,how='left',on=['spot_id','date'])
    top_spt=top_spt.fillna(0) 
    top_spt=pd.merge(top_spt,meta_spot,how='left',left_on='spot_id',right_on='spotid')  
    top_spt.loc[top_spt['name'].isnull(),'name']=top_spt[top_spt['name'].isnull()]['spot_id']
    
    line = Line(name+'TOP广告位消耗')
    for i in top_spt['name'].drop_duplicates():
           line.add(i,top_spt[top_spt['name']==i]['date'].apply(lambda x:str(x)),top_spt[top_spt['name']==i]['cost_r'].apply(lambda x:int(x)),legend_pos="84%",mark_point=['max']) 
#    grid= Grid(width=1000,height=350)
#    grid.add(line,grid_right="25%")
#    page.add_chart(grid)
    
    '''                     涨跌幅TOP广告位                '''
    lists=[]
    for i,j in itertools.product(follow_spot['spot_id'].drop_duplicates(),df['date'].drop_duplicates()):
          lists.append([i,j])
    lists=pd.DataFrame(lists,columns=['spot_id','date'])
    follow_spt=pd.merge(lists,follow_spot,how='left',on=['spot_id','date'])#笛卡尔积
    follow_spt=follow_spt.fillna(0) 
    diff_spt=follow_spt.groupby(by='spot_id',as_index=False).diff()['cost_r'].rename('rise')
    diff_spt=pd.concat([follow_spt,diff_spt],axis=1)
    #涨幅TOP 7
    spt=diff_spt[diff_spt['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='rise').tail(7)['spot_id']
    rise_spt=diff_spt[diff_spt['spot_id'].isin(spt)] #TOP广告位
    rise_spt=pd.merge(rise_spt,meta_spot,how='left',left_on='spot_id',right_on='spotid') 
    rise_spt.loc[rise_spt['name'].isnull(),'name']=rise_spt[rise_spt['name'].isnull()]['spot_id']
    
    line1 = Line(name+'涨幅TOP广告位',width=1000,height=450)
    for i in rise_spt['name'].drop_duplicates():
           line1.add(i,rise_spt[rise_spt['name']==i]['date'].apply(lambda x:str(x)),rise_spt[rise_spt['name']==i]['cost_r'].apply(lambda x:int(x)),legend_pos="84%") 
    
    #跌幅TOP 7
    spt=diff_spt[diff_spt['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='rise').head(7)['spot_id']
    fall_spt=diff_spt[diff_spt['spot_id'].isin(spt)] #TOP广告位
    fall_spt=pd.merge(fall_spt,meta_spot,how='left',left_on='spot_id',right_on='spotid')  
    fall_spt.loc[fall_spt['name'].isnull(),'name']=fall_spt[fall_spt['name'].isnull()]['spot_id']
    
    line2 = Line(name+'跌幅TOP广告位',width=1000,height=450)
    for i in fall_spt['name'].drop_duplicates():
           line2.add(i,fall_spt[fall_spt['name']==i]['date'].apply(lambda x:str(x)),fall_spt[fall_spt['name']==i]['cost_r'].apply(lambda x:int(x)),legend_pos="84%") 
    #画图
    grid= Grid(width=900,height=350)
    timeline = Timeline(timeline_bottom=0)
    timeline.add(line, name+'TOP消耗')
    timeline.add(line1, name+'涨幅TOP消耗')
    timeline.add(line2, name+'跌幅TOP消耗')
    grid.add(timeline)
#        grid= Grid(width=1200,height=350)
#        grid.add(line,grid_right="55%")
#        grid.add(line1,grid_left="55%")
    page.add_chart(grid)
    

spot_line(10108,'今日头条')
spot_line(10044,'聚效')
spot_line(10169,'哔哩哔哩')
spot_line(10167,'咪咕')
spot_line(10008,'优酷')
spot_line(10097,'陌陌')
spot_line(10010,'新浪')
spot_line(10048,'新浪微博移动端')
spot_line(10004,'淘宝')
spot_line(10009,'百度')
page.render(path='F:/test.html')
#%% TOP客户消耗
def user_table(cnt,name):
    if cnt:
        querybody = {
                "begin_time": date2ts(begin_time),
                "end_time":   date2ts(end_time),
                "timeout": 300000,
                "keys": [
                        "user_id","date"
                    ],
                "dims": [],
                "query_type": "default",
                "metrics": [
                    "cost_r",
                    ],
                "orderby": [
                    {"name": "date", "asc":True}
                    ],
                "conds": {
                    "channel_id":cnt
                    },
                "must_keys": 0,
                "in_keys_order": 0,
                "limit": 2000000,
                "use_item": 1,
                "use_realtime": 1,
                "debug": 1
                }
    else:
        querybody = {
                "begin_time": date2ts(begin_time),
                "end_time":   date2ts(end_time),
                "timeout": 300000,
                "keys": [
                        "user_id","date"
                    ],
                "dims": [],
                "query_type": "default",
                "metrics": [
                    "cost_r",
                    ],
                "orderby": [
                    {"name": "date", "asc":True}
                    ],
                "conds": {
                    },
                "must_keys": 0,
                "in_keys_order": 0,
                "limit": 2000000,
                "use_item": 1,
                "use_realtime": 1,
                "debug": 1
                }
    resp = requests.post(url, data=json.dumps(querybody))
    resp.close()
    rbody = resp.json()
    if rbody['suc'] == 1 and rbody['is_suc']:
        df1=pd.DataFrame(rbody['items'],columns=['user_id','date','cost_r'])
    #Top 客户消耗
    user=df1[df1['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='cost_r').tail(7)['user_id']
    top_user=df1[df1['user_id'].isin(user)] #TOP用户
    lists=[]
    for i,j in itertools.product(top_user['user_id'].drop_duplicates(),df1['date'].drop_duplicates()):
          lists.append([i,j])
    lists=pd.DataFrame(lists,columns=['user_id','date'])
    top_user=pd.merge(lists,top_user,how='left',on=['user_id','date'])
    top_user=top_user.fillna(0) 
    top_user=pd.merge(top_user,meta_dsp_user,how='left',left_on='user_id',right_on='userid')  
    top_user.loc[top_user['username'].isnull(),'username']=top_user[top_user['username'].isnull()]['user_id']
    top_user['TOP客户']=top_user['user_id'].map(str)+'—'+top_user['username'].map(str)
    top_user=top_user[['date','TOP客户','cost_r']]
    top_user.columns=['日期','TOP客户','原始消耗']
    top_user=top_user.pivot('TOP客户','日期','原始消耗').applymap(lambda x:int(x))
    top_user['差额']=(top_user.iloc[:,-1].replace('',0)-top_user.iloc[:,-2].replace('',0)).apply(lambda x:int(x))
    
    pd.set_option('colheader_justify', 'center')  
    html_string = '''
    <html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
      <link rel="stylesheet" type="text/css" href="https://raw.githack.com/herinhu/filelist/master/df_style.css"/>
      <body>
         <h3>'''+name+'top客户消耗'+'''</h3>
        {table}
      </body>
    </html>.
    '''
    import codecs
    with codecs.open('F:/test.html','a+','utf-8') as html_file:
            html_file.write(html_string.format(table=top_user.to_html(header = True,index = True,classes='mystyle',escape=False)))

    
    '''                     涨跌幅TOP客户                '''
    #差分
    lists=[]
    for i,j in itertools.product(df1['user_id'].drop_duplicates(),df1['date'].drop_duplicates()):
          lists.append([i,j])
    lists=pd.DataFrame(lists,columns=['user_id','date'])
    follow_user=pd.merge(lists,df1,how='left',on=['user_id','date'])#笛卡尔积
    follow_user=follow_user.fillna(0) 
    diff_user=follow_user.groupby(by='user_id',as_index=False).diff()['cost_r'].rename('rise')
    diff_user=pd.concat([follow_user,diff_user],axis=1)

    #涨幅TOP
    user=diff_user[diff_user['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='rise').tail(7)['user_id']
    rise_user=diff_user[diff_user['user_id'].isin(user)] #TOP广告位
    rise_user=pd.merge(rise_user,meta_dsp_user,how='left',left_on='user_id',right_on='userid') 
    rise_user.loc[rise_user['username'].isnull(),'username']=rise_user[rise_user['username'].isnull()]['user_id']
    rise_user['涨幅TOP客户']=rise_user['user_id'].map(str)+'—'+rise_user['username'].map(str)
    rise_user=rise_user[['date','涨幅TOP客户','cost_r']]
    rise_user.columns=['日期','涨幅TOP客户','原始消耗']
    rise_user=rise_user.pivot('涨幅TOP客户','日期','原始消耗').applymap(lambda x:int(x))
    rise_user['差额']=(rise_user.iloc[:,-1].replace('',0)-rise_user.iloc[:,-2].replace('',0)).apply(lambda x:int(x))
    
    pd.set_option('colheader_justify', 'center')  
    html_string = '''
    <html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
      <link rel="stylesheet" type="text/css" href="https://raw.githack.com/herinhu/filelist/master/df_style.css"/>
      <body>
         <h3>'''+name+'涨幅top客户消耗'+'''</h3>
        {table}
      </body>
    </html>.
    '''
    import codecs
    with codecs.open('F:/test.html','a+','utf-8') as html_file:
            html_file.write(html_string.format(table=rise_user.to_html(header = True,index = True,classes='mystyle',escape=False)))

    #跌幅TOP
    user=diff_user[diff_user['date'].apply(lambda x:str(x))==end_time.replace('-', '')].sort_values(by='rise').head(7)['user_id']
    fall_user=diff_user[diff_user['user_id'].isin(user)] #TOP广告位
    fall_user=pd.merge(fall_user,meta_dsp_user,how='left',left_on='user_id',right_on='userid') 
    fall_user.loc[fall_user['username'].isnull(),'username']=fall_user[fall_user['username'].isnull()]['user_id']
    fall_user['跌幅TOP客户']=fall_user['user_id'].map(str)+'—'+fall_user['username'].map(str)
    fall_user=fall_user[['date','跌幅TOP客户','cost_r']]
    fall_user.columns=['日期','跌幅TOP客户','原始消耗']
    fall_user=fall_user.pivot('跌幅TOP客户','日期','原始消耗').applymap(lambda x:int(x))
    fall_user['差额']=(fall_user.iloc[:,-1].replace('',0)-fall_user.iloc[:,-2].replace('',0)).apply(lambda x:int(x))
    
    pd.set_option('colheader_justify', 'center')  
    html_string = '''
    <html>
      <head><title>HTML Pandas Dataframe with CSS</title></head>
      <link rel="stylesheet" type="text/css" href="https://raw.githack.com/herinhu/filelist/master/df_style.css"/>
      <body>
         <h3>'''+name+'跌幅top客户消耗'+'''</h3>
        {table}
      </body>
    </html>.
    '''
    import codecs
    with codecs.open('F:/test.html','a+','utf-8') as html_file:
            html_file.write(html_string.format(table=fall_user.to_html(header = True,index = True,classes='mystyle',escape=False)))
user_table('','')
user_table(10108,'今日头条')
user_table(10044,'聚效')
user_table(10169,'哔哩哔哩')
user_table(10010,'新浪')
user_table(10167,'咪咕')
spot_line(10008,'优酷')
user_table(10097,'陌陌')
user_table(10048,'新浪微博移动端')
user_table(10004,'淘宝')
user_table(10009,'百度')







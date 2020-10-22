#!/usr/bin/env python
# coding: utf-8

import json
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from pandas import json_normalize
from datetime import datetime, timedelta
import os
from sqlalchemy import exc

engine = create_engine("mysql+pymysql://teb101Club:teb101Club@127.0.0.1:3306/twstock??charset=utf8mb4", max_overflow=5)


connection = engine.connect


connection = engine.connect

if(datetime.now().hour>14):
    year=datetime.today().date().year
    month=datetime.today().date().month
    day=datetime.today().date().day
    newfile = datetime.today().date().isoformat()

else:
    year=(datetime.today()-timedelta(days=1)).date().year
    month=(datetime.today()-timedelta(days=1)).date().month
    day=(datetime.today()-timedelta(days=1)).date().day
    newfile = (datetime.today()-timedelta(days=1)).date().isoformat()



#year=datetime.today().year
#month=datetime.today().month
#day=datetime.today().day
#print(year, month, day)
# year=2020
# month=8

#newfile = datetime.today().date().isoformat()

rootdir='/home/spark/PycharmProjects/Stock_Price_API'

if month >9:
    jsonpath= rootdir+'/'+str(year)+'/'+str(year)+str(month)
else:
    jsonpath= rootdir+'/'+str(year)+'/'+str(year)+'0'+str(month)


filelist=os.listdir(jsonpath)
#print(filelist)
    
#print(len(filelist))

#print(newfile)

data=[]
for _file in filelist:
    #print(_file)
    if 'ng' not in _file and newfile in _file: #update latest data
        filename=jsonpath+'/'+_file
        with open(filename, 'r') as f:
            data.append(json.load(f))
            
df=json_normalize(data)
#print(df)
df=df.rename(index=str, columns={'code':'sid'})

df[["date"]] = df[["date"]].astype('datetime64[ns]')
df[["capacity"]] = df[["capacity"]].astype('int')
df[["transaction"]] = df[["transaction"]].astype('int')
#print(df.tail())


#df.info()
#print(df.describe())


# # 處理有交易紀錄的缺值



df['open'].fillna(df['turnover']/df['capacity'],inplace=True)
df['high'].fillna(df['turnover']/df['capacity'],inplace=True)
df['low'].fillna(df['turnover']/df['capacity'],inplace=True)
df['close'].fillna(df['turnover']/df['capacity'],inplace=True)
df[df['open'].isna()]
#nalist = df[df['open'].isna()].index
#print(nalist)


# # 處理有開市，但沒有紀錄 (移除該欄位)

# In[4]:


df=df.dropna()
df= df.reset_index(drop=True)
#print(df[df['open'].isna()])


# # 設定輸出到mySQL的資料型態

#from sqlalchemy.exc import SQLAlchemyError, IntegrityError
def sqlcol(dfparam):    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=4)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=2, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
            
        if "int64" in str(j):
            dtypedict.update({i: sqlalchemy.types.BIGINT()})
            
    return dtypedict

outputdict = sqlcol(df)
#print(outputdict)
#print(df.iloc[250:260])

#try:

df.to_sql('transactions', engine, schema='twstock',if_exists='append',index= False, dtype=outputdict, chunksize = 1000)
print('import mySQL Done')

# except exc.IntegrityError:
#     print('IntegrityError')
#     pass

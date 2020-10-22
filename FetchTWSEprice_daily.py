#!/usr/bin/env python
# coding: utf-8

import json
import os
import time
from datetime import datetime, timedelta

from requests.exceptions import ConnectionError

#from TWstockPrice import Stock
from TWstockPrice_proxy import Stock
from codes import codes
from codes import fetch

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

fetch.__update_codes()
rootdir='/home/spark/PycharmProjects/Stock_Price_API'

#False means no data update -- by 0050
def latestcheck(year, month):
    datelist=[]
    code='0050'
    stock= Stock(code)
    stock.fetch(year, month)

    if stock.data!=[]:
        datelist=[]
        for data in stock.data:
            datelist.append(data.date)

        #print(datelist)
    if datetime.today().date()>max(datelist).date() and (datetime.now().hour>14):
        return False
    elif (datetime.today()-timedelta(days=1)).date()>max(datelist).date():
        return False
    else:
        return True
    
year=datetime.today().date().year
month=datetime.today().date().month
#print(year, month)
#print(latestcheck(year, month))

def dlcheck_daily(path, ymd):
    filename=os.listdir(path)
    codeday_cap=[]
    
    for _file in filename:
        #print(_file[:-5])
        if _file.find('ng')==-1:
            codeday_cap.append(_file[:-5])
        else:
            codeday_cap.append(_file[:-8])
            #print(_file[:-8])
        
    codeday_cap=set(codeday_cap)
    undownload=[]
    
    for code, v in codes.items():
        if (v.type=="股票" and v.market=="上市") or code == '0050':
            fc='{}_{:%Y-%m-%d}'.format(code, ymd)
            #print(fc)
            if fc not in codeday_cap:
                undownload.append(fc)
            #print('undownloaded:{}'.format(fc)) 
    
    return undownload



if month >9:
    jsonpath= rootdir+'/'+str(year)+'/'+str(year)+str(month)
else:
    jsonpath= rootdir+'/'+str(year)+'/'+str(year)+'0'+str(month)


#print(jsonpath)


if not os.path.exists(jsonpath):
    os.mkdir(jsonpath)

    
if(datetime.now().hour>14):
    year=datetime.today().date().year
    month=datetime.today().date().month
    ymd=datetime.strptime(str(datetime.today().date()), "%Y-%m-%d")
else:
    year=(datetime.today()-timedelta(days=1)).date().year
    month=(datetime.today()-timedelta(days=1)).date().month
    ymd=datetime.strptime(str((datetime.today()-timedelta(days=1)).date()), "%Y-%m-%d")

undownload=dlcheck_daily(jsonpath, ymd)

#print('undownlowed count:', len(undownload))


stockDict={}
stockList=[]

cycle=0
while len(undownload)!=0:
    if latestcheck(year, month) == False:
        print('No Update Data!!')
        break

    cycle+=1
    print('downloading cycle:', cycle )
    for fc in undownload:
        try:
            code = fc[:4]
           
            stock= Stock(code)
            stock.fetch(year, month)
            print('{} is under fetching'.format(code))
            stockDict={}
            if len(stock.data) == 0:
                print('no data:{}'.format(code))
                if datetime.today().hour>14:
                    filename = code + '_' + '{:%Y-%m-%d}'.format(datetime.today().date()) + '.json'
                else:
                    filename = code + '_' + '{:%Y-%m-%d}'.format((datetime.today() - timedelta(days=1)).date()) + '.json'

                print('write to:', filename)
                with open(jsonpath + '/' + fc + '-ng.json', 'w') as f:
                    json.dump(stockDict, f)
                continue

            elif stock.data[0] == 'JSONDecodeError':
                print(stock.data)
                continue

            else:
                if stock.date[-1].date() < datetime.today().date() and datetime.today().hour > 14:  # 當天未有股價
                    with open(jsonpath + '/' + fc + '-ng.json', 'w') as f:
                        json.dump(stockDict, f)
                        break
                elif stock.date[-1].date() < (datetime.today() - timedelta(days=1)).date():  # 當天未有股價
                    with open(jsonpath + '/' + fc + '-ng.json', 'w') as f:
                        json.dump(stockDict, f)
                        break
                else:
                    for _data in stock.data:
                        filename=code+'_'+'{:%Y-%m-%d}'.format(_data.date)+'.json'
                        stockDict['code'] =code           
                        content =dict(_data._asdict())
                        
                        for k, v in content.items():
                            if k is 'date':
                                stockDict[k] = '{:%Y-%m-%d}'.format(_data.date)
                            else:
                                stockDict[k] = v

                        if not os.path.exists(jsonpath+'/'+filename): #減少磁碟寫入
                            print('write to:', filename)
                            with open(jsonpath+'/'+filename, 'w') as f:
                                json.dump(stockDict, f)
                        else:
                            #print('{} already download'.format(filename))
                            pass

        except ConnectionError:
        #      undownload=dlcheck_daily(jsonpath, ymd)
              print('ConnectionError')
              time.sleep(30)
              continue
                
    undownload=dlcheck_daily(jsonpath, ymd)
    print('undownlowed count:', len(undownload))           

print('complete!!')

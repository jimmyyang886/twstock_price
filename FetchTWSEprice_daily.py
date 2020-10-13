#!/usr/bin/env python
# coding: utf-8

# # Modify dlcheck

# In[140]:


import json
import os
import time
from datetime import datetime

from requests.exceptions import ConnectionError

from TWstockPrice import Stock
from codes import codes
from codes import fetch

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
        if datetime.today().date()>max(datelist).date(): 
            return False
    else:
        return True
    
year=datetime.today().date().year
month=datetime.today().date().month
#print(year, month)
#print(latestcheck(year, month))


# def dlcheck(path, ym):
#     filename=os.listdir(path)
#     code_cap=[]
    
#     for _file in filename:
#         code_cap.append(_file[:-8])
#     code_cap=set(code_cap)
#     undownload=[]
    
#     for code, v in codes.items():
#         if (v.type=="股票" and v.market=="上市") or code == '0050':
#             fc='{}_{:%Y-%m}'.format(code, ym)
#             if fc not in code_cap:
#                 undownload.append(fc)
#             #print('undownloaded:{}'.format(fc))         
#     return undownload

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
    
    #print(codeday_cap)
    
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

    
ymd=datetime.strptime(str(datetime.today().date()), "%Y-%m-%d")
#ymd=datetime(2020,10,7)
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
                filename=fc+'-ng.json'
                
                with open(jsonpath+'/'+filename, 'w') as f:
                    json.dump(stockDict, f)

            else:
                if stock.date[-1].date()< datetime.today().date():#當天未有股價
                    with open(jsonpath+'/'+fc+'-ng.json', 'w') as f: 
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
                            print('{} already download'.format(filename))

                        

        except ConnectionError:
            undownload=dlcheck_daily(jsonpath, ymd)
            print('ConnectionError')
            #break
            time.sleep(30)
            continue
                
    undownload=dlcheck_daily(jsonpath, ymd)
    print('undownlowed count:', len(undownload))           

print('complete!!')


# In[ ]:





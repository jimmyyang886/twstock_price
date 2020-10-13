#!/usr/bin/env python
# coding: utf-8

# # Modify dlcheck

# In[ ]:


import json
import os
import time
from datetime import datetime

from requests.exceptions import ConnectionError

from TWstockPrice import Stock
from codes import codes


def dlcheck(path, ym):
    filename=os.listdir(path)
    code_cap=[]
    
    for _file in filename:
        code_cap.append(_file[:-8])
    code_cap=set(code_cap)
    undownload=[]
    
    for code, v in codes.items():
        if v.type=="股票" and v.market=="上市":
            fc='{}_{:%Y-%m}'.format(code, ym)
            if fc not in code_cap:
                undownload.append(fc)
            #print('undownloaded:{}'.format(fc))         
    return undownload

#print('[undownload file list]')

year, month =2020,9

if month >9:
    jsonpath= str(year)+str(month)
else:
    jsonpath= str(year)+'0'+str(month)


if not os.path.exists(jsonpath):
    os.mkdir(jsonpath)
    
stockDict={}
stockList=[]

#global ym, jsonpath

ym=datetime.strptime(str(year)+'-'+str(month), "%Y-%m")

undownload=dlcheck(jsonpath, ym)
print('undownlowed count:', len(undownload))


cycle=0
while len(undownload)!=0:
    cycle+=1
    print('downloading cycle:', cycle )
    for fc in undownload:
        try:
            code = fc[:4]
            #print(fc)
            #print(code)           
            stock= Stock(code)
            stock.fetch(year, month)
            print('write to:', fc)
            stockDict={}
            if len(stock.data) == 0:
                print('no data:{}'.format(code))
                filename=fc+'-ng.json'
                with open(jsonpath+'/'+filename, 'w') as f:
                    json.dump(stockDict, f)

            else:               
                for _data in stock.data:
                    ymd='{:%Y-%m-%d}'.format(_data.date)
                    filename=code+'_'+ymd+'.json'
                    stockDict['code'] =code           
                    content =dict(_data._asdict())
                    for k, v in content.items():
                        if k is 'date':
                            stockDict[k] = '{:%Y-%m-%d}'.format(_data.date)
                        else:
                            stockDict[k] = v

                    #print('write to:', filename)
                    with open(jsonpath+'/'+filename, 'w') as f:
                        json.dump(stockDict, f)

        except ConnectionError:
            undownload=dlcheck(jsonpath, ym)
            print('ConnectionError')
            #break
            time.sleep(30)
            continue
                
    undownload=dlcheck(jsonpath, ym)
    print('undownlowed count:', len(undownload))           

print('complete!!')


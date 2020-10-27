from datetime import datetime, timedelta
from .TWstockPrice import Stock
#from TWstockPrice import Stock


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
    print(max(datelist), (datetime.today()-timedelta(days=1)).date())

        #print(datelist)
    if datetime.today().date()>max(datelist).date() and (datetime.now().hour>14):
        return False
    elif (datetime.today()-timedelta(days=1)).date()>max(datelist).date():
        return False
    else:
        return True


#year=datetime.today().date().year
#month=datetime.today().date().month
#print(latestcheck(year, month))

# if latestcheck(year, month) == False:
#     print('No Update Data!!')
#     break


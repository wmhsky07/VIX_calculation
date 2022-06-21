import yfinance as yf
import pandas as pd
import csv
import pymysql
import datetime as dt
import os
import time
import math
import statsmodels.formula.api as smf
import openpyxl

start = dt.datetime.now()
#------------------------------------------------------
db = pymysql.connect(user='root',password='root123456',database='local')
cs=db.cursor()
#----------------------Database------------------------
expset=[]
sql = 'select distinct expiration from vix;'
cs.execute(sql)
db.commit()
for r in cs:
    expset.append(r[0])
print(expset)

#------------------------------------------------------
def cal_diff(datalist,n,ele):
    diff=math.log(datalist[n+1][ele]/datalist[n][ele])
    return diff

def expfix_data(expdate):
    sql = "select * from vix where expiration = %s;"
    cs.execute(sql,expdate)
    db.commit()
    temp=[]
    test=[]
    final=[]
    for row in cs:
        temp.append(row)
    for i in range(0,len(temp)-1):
        if temp[i+1][4] - temp[i][4] < dt.timedelta(minutes=2):
            #print(temp[i])
            test.append(temp[i])

        else:
            final.append(test)
            #print(final)
            test=[]
    return final

def comfix_data(comp,exp):
    sql = "select * from vix where expiration = %s and company = %s;"
    cs.execute(sql,(exp,comp))
    db.commit()
    timeindex = 0
    timese=[]
    regse=[]
    for row in cs:
        timese.append([timeindex,row[0],row[3],row[5],row[6]])  #timeind/company/vix/forward/stockprice
        timeindex=timeindex+1
    for i in range(4,len(timese)-1):
        regse.append([timese[i][0],cal_diff(timese,i,4),cal_diff(timese,i,2),cal_diff(timese,i-1,2),cal_diff(timese,i-2,2),cal_diff(timese,i-3,2)])
    data_re = pd.DataFrame(regse)
    data_re.rename(columns={0:'time',1:'vix',2:'sp1',3:'sp2',4:'sp3',5:'sp4'},inplace=True)
    return data_re

def reg(name,exp,dataset,cat,res_set):
    if cat == 's':
        mod = smf.ols(formula='vix~sp1+sp2+sp3+sp4',data=dataset)
        reg=mod.fit()
        
        alpha = reg.params[0]
        beta1=reg.params[1]
        beta2=reg.params[2]
        beta3=reg.params[3]
        beta4=reg.params[4]
        tstat=reg.tvalues
        res_set.append([name,exp,alpha,beta1,beta2,beta3,beta4,tstat[1],tstat[2],tstat[3],tstat[4],len(dataset)])
    else:
        pass
        

dateset=[]
stocks=["AAL","CCL","AAPL","PFE","BAC","F","RLX","NCLH","DAL",
        "LCID","UAL","AMD","MRNA","UBER","BBD","PBR",
        "T","NVDA","XOM","EDU","MSFT","INTC",
        "ITUB","VALE"]
stocksa=["AAL"]

reg_result=[]

for name in stocks:
    for date in expset:
        sqlt='select * from options where expiration = %s and companyname = %s;'
        cs.execute(sqlt,(date,name))
        db.commit()
        index = 0
        for n in cs:
            index = index +1
        if index <300:
            print('less data')
        else:
            print('prifent')
            reg(name,date,comfix_data(name,date),'s',reg_result)
reg_resfm = pd.DataFrame(reg_result)
reg_resfm.rename(columns={0:'company',1:'date',2:'alpha',3:'beta1',4:'beta2',5:'beta3',6:'beta4',7:'t-stat1',8:'t-stat2',9:'t-stat3',10:'t-stat4',11:'obs'},inplace=True)
print(reg_resfm.iloc[5])
reg_resfm.to_excel('C:/courses/491/Finalresult1.xlsx')
#reg(comfix_data("AAL","2022-1-21"),'s')

        

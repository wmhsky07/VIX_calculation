import yfinance as yf
import pandas as pd
import csv
import pymysql
import datetime as dt
import os
import time
import math

start=dt.datetime.now()
#---------------------------------Begining of the Function Part----------------------------------------------------

db = pymysql.connect(user='root',password='root123456',database='local')
cs=db.cursor()
#-------------------------------------------------------------------------------------
rf=yf.Ticker("^TNX")
rf_rate=rf.history(interval='5m')
rf_rate=rf_rate.iloc[-1].loc['Close']

#-------------------------------------------------------------------------------------
def info_get(ticker,frameopt,framestk,date):
    stock_price=framestk.iloc[-1].loc['Close']
    call=frameopt[0]
    put=frameopt[1]
    call_opset=[]
    put_opset=[]
    for i in range(0,len(call)):
        item=call.iloc[i]
        company=ticker
        op_type="call"
        symbol = item.loc['contractSymbol']
        lastTD = item.loc['lastTradeDate']
        strike=item.loc['strike']
        lastprice=item.loc['lastPrice']
        bid=item.loc['bid']
        ask=item.loc['ask']
        volume=item.loc['volume']
        openint=item.loc['openInterest']
        iv=item.loc['impliedVolatility']
        maturity=date.date()-sdate
        if maturity.days<2:
            pass
        elif bid ==-1:
            pass
        else:
            call_opset.append([company,op_type,symbol,lastTD,strike,lastprice,bid,ask,volume,openint,iv,stock_price,maturity.days,rf_rate,date.date()])
    #print(call_opset)
    for i in range(0,len(put)):
        item=put.iloc[i]
        company=ticker
        op_type="put"
        symbol = item.loc['contractSymbol']
        lastTD = item.loc['lastTradeDate']
        strike=item.loc['strike']
        lastprice=item.loc['lastPrice']
        bid=item.loc['bid']
        ask=item.loc['ask']
        volume=item.loc['volume']
        openint=item.loc['openInterest']
        iv=item.loc['impliedVolatility']
        maturity = date.date() - sdate
        if maturity.days<2:
            pass
        elif bid ==-1:
            pass
        else:
            put_opset.append([company, op_type, symbol, lastTD, strike, lastprice, bid, ask, volume, openint,iv,stock_price,maturity.days,rf_rate,date.date()])

    return call_opset,put_opset

def vix_get(chainsc,chainsp):
    K0=0
    T=chainsc[0][12]/365
    upper_lim=chainsc[0][5]
    for i in chainsc:
        for k in chainsp:
            if i[4]==k[4]:
                price_delta=abs(i[5]-k[5])
                if price_delta < upper_lim:
                    upper_lim = price_delta
                    target_strike=i[4]
                else:
                    pass
    F=target_strike+math.exp(rf_rate*T)*(upper_lim)  #F has been calculated
    for i in range(len(chainsc)):
        if chainsc[i][4] < F:
            pass
        else:
            K0=chainsc[i-1][4]
            break                                      #K0 has been calculated
    sumation=0
    for i in range(0,min(len(chainsc),len(chainsp))-1):
        if K0 ==0:
            break
        elif chainsp[i][4] <= K0:
            QKi=chainsp[i][7]-chainsp[i][6]
            Ki=chainsp[i][4]
            if i==0:
                delta_Ki=(chainsp[i+1][4]-chainsp[i][4])/2
            else:
                delta_Ki=(chainsp[i+1][4]-chainsp[i-1][4])/2
        elif chainsc[i][4] > K0:
            QKi=chainsc[i][7]-chainsc[i][6]
            Ki=chainsc[i][4]
            if i==len(chainsc)-1:
                delta_Ki=(chainsc[i][4]-chainsc[i-1][4])/2
            else:
                delta_Ki = (chainsp[i+1][4] - chainsp[i-1][4])/2
        sumation=sumation+(delta_Ki/(Ki**2))*QKi
    part1=(2/T)*math.exp(rf_rate*T)*sumation
    part2=(1/T)*((F/K0)-1)**2
    vix=(part1-part2)**0.5*100
    #print(chainsc[0],vix)

    return vix,F


#-------------------------------------------------------------------------------------
sdate = dt.date.today()

stocks=["AAL","CCL","AAPL","PFE","BAC","F","RLX","NCLH","DAL",
        "LCID","UAL","AMD","MRNA","UBER","BBD","PBR",
        "T","NVDA","XOM","EDU","MSFT","INTC",
        "ITUB","VALE"]
stocks_mega=['AAPL','MSFT','GOOGL','AMZN','TSLA','FB','NVDA','TSM','UNH','JNJ','V','TCEHY','JPM','BAC','WMT','XOM','PFE','KO']

stocks_buffer=["NOK","FCX","PLTR",
        "FCEL","AMC","MRK","BA","NLY","ABEV","VZ","BP","CSCO",
        "PTON","MU","DVN","BMY","LVS","MRO","SWN","BABA"]


stocks_sample=["UAL","BAC","PFE","QQQ","AAPL"]

for i in stocks_sample:
    print(i)
    expdate=[]
    tk=yf.Ticker(i)
    stck=tk.history(period='1w',interval='1m')

    try:
        print(tk.option_chain('2021-11-27'))
    except Exception as ex:
        datechain=(str(ex)[str(ex).find("[")+1:-1])
        dateunit=""
        for char in datechain:  #Sorting expiration date
            if char== ",":
                expdate.append(dt.datetime.strptime(str(dateunit),"%Y-%m-%d"))
                dateunit=""
            elif char==" ":
                pass
            else:
                dateunit=dateunit+char
    print("Dateset for options is:",expdate)
    #try:
     #   expdate.pop(0)
    #except IndexError:
     #   pass


    for date in expdate:
        optc=tk.option_chain(str(date.date()))
        Atdate_data=list(info_get(i,optc,stck,date))

        Final_data=Atdate_data[0]+Atdate_data[1]

        try:
            vix_value=list(vix_get(Atdate_data[0],Atdate_data[1]))
        except UnboundLocalError:
            pass
        except IndexError:
            pass
        print(vix_value)

        try:
            sql="INSERT into options(CompanyName,type,symbol,LastTrade,strike,lastprice,bid,ask,volume,openint,iv,stockPrice,maturity,rf_rate,expiration) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            cs.executemany(sql,Final_data)
            db.commit()
        except pymysql.err.DataError:
            pass
        try:
            sql="INSERT into vix(company,maturity,expiration,vix,time,forward,stockprice) VALUES(%s,%s,%s,%s,%s,%s,%s);"
            cs.execute(sql,[i,(date.date()-sdate).days,date.date(),vix_value[0],dt.datetime.now(),vix_value[1],stck.iloc[-1].loc['Close']])
            db.commit()
        except pymysql.err.DataError:
            pass


#-------------------------------End of the function part----------------------------------------------
end=dt.datetime.now()
print("Running timer:",end-start)

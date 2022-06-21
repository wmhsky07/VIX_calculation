import yfinance as yf
import pandas as pd
import csv
import pymysql
import datetime as dt
import os
import time
import math

db = pymysql.connect(user='root',password='root123456',database='local')
cs=db.cursor()


Ticker = 'AAPL'
expiration = '220513'
cp_strike = '00175000'
cp='C'

Final = Ticker+expiration+cp+cp_strike
print(Final)


sql = 'select * from options where symbol = %s;'
sql1 = 'select * from options where symbol = \''+Final+'\';'
print(sql1)
cs.execute(sql,Final)
db.commit()

print(['time','lastprice','volume','openint','iv','stockprice','lasttrade'])
for row in cs:
    print([row[15],row[5],row[8],row[9],row[10],row[11],row[3]])

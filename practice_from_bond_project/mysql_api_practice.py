import requests
import pandas as pd
import re
import os
import json
from dotenv import load_dotenv

load_dotenv()

#1. DB 에서 accountId 전부 가져오기

#1-1. DB connect

import pymysql

def connection():
    try:
        conn = pymysql.connect(
            host = os.getenv('HOST'),
            user = os.getenv('DB_USER'),
            password = os.getenv('PASSWORD'),
            db = os.getenv('SCHEMA'),
            charset = os.getenv('CHARSET')
            )
        return conn

    except Exception as e:
        print(f'database connection failed due to : {e}')

conn = connection()
cur = conn.cursor()

#1-2 db에서 가져오기
sql = 'SELECT * FROM IRUDA_TRADE.BOND_PORTFOLIO'
cur.execute(sql)
data = cur.fetchall()

# dataframe 으로 직접
df = pd.read_sql(sql, conn)

# 2. API 연결해서 필요한 자료 가져오기

kb_url = os.getenv('url')

accountId = os.getenv('accountId')
bond_balance = f'/kb/v1/accounts/{accountId}/bond'

params = {
    'userNumber' : os.getenv('csNo'),
    'userPinCode' : os.getenv('pinNo')
}

res = requests.get(kb_url + bond_balance, params = params)
res.json()





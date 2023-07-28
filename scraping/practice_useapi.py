# %% naver 개발 api

import json
import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(format = '%(asctime)s - $(name)s - %(message)s', level = logging.INFO)

import openpyxl

wb = openpyxl.Workbook()
ws = wb.active
ws.column_dimensions['B'].width = 100
ws.append(['번호', '제품명', '링크'])

# naver api : header / params 둘다 존재

client_id = 'vg3Y3df93S3_0h1SKmZu'
client_pw = 'IsOaXyED9c'

header = {'X-Naver-Client-Id' : client_id, "X-Naver-Client-Secret" : client_pw}

text = input('쇼핑몰 검색어 입력 : ')

start_num = 1
prod_list = []


for index in range(10):

    start_num = start_num + index * 100

    params = {
    'query' : text,
    'display' : '100',
    'start' : str(start_num)
    }

    naver_open_api = 'https://openapi.naver.com/v1/search/shop.json'
    res = requests.get(naver_open_api, headers = header, params = params)

    try:
        if res.status_code == 200:
            data = res.json()
            for num, item in enumerate(data['items']):
                item_list = [num, item['title'], item['link']]
                prod_list.append(item_list)
                
            ws.append(item_list)
    except Exception as e:
        logging.ERROR(f'API CONNECTION FAILED : errorType {e}')

wb.save("C:/Users/문희관/Desktop/WB.xlsx")
wb.close()

# %% 정부 data api

import requests
from bs4 import BeautifulStoneSoup
import json

api_key = 'sJGdln0YmVJEEBEUxByk7QQKABv5DE/yhJPkzMdGQFjIhPShJ3okXRk+JeeA+szPr/5uzGEiN729LDMsNaxWTQ=='

url = 'http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo'

params = {
    'serviceKey' : api_key,
    'returnType' : 'json',
    'year' : 2022
}

res = requests.get(url, params = params)
res.json()
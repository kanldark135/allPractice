#%% 셀레늄 기초

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
# 1) chromedriver 이슈

# chromedriver 가 시도때도없이 업데이트되므로 그때마다 받기 귀찮으므로 아예 실행할때마다 재설치
# pip install webdriver_manager 설치 필요
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import time

# 2) driver 선언하기 driver = webdriver.Chrome(service = ChromeService(ChromeDriverManager().install()))
driver = webdriver.Chrome(service = ChromeService(ChromeDriverManager().install()))

#3) driver.get(url)
driver.get("http://davelee-fun.github.io/")

print(driver.title)

#4) 크롤링 및 입력 > 전부 javascript 함수 갖다 씀

driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")

elem = driver.find_element(By.CSS_SELECTOR, 'input.required.email[name = EMAIL]')
elem.clear()
elem.send_keys("kanldark135@naver.com")
elem.send_keys(Keys.RETURN)

time.sleep(20)

driver.quit()


#%% 캡쳐

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()))

driver.get("http://davelee-fun.github.io/")

elem = driver.find_element(By.CSS_SELECTOR, 'section.featured-posts')
elem.screenshot("C:/Users/문희관/Desktop/ee.png")

driver.quit()

#%% 루프로 텍스트 뽑아내기

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()))

driver.get("http://davelee-fun.github.io/")

elems = driver.find_elements(By.CSS_SELECTOR, 'section.featured-post')

for i in elems:
    print(i.text)


driver.quit()

#%% send_keys 로 입력하기 및 확인

import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

options = webdriver.ChromeOptions()

driver = webdriver.Chrome(options = options, service = Service(ChromeDriverManager().install()))

driver.get('https://davelee-fun.github.io/blog/TEST/index.html')

json = {
    'input#username' : 'kanldark135',
    'input#password' : 'mhk!#$134'
}

for place in json.keys():

    id = driver.find_element(By.CSS_SELECTOR, place)
    id.clear()
    id.send_keys(json.get(place))

time.sleep(5)

# Keys.REUTRN 
button = driver.find_element(By.CSS_SELECTOR, 'input[type = submit]')    
button.send_keys(Keys.RETURN)


time.sleep(5)
txt = driver.find_element(By.CSS_SELECTOR, 'div.message')
print(txt.text)

#%% 여러 페이지 순서대로 전환하면서 메 페이지마다 find 실시

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

import time

myoptions = webdriver.ChromeOptions()
# myoptions.add_argument('headless')

driver = webdriver.Chrome(options = myoptions, service = Service(ChromeDriverManager().install()))

driver.get('http://davelee-fun.github.io')

item_list = []

page_num = 5
i = 0

while i < page_num:
    i += 1

    items = driver.find_elements(By.CSS_SELECTOR, 'h4.card-text')
    for item in items:
        item_list.append(item.text)

    time.sleep(3)
    
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    
    next_page = driver.find_elements(By.CSS_SELECTOR, 'a.ml-1.mr-1')[-1]
    next_page.click()

driver.quit()

#%% 웹에 있는 이미지 크롤링 (엄밀히 말하면 다운로드)

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

import time

myoptions = webdriver.ChromeOptions()
# myoptions.add_argument('headless')

driver = webdriver.Chrome(options = myoptions, service = Service(ChromeDriverManager().install()))

driver.get('http://davelee-fun.github.io')

elems = driver.find_elements(By.CSS_SELECTOR, 'div.wrapthumbnail img')

src_list = []

for i in elems:
    src_list.append(i.get_attribute('src'))

import requests
import os

dir = 'C:/Users/문희관/Desktop/images'
os.makedirs(dir)

for j in src_list:
    response = requests.get(j)
    filename = dir + "/" + j.split("/")[-1]
    with open(filename, 'wb') as f:
        f.write(response.content)
        f.close()
#%%

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service

from webdriver_manager.chrome import ChromeDriverManager

import time

myoptions = webdriver.ChromeOptions()
# myoptions.add_argument('headless')

driver = webdriver.Chrome(options = myoptions, service = Service(ChromeDriverManager().install()))

dir = 'C:/Users/문희관/Desktop/screenshot_saved'
import os
os.makedirs(dir)

driver.get('http://davelee-fun.github.io/')
driver.save_screenshot(dir + "/" + '1page.png')

page_num = 5
i = 1

while i < page_num:
    i += 1

    driver.get('http://davelee-fun.github.io/page{0}/'.format(i))
    driver.save_screenshot(dir + "/{0}page_all.png".format(i))

    time.sleep(1)
    
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    
    next_page = driver.find_elements(By.CSS_SELECTOR, 'a.ml-1.mr-1')[-1]
    next_page.click()

driver.quit()
    
#%% 

from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select

from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()))

driver.get("https://finviz.com/")

rows = driver.find_elements(By.CSS_SELECTOR, 'table#js-signals_1 tr')

tickers = dict()

for row in rows[1:]:
    ticker = row.find_element(By.CSS_SELECTOR, 'a.tab-link').text
    price = row.find_element(By.CSS_SELECTOR, 'td').text
    tickers[ticker] = price

driver.close()
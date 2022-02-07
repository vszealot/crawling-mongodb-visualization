import requests
import datetime
import cx_Oracle
import json
import os
from pymongo import MongoClient
import csv
import pandas as pd
import plotly.express as px

# http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201
# 세부안내 -> 업종분류 현황 메뉴 data 크롤링
def get_stock_data():
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'}
    data = {"bld": "dbms/MDC/STAT/standard/MDCSTAT03901",  # 주소에 넘겨줄 값
            "mktId": "STK",
            "trdDd": f"{datetime.date.today().strftime('%Y%m%d')}",  # datetime.date.today().strftime('%Y%m%d')
            "money": "1",
            "csvxls_isNo": "false", }
    response = requests.post(url, headers=header, data=data).json()  # 주소에 값을 전달해주고 값을 받아 json으로 만든 부분!
    return response


def insert_oracle(response):
    dsn = cx_Oracle.makedsn("localhost", 1521, 'xe')  # oracle 주소를 입력
    db = cx_Oracle.connect('SCOTT', 'TIGER', dsn)  # oracle 접속 유저 정보
    cur = db.cursor()
    for i in response['block1']:
        value_list = list(i.values())[:8]  # 오라클에 넣을 값
        try:
            cur.execute("INSERT INTO STOCK VALUES(:1, :2, :3, :4, :5, :6, :7, :8)", value_list)
            db.commit()
        except:
            pass
    db.close()


def select_oracle():
    dsn = cx_Oracle.makedsn("localhost", 1521, 'xe')  # oracle 주소를 입력
    db = cx_Oracle.connect('SCOTT', 'TIGER', dsn)  # oracle 접속 유저 정보
    cur = db.cursor()
    cur.execute("SELECT STOCK_NAME, INDUSTRY_TYPE, FLUCTUATION, MARKET_CAPITALIZATION FROM STOCK") # oracle에서 만든 stock table에서 필요한 column들만 추출!
    column = ["STOCK_NAME", "INDUSTRY_TYPE", "FLUCTUATION", "MARKET_CAPITALIZATION"]  # dict 만들려고 컬럼명 만듬
    rows = cur.fetchall()  # SQL 결과를 받아옴
    data = []  # dict를 저장할 빈 리스트 선언
    for row in rows:
        list_row = list(row)
        list_row[2] = float(list_row[2])
        list_row[3] = int(list_row[3].replace(',', ''))
        data.append(dict(zip(column, list_row))) # column과 row를 묶어 dictionary를 만들어 data에 append함
    data_json = {}
    data_json['data'] = data
    # json형식으로 파일 만들기 처리!
    with open("../stock.json", "w", encoding='utf-8') as f:
        json.dump(data_json, f, ensure_ascii=False)


def mongo_import():
    os.system('mongoimport --db stock -c stock --file stock.json')


def mr_return():
    client = MongoClient('localhost', 27017)  # mongo 접속
    db = client.stock  # stock db접속
    res = db.stock.aggregate([
        {"$unwind": "$data"},
        {"$group": {"_id": "$data.INDUSTRY_TYPE", "sum": {"$sum": "$data.MARKET_CAPITALIZATION"}}}
    ])
    res2 = db.stock.aggregate([
        {"$unwind": "$data"},
        {"$project": {"_id": 0}}
    ])
    result = []
    for i in res2:
        result.append(i['data'])
    with open("../stock.csv", 'w', encoding='utf-8') as f:
        wr = csv.DictWriter(f, fieldnames=result[0].keys())
        wr.writeheader()
        wr.writerows(result)
    return res


def plotlyOnly():
    df = pd.read_csv('stock.csv')
    df = pd.DataFrame(data=df)
    fig = px.treemap(df,
                 path=['INDUSTRY_TYPE', 'STOCK_NAME'],
                 values='MARKET_CAPITALIZATION',
                 color='FLUCTUATION', color_continuous_scale='RdBu_r'
                )
    fig.data[0].texttemplate = "%{label}<br>등락률(전일대비):%{customdata}"
    fig.show()


if __name__ == '__main__':
    response = get_stock_data()
    insert_oracle(response)
    select_oracle()
    mongo_import()
    mr_return()
    plotlyOnly()

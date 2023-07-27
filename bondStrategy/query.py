#%% 

# adhoc으로 당분간 대응

import pymysql
import json
import requests
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

withdrawal_uid = 0

def db_get(withdrawal_uid):

    host = os.environ.get('HOST')
    db_user = os.environ.get('DB_USER')
    pw = os.environ.get('PASSWORD')
    db = os.environ.get('DB')
    charset= os.environ.get('CHARSET')

    # HOST=iruda.cp0tscwoictc.ap-northeast-2.rds.amazonaws.com
    # DB_USER=iruda
    # PASSWORD=4pD&S#srcCtV#uOaMP[y
    # DB=iruda_trade
    # CHARSET=utf8
    conn = pymysql.connect(host = host, user = db_user, password = pw, database = db, charset = charset)
    cur = conn.cursor()


    sql =  "select stock_company_uid, stock_company_pin, c.stock_account_id, c.uid, " \
        "iruda_member.decrypt(account_number, 'ACCOUNT') as account_number, pf.risk_grade, " \
        "c.first_operation_started_date, " \
        "(select max(created_at) from iruda_trade.deposit_withdraw " \
        "where stock_account_id = s.stock_account_id and inout_type = 'IN' " \
        "and trade_type NOT REGEXP '배당|수수료|예탁금이용료|환전' ) latest_deposited_at " \
        "from iruda_member.contract c, iruda_trade.stock_account s, " \
        "iruda_service.portfolio pf, iruda_service.product p " \
        "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
        f"and c.uid = {withdrawal_uid} " \
        "and c.stock_account_id = s.stock_account_id and c.product_id=18 " \
        "and c.status = 'ACTIVE' and s.status = 'ACTIVE' and c.first_operation_started_date is not null " \
        "order by latest_deposited_at asc, first_operation_started_date asc;"

    cur.execute(sql)
    df = cur.fetchall()
    conn.close()
    
    return df

if __name__ == "__main__":
    
    withdrawal_uid = input("type in uid : ")
    df = db_get(withdrawal_uid)
    df_get = pd.DataFrame(df, columns = ['csNo', 'pinNo', 'account_id(needless)', 'uid', 'accountId', '1', '2', '3'])


class bond_api:

    endpoint = "https://kb.iruda.io"
    userPassword = "VTYvK2xkQ1NMZ1NvcXFpRWh2NjRjUT09LENSWVBUT19LRVlQQURfRU5DLDIwMjMwMzEwMTYwNDQ0MjEz"
    userCI = "OJh4xJgF8IB4pbbXd9RPQVjZ1BiZJwJXSm0tsczgfOVjQoYBgqWR9Knkxdn6hTkydUiaGQOc4bo/ONd6tTCAuQ=="

    def __init__(self, uid):
    
        self.uid = uid
        df = db_get(self.uid)
        self.csNo = df[0][0]
        self.pinNo = df[0][1]
        self.accountId = df[0][4]

    def account_get(self):

        res = requests.get(url = f"{self.endpoint}/kb/v1/accounts/{self.accountId}/bond?userNumber={self.csNo}&userPinCode={self.pinNo}").json()
        print(res)
        return res
    
    def sellorder_post(self, securityCode, boughtDate, quantity = 1, price = 12000):

        data = {
            "orderType": "SELL",
            "userNumber": str(self.csNo),
            "userPinCode": str(self.pinNo),
            "securityCode": securityCode,
            "quantity": quantity,
            "price": price,
            "boughtDate" : boughtDate
        }
        
        requests.post(url = f"{self.endpoint}/kb/v1/accounts/{self.accountId}/orders/bond/LISTED", data = json.dumps(data))

client_1 = bond_api(withdrawal_uid)
client_1.account_get()

#%% 
# TODO 게좌조회
# def select_account_info():
#     sql_select = "select stock_company_uid, stock_company_pin, c.stock_account_id, c.uid," \
#                  "       iruda_member.decrypt(account_number, 'ACCOUNT') as account_number, pf.risk_grade, " \
#                  "       c.first_operation_started_date " \
#                  "from iruda_member.contract c, iruda_trade.stock_account s, " \
#                  "     iruda_service.portfolio pf, iruda_service.product p " \
#                  "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
#                  "and c.stock_account_id = s.stock_account_id and c.product_id = 18 " \
#                  "and c.status = 'ACTIVE' and s.status = 'ACTIVE'"
#
#     return sql_select
def select_account_info():
    sql_select = "select stock_company_uid, stock_company_pin, c.stock_account_id, c.uid, " \
                 "iruda_member.decrypt(account_number, 'ACCOUNT') as account_number, pf.risk_grade, " \
                 "c.first_operation_started_date, " \
                 "(select max(created_at) from iruda_trade.deposit_withdraw " \
                 "where stock_account_id = s.stock_account_id and inout_type = 'IN' " \
                 "AND trade_type NOT REGEXP '배당|수수료|예탁금이용료|환전' ) latest_deposited_at " \
                 "from iruda_member.contract c, iruda_trade.stock_account s, " \
                 "iruda_service.portfolio pf, iruda_service.product p " \
                 "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
                 "and c.stock_account_id = s.stock_account_id and c.product_id=18 " \
                 "and c.status = 'ACTIVE' and s.status = 'ACTIVE' and c.first_operation_started_date is not null " \
                 "order by latest_deposited_at asc, first_operation_started_date asc;"

    return sql_select


# TODO df csv로 insert
# symbol, name, remaining_days, start_date, end_date, interest_rate, average_price,
#                         issued_amount, grade, ratio, status, price, extra
def insert_csv():
    sql_insert = "INSERT INTO iruda_trade.bond_portfolio " \
                 "(security_code, security_name, remaining_days, start_date, end_date, interest_rate," \
                 "average_price, issued_amount, grade, ratio, status, price, extra) " \
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    return sql_insert


# TODO csv update
def update_csv():
    sql_update = "UPDATE iruda_trade.bond_portfolio SET deleted_at=Now() WHERE deleted_at IS NULL"

    return sql_update


def insert_datas_bond_history():
    sql_insert = "INSERT INTO iruda_trade.bond_history (user_id, user_name, security_name, security_code, price, uuid) " \
                 "VALUES (%s, %s, %s, %s, %s, %s)"

    return sql_insert


# def select_price_by_security_code():
#     sql_select = "SELECT price FROM iruda_trade.bond_portfolio WHERE security_code = %s AND deleted_at IS NULL"
#
#     return sql_select


def update_price_bond_portfolio():
    sql_update = "UPDATE iruda_trade.bond_portfolio SET price= %s WHERE security_code = %s AND deleted_at IS NOT NULL"

    return sql_update


# def select_status_and_ratio_by_code():
#     sql_select = "SELECT status, ratio FROM iruda_trade.bond_portfolio WHERE security_code = %s"
#
#     return sql_select

def select_status_and_ratio_by_code():
    sql_select = "SELECT status, ratio FROM iruda_trade.bond_portfolio WHERE security_code = %s and deleted_at is NULL"

    return sql_select


def insert_etf_order_book():
    sql_insert = "INSERT INTO iruda_trade.etf_order_book (account_number, security_name, security_code, " \
                 "etf_amounts, price, amount, quantity, status, market_day) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    return sql_insert


def select_sell_security():
    sql_select = "SELECT security_code FROM iruda_trade.bond_portfolio WHERE status = %s AND deleted_at IS NULL"

    return sql_select


def select_primary_key():
    sql_select = "SELECT order_id FROM iruda_trade.etf_order_book WHERE account_number=%s AND market_day = %s"

    return sql_select


def insert_etf_order_response():
    sql_insert = "INSERT INTO iruda_trade.etf_order_book_log (account_number, succeeded, management_id, order_number," \
                 "parent_order_number, security_code, quantity, price, message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    return sql_insert


def select_etf_order_book():
    sql_select = "SELECT account_number, security_code, quantity, price " \
                 "FROM iruda_trade.etf_order_book WHERE created_at=%s"

    return sql_select


def select_recent_etf_data():
    sql_select = """
                        SELECT etf.account_number, etf.price, etf.quantity, etf.market_day, etf.created_at
                        FROM iruda_trade.etf_order_book AS etf
                        INNER JOIN (
                            SELECT MAX(created_at) AS latest_created_at
                            FROM iruda_trade.etf_order_book
                        ) AS latest_orders
                        ON etf.created_at = latest_orders.latest_created_at
                    """

    return sql_select


def select_order_number_and_quantity():
    sql_select = """
                    SELECT log.account_number, log.order_number, log.quantity, log.created_at, log.succeeded
                    FROM iruda_trade.etf_order_book_log AS log
                    INNER JOIN (
                        SELECT MAX(created_at) AS latest_created_at
                        FROM iruda_trade.etf_order_book_log
                    ) AS latest_orders
                    ON log.created_at = latest_orders.latest_created_at
                """
    return sql_select


# def insert_modify_order_response():
#     sql_insert = "INSERT INTO iruda_trade.etf_order_book_log (account_number, management_id, order_number," \
#                  "parentOrderNumber, security_code, quantity, price) VALUES (%s, %s, %s, %s, %s, %s, %s)"
#
#     return sql_insert


def get_recent_management_id():
    sql_select = "SELECT management_id FROM iruda_trade.etf_order_book_log " \
                 "WHERE account_number = %s AND order_number = %s"

    return sql_select


def insert_order_book():
    sql_insert = "INSERT INTO iruda_trade.order_book (account_number, security_code, price, quantity, status, bought_date) " \
                 "VALUES (%s, %s, %s, %s, %s, %s)"

    return sql_insert


def select_order_data():
    sql_select = """
                    SELECT ob.account_number, ob.security_code, ob.price, ob.quantity, 
                            ob.status, ob.bought_date, ob.created_at 
                    FROM iruda_trade.order_book as ob
                    JOIN (
                    SELECT MAX(created_at) as max_created_at
                    FROM iruda_trade.order_book
                    ) AS temp
                    ON ob.created_at = temp.max_created_at;
                """

    return sql_select


def select_order_data_for_modify():
    sql_select = "WITH latest_order_book AS " \
                 "(SELECT account_number, security_code, MAX(created_at) AS latest_created_at " \
                 "FROM iruda_trade.order_book " \
                 "WHERE DATE(created_at) = CURRENT_DATE " \
                 "GROUP BY account_number, security_code), " \
                 "latest_order_book_log AS " \
                 "(SELECT account_number, security_code, succeeded, order_number, " \
                 "MAX(created_at) AS latest_log_created_at " \
                 "FROM iruda_trade.order_book_log " \
                 "WHERE DATE(created_at) = CURRENT_DATE " \
                 "GROUP BY account_number, security_code) " \
                 "SELECT ob.account_number, ob.security_code, lbl.succeeded, " \
                 "lbl.order_number, lbl.latest_log_created_at " \
                 "FROM latest_order_book ob " \
                 "JOIN latest_order_book_log lbl ON ob.account_number = lbl.account_number " \
                 "AND ob.security_code = lbl.security_code;"

    return sql_select


def insert_bond_order_response():
    sql_insert = "INSERT INTO iruda_trade.order_book_log (account_number, security_code, succeeded, message, order_number) " \
                 "VALUES (%s, %s, %s, %s, %s)"

    return sql_insert


def select_account_info_for_modify():
    sql_select = "select stock_company_uid, stock_company_pin " \
                 "from iruda_member.contract c, iruda_trade.stock_account s, " \
                 "     iruda_service.portfolio pf, iruda_service.product p " \
                 "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
                 "and c.stock_account_id = s.stock_account_id and c.product_id = 18 " \
                 "and c.status = 'ACTIVE' and s.status = 'ACTIVE'" \
                 "and account_number = iruda_member.encrypt(%s, 'ACCOUNT')"

    return sql_select


# 오늘 날짜 주문지 있는지 확인
def check_today_order_exists():
    sql_select = "select count(*) from iruda_trade.order_book where DATE(created_at) = CURRENT_DATE"

    return sql_select


# 원금
# def get_adjusted_principal():
#     sql_select = """
#         select dae.adjusted_principal, dae.stock_account_id
#         from iruda_trade.daily_account_evaluation dae
#         JOIN (select DATE_FORMAT(MAX(base_date)-INTERVAL 1 DAY, '%Y%m%d') as latest_base_date
#         from iruda_trade.daily_account_evaluation) AS temp
#         WHERE temp.latest_base_date = dae.base_date AND dae.stock_account_id IN (SELECT c.stock_account_id
#         FROM iruda_member.contract c, iruda_trade.stock_account s
#         WHERE c.product_id IN (18)
#         AND first_operation_started_date IS NOT NULL
#         AND expired_at IS NULL AND c.status = 'ACTIVE' AND s.status = 'ACTIVE');
#     """
#
#     return sql_select

def get_adjusted_principal():
    sql_select = """
        select dae.adjusted_principal, dae.stock_account_id 
        from iruda_trade.daily_account_evaluation dae 
        JOIN (select MAX(base_date) as latest_base_date 
        from iruda_trade.daily_account_evaluation) AS temp 
        WHERE temp.latest_base_date = dae.base_date AND dae.stock_account_id IN (SELECT c.stock_account_id 
        FROM iruda_member.contract c, iruda_trade.stock_account s 
        WHERE c.product_id IN (18) 
        AND first_operation_started_date IS NOT NULL 
        AND expired_at IS NULL AND c.status = 'ACTIVE' AND s.status = 'ACTIVE');
    """

    return sql_select

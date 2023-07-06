import csv
import aiohttp
import asyncio
import query
import decimal
from database import Database
from datetime import datetime as dt
import datetime
import nest_asyncio
from dto import *


# Test Modify etf_order_book

def get_pk():
    try:
        db = Database()
        db.connect_db()
        db.cur.execute(query.select_primary_key(),
                       datetime.now().strftime("%Y-%m-%d"))
        result = db.cur.fetchone()
        if result:
            pk = result[0]
            print(pk)
    except Exception as e:
        print(e)


def save_etf_order_book():
    try:
        db = Database()
        db.connect_db()
        account_number = '36546627801'
        etf_amounts = 199926.40000000002
        amount = 199926.40000000002
        quantity = 1
        market_day = '2023-04-14'

        db.cur.execute(query.insert_etf_order_book(),
                       (account_number, 'KBSTAR 단기종합채권(AA-이상)액티브', '385550',
                        etf_amounts, 103915, amount, quantity, '매수', market_day))
        db.con.commit()
    except Exception as e:
        print(e)
    finally:
        db.disconnect_db()


def check_dict():
    securities = {
        'KR6150351D32': {'securityName': '삼척 블루파워8 ', 'amounts_to_buy': 1195878.3133499997, 'quantity_to_buy': 1161},
        'KR6003492C91': {'securityName': '대한항공100-2 ', 'amounts_to_buy': 170839.75905000002, 'quantity_to_buy': 170},
        'KR6000152C15': {'securityName': '두산 307-2 ', 'amounts_to_buy': 170839.75905000002, 'quantity_to_buy': 155},
        'KR6079161AC2': {'securityName': '씨제이 씨지브이31 ', 'amounts_to_buy': 170839.75905000002, 'quantity_to_buy': 167}
    }
    print('KR6150351D32' in securities)


def check_todays_date_and_timestamp():
    db_time = "2023-04-20 12:58:41"
    today = datetime.today().strftime('%Y-%m-%d')
    print(today)
    print(today in db_time)

    db = Database()
    db.connect_db()
    db.cur.execute(query.select_order_number_and_quantity())
    rows = db.cur.fetchall()
    for row in rows:
        print(row)
        print(row[3].strftime('%Y-%m-%d'))
        print(today in row[3].strftime('%Y-%m-%d'))
    db.disconnect_db()


def select_recent_order_number_and_quantity():
    order_data_list = []
    # today = (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.today().strftime('%Y-%m-%d')

    db = Database()
    db.connect_db()
    db.cur.execute(query.select_order_number_and_quantity())
    rows = db.cur.fetchall()
    for row in rows:
        if today in row[3].strftime('%Y-%m-%d'):
            order_data_list.append({"account_number": row[0], "order_number": row[1], "quantity": row[2]})
        else:
            print("No accounts for ETF orders today")

    db.disconnect_db()
    print(order_data_list)


# def fetch_recent_order_status_api(account_number):
#     api_url = f"https://kb.iruda.io/kb/v1/accounts/{account_number}/orders"
#     params = {
#         "userNumber": "H003080517893",
#         "userPinCode": "1006822022",
#         "orderStatus": "ALL",
#         "orderType": "ALL",
#         "orderBy": "DESC",
#         "date": datetime.today().strftime("%Y%m%d")
#     }
#
#     response = requests.get(api_url, params=params)
#     if response.status_code == 200:
#         response_data = response.json()
#         if response_data["succeeded"]:
#             recent_order_response = response_data["result"]["orderHistories"]
#             print(recent_order_response)
#     else:
#         response_data = response.json()
#         print(response_data["error"]["exchangeMessage"])


# def fetch_recent_order_status_api_for_accounts():
#     recent_order_data = [{'account_number': '36625519601', 'order_number': '0080146260', 'quantity': 4}]
#
#     for account_data in recent_order_data:
#         account_number = account_data["account_number"]
#
#         fetch_recent_order_status_api(account_number)


def str_to_date():
    print(datetime.today().strftime("%Y%m%d"))


def select_recent_order_log_data(bond_orderbook):
    account_number_list = []
    order_log_list = []

    db = Database()
    db.connect_db()

    db.cur.execute(query.select_order_data_for_modify())
    rows = db.cur.fetchall()
    for row in rows:
        order_log_list.append(row)
        account_number_list.append(row[0])

    if not order_log_list:
        print("오늘 첫번째로 나간 채권 주문 없으므로 정정주문 필요 없음")

    return order_log_list, account_number_list


async def fetch_recent_bond_order_status(account, session: aiohttp.ClientSession, next_key=None):
    account_number = account["accountNumber"]

    api_url = f"https://kb.iruda.io/kb/v1/accounts/{account_number}/orders/bond"
    params = {
        "userNumber": account["csNo"],
        "userPinCode": account["pinNo"],
        "inquiryType": "BOND",
        "date": dt.today().strftime("%Y%m%d"),
        "bondMarketType": "Listed",
        "orderStatus": "UNFILLED",  # could be UNFILLED, FILLED, ORDER_FILLED
        "nextKey": next_key if next_key else ""
    }

    async with session.get(api_url, params=params) as response:
        if response.status == 200:
            response_data = await response.json()
            if response_data["succeeded"]:
                recent_bond_order_response = response_data["result"]["orderStatusList"]

                return recent_bond_order_response
        else:
            response_data = await response.json()
            print(response_data["error"]["exchangeMessage"])
            print(f"Error: failed to fetch recent bond order status for account_number: {account.account_number}")


# DB order_book_log에서 가장 latest datas 가져오기
def set_account_information_for_modify(account_number_list):
    account_information = {}
    db = Database()

    if account_number_list:
        db.connect_db()

        try:
            for account_number in account_number_list:
                account_information[account_number] = {}
                account_information[account_number]["accountNumber"] = account_number
                db.cur.execute(query.select_account_info_for_modify(), account_number)
                rows = db.cur.fetchall()
                for row in rows:
                    account_information[account_number]["csNo"] = row[0]
                    account_information[account_number]["pinNo"] = row[1]
            print(account_information)
            return account_information  # order API 호출할 때 필요한 정보들

        except Exception as e:
            print(f"정정주문용 account_information set 실패: {e}")
        finally:
            db.disconnect_db()

    else:
        print(f"오늘 나간 채권 주문이 없으므로 정정주문할 account 존재하지 않음")


async def fetch_all_recent_bond_order_status(order_log_list, account_information):
    bond_balances = {}
    next_key = None  # ('36644661601', 'KR6079161BC0', 1, '0020119539', datetime.datetime(2023, 4, 25, 10, 45, 18))

    async with aiohttp.ClientSession() as session:
        for order_log in order_log_list:
            account_number = order_log[0]

            bond_balances[account_number] = []
            security_code = order_log[1]
            succeeded = order_log[2]

            if succeeded:  # 주문이 성공했을시에만 채권 실시간 데이터 조회
                while True:  # nextKey 있을시 다음 페이지 조회
                    bond_balance_data = await fetch_recent_bond_order_status \
                        (account_information[account_number], session, next_key)
                    bond_balances[account_number].extend(bond_balance_data)

                    if "result" in bond_balance_data and "nextKey" in bond_balance_data["result"]:
                        next_key = bond_balance_data["result"]["nextKey"]
                        if not next_key.strip():
                            break
                    else:
                        break

            else:
                print(f"{account_number}의 첫번째 {security_code}가 실패하였으므로 정정 주문 불가")
        print(bond_balances)

        return bond_balances


# def parse_bond_balance_data(bond_balances):
#     modify_information_for_accounts = {}
#
#     for account_number, bond_balance in bond_balances.items():
#         modify_information_for_accounts[account_number] = {}
#         all_bonds = []
#         single_bond = {}
#
#         bonds = [bond for item in bond_balance for bond in item]
#
#         if bonds:
#             for bond in bonds:
#                 origin_order_number = bond["originOrderNumber"]
#                 order_number = bond["orderNumber"]
#                 unfilled_quantity = bond["unfilledQuantity"]
#
#                 if unfilled_quantity > 0:
#                     single_bond["origin_order_number"] = origin_order_number
#                     single_bond["order_number"] = order_number
#                     single_bond["unfilled_quantity"] = unfilled_quantity
#                     all_bonds.append(single_bond)
#
#         modify_information_for_accounts[account_number] = all_bonds
#
#     return modify_information_for_accounts

# modify_information_for_accounts =
#  {'36644661601': [],
#   '36641544901': [{'origin_order_number': '0000000000', 'order_number': '0080115022', 'unfilled_quantity': 20110}],
#   '36645470501': [{'origin_order_number': '0000000000', 'order_number': '0000112875', 'unfilled_quantity': 5025}],
#   '36640325001': [{'origin_order_number': '0000000000', 'order_number': '0000112876', 'unfilled_quantity': 10050}]}
def parse_bond_balance_data(bond_balances, order_log_list):
    try:
        modify_information_for_accounts = {}
        for order_log in order_log_list:
            account_number = order_log[0]
            order_number = order_log[3]
            security_code = order_log[1]

            bond_balance = bond_balances[account_number]
            bonds = [bond for item in bond_balance for bond in item]
            all_bonds = [
                {
                    "origin_order_number": bond["originOrderNumber"],
                    "order_number": bond["orderNumber"],
                    "security_code": security_code,
                    "unfilled_quantity": bond["unfilledQuantity"],
                } for bond in bonds
                if bond["orderNumber"] == order_number and bond["unfilledQuantity"] > 0
            ]
            modify_information_for_accounts[account_number] = all_bonds

        print(modify_information_for_accounts)
        return modify_information_for_accounts

    except Exception as e:
        print(f"failed to parse bond balance data: {e}")


# def fetch_order_api(response_data):
#     if response_data["succeeded"]:
#         buy_order_response = response_data["result"]
#
#         account_number = buy_order_response["accountNumber"]
#         succeeded = True
#         management_id = account["pk"]
#         order_number = buy_order_response["orderNumber"]
#         parent_order_number = buy_order_response["parentOrderNumber"]
#         security_code = buy_order_response["securityCode"]
#         quantity = buy_order_response["quantity"]
#         price = buy_order_response["price"]
#
#         await self.save_order_response(account_number, succeeded, management_id,
#                                        order_number, parent_order_number, security_code,
#                                        quantity, price)


async def test_order_book_for_selling_securities(bond_orderbook):
    account_list = []
    account1 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901', decimal.Decimal('2.80'),
                           'ACTIVE')  # LIZ
    account1.evaluation_balance = 100000
    account1.total_evaluation_balance = 100000
    # account2 = AccountData('1007633319', 'H001105129993', 18001, 2, '36540453101', decimal.Decimal('2.80'), 'ACTIVE')   #MINO

    account_list.append(account1)
    # Sample securities_to_sell_with_account dictionary
    sample_securities_to_sell_with_account = {
        "36540447901": [
            {
                "securityCode": "A385550",
                "name": "KBSTAR 단기종합채권(AA-이상)액티브",
                "currency": "KRW",
                "quantity": 0,
                "possibleOrderQuantity": 0,
                "currentPrice": 104035.00,
                "buyUnitPrice": 0.00,
                "evaluatedAmount": 0,
            },
            {
                "securityCode": "KR6150351D32",
                "name": "삼척블루파워8",
                "currency": "KRW",
                "quantity": 241,
                "possibleOrderQuantity": 241,
                "currentPrice": 10083.04,
                "buyUnitPrice": 10060.00,
                "evaluatedAmount": 243001,
            }
        ]
    }

    # bond_orderbook = BondOrderBook()
    if sample_securities_to_sell_with_account:
        matching_securities_by_account = {}
        for account_number, securities in sample_securities_to_sell_with_account.items():
            matching_account = None
            for account in account_list:
                if account.account_number == account_number:
                    matching_account = account
                    break

            matching_securities = await bond_orderbook.fetch_bond_balance_info(matching_account, securities)
            matching_securities_by_account.update(matching_securities)

            return account_list, matching_securities


def order_book_for_selling_securities(matching_securities_by_account):
    order_book = {}

    for account_number, matching_securities in matching_securities_by_account.items():
        if account_number not in order_book:
            order_book[account_number] = {}

        for matching_security in matching_securities:
            security_code = matching_security["securityCode"]
            security_name = matching_security["securityName"]
            bought_date = matching_security["boughtDate"]
            quantity = matching_security["quantity"]

            if security_code not in order_book[account_number]:
                order_book[account_number][security_code] = {
                    "securityName": security_name,
                    "boughtDatesAndQuantity": [{"date": bought_date, "quantity": quantity}],
                }
            else:
                order_book[account_number][security_code]["boughtDatesAndQuantity"].append({
                    "date": bought_date,
                    "quantity": quantity,
                })

    return order_book


def generate_sell_csv_file(account_list, order_book, file_name="매도 orderbook.csv"):
    try:
        with open(file_name, mode="w", newline='') as file:
            csv_writer = csv.writer(file)

            # Write selling_orderbook header
            csv_writer.writerow(["매도 오더북"])

            # Find unique security codes in the order_book
            selling_security_names = set()
            for account_number, securities in order_book.items():
                for security in securities.values():
                    selling_security_names.add(security["securityName"])

            # header
            csv_writer.writerow([""] + list(selling_security_names))

            for account_number, securities in order_book.items():
                row = [account_number]
                account_evaluation_balance = next(account.evaluation_balance for account in account_list if
                                                  account.account_number == account_number)
                row.append(account_evaluation_balance)

                for security_name in selling_security_names:
                    total_quantity = 0
                    for security in securities.values():
                        if security["securityName"] == security_name:
                            total_quantity += sum(
                                [bought_date_info["quantity"] for bought_date_info in
                                 security["boughtDatesAndQuantity"]])

                    row.append(total_quantity if total_quantity > 0 else "")
                csv_writer.writerow(row)

        return file_name

    except Exception as e:
        print(f"csv 생성 실패: {e}")


async def test_save_order_response(bond_orderbook):
    etf_order_response = {
        "succeeded": True,
        "result": {
            "orderNumber": "0040177058",
            "parentOrderNumber": "0000000000",
            "managementId": None,
            "orderType": "SELL",
            "orderStatus": None,
            "securityCode": "003490",
            "quantity": 1,
            "price": 22550,
            "accountNumber": "36540447901"
        },
        "error": None,
        "message": None,
        "code": 0
    }

    if etf_order_response["succeeded"]:
        buy_order_response = etf_order_response["result"]

        # order_history = buy_order_response["orderHistories"][0]  # Nested structure

        account_number = buy_order_response["accountNumber"]
        succeeded = True
        management_id = 3
        order_number = buy_order_response["orderNumber"]
        parent_order_number = buy_order_response["parentOrderNumber"]
        security_code = buy_order_response["securityCode"]
        quantity = buy_order_response["quantity"]
        price = buy_order_response["price"]

        await bond_orderbook.save_order_response(account_number, succeeded, management_id,
                                                 order_number, parent_order_number, security_code,
                                                 quantity, price, '정상적으로 매수주문 완료되었습니다.')


def test_created_at():
    account_list = []
    account1 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901', decimal.Decimal('2.80'),
                           'ACTIVE', '2023-04-19 14:32:44')  # LIZ
    account2 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901', decimal.Decimal('2.80'),
                           'ACTIVE', '2023-04-20 08:32:59')  # LIZ
    account3 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901', decimal.Decimal('2.80'),
                           'ACTIVE', '2023-04-18 08:32:59')  # LIZ
    account4 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901', decimal.Decimal('2.80'),
                           'ACTIVE', '2023-04-17 08:32:59')  # LIZ
    account_list.append(account1)
    account_list.append(account2)
    account_list.append(account3)
    account_list.append(account4)

    account_list.sort(key=lambda account: account.latest_deposited_at)
    for account in account_list:
        print(account.latest_deposited_at)


def test():
    temp = {}
    temp["yeongmin"] = []
    list = [{1, 2, 3, 4}, {5, 6, 7, 8}]
    temp["yeongmin"].append(list)
    print(temp)


def select_order_data():
    order_list = []

    today = dt.today().strftime('%Y-%m-%d')
    db = Database()
    db.connect_db()
    db.cur.execute(query.select_order_data())
    rows = db.cur.fetchall()
    for row in rows:
        if today in row[6].strftime('%Y-%m-%d'):
            order_list.append(row)

    return order_list


def set_account_information():
    db = Database()
    db.connect_db()
    account_list = []
    db.cur.execute(query.select_account_info())
    rows = db.cur.fetchall()
    for row in rows:  # 고객별 first_operation_started_date => row[6]
        account = AccountData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
        account_list.append(account)

    return account_list


def test2():
    order_list = select_order_data()
    tasks = []
    account_list = set_account_information()
    for data in order_list:
        try:
            account_number, security_code, price, quantity, order_type, bought_date, created_at = data
            account = next(account for account in account_list if account.account_number == account_number)
            tasks.append([account.account_number, security_code, quantity, price, order_type, bought_date])
        except StopIteration:
            break
    print(tasks)


def test_schedule2():
    print("etf_order_book process 시작함")


def reset_values():
    print(f"reset_values 시작함: {datetime.datetime.now().minute}")


def test_scheduling():
    schedule_temp = AsyncIOScheduler(timezone=ZoneInfo('Asia/Seoul'))
    schedule_temp.add_job(test_schedule2, 'cron', day_of_week='mon-fri', hour='9-15', minute='*')
    schedule_temp.add_job(test_schedule2, 'cron', day_of_week='mon-fri', hour='8', minute='50-59')
    schedule_temp.add_job(reset_values, 'cron', day_of_week='mon-fri', hour='15', minute='41-59')
    schedule_temp.add_job(reset_values, 'cron', day_of_week='mon-fri', hour='16-23', minute='*')
    schedule_temp.add_job(reset_values, 'cron', day_of_week='mon-fri', hour='0-8', minute='*')
    schedule_temp.start()


async def get_bought_dates(account: AccountData, session: aiohttp.ClientSession):
    api_url = f"https://kb.iruda.io/kb/v1/accounts/{account.account_number}/bond"
    params = {
        "userNumber": account.csNo,
        "userPinCode": account.pinNo
    }

    async with session.get(api_url, params=params) as response:
        if response.status == 200:
            response_data = await response.json()
            if response_data["succeeded"]:
                bought_dates_response = response_data["result"]["stockBalances"]

                return bought_dates_response
        else:
            logging.info(f"Error: failed to fetch recent order status for account_number: {account.account_number}")


def parse_bought_dates_response(bought_dates_response):
    sell_infos = []
    for bought_date_response in bought_dates_response:
        bought_date = bought_date_response["boughtDate"]
        quantity = bought_date_response["quantity"]
        security_code = bought_date_response["securityCode"]
        sell_info = {"bought_date": bought_date, "quantity": quantity, "security_code": security_code}
        sell_infos.append(sell_info)

    return sell_infos


async def test_sell_logic():
    account_list = []
    account1 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901',
                           decimal.Decimal('4.00'), '20230426')  # LIZ
    account2 = AccountData('1007633319', 'H001105129993', 18001, 2, '36540453101', decimal.Decimal('2.80'),
                           '20230426')  # MINO
    account_list.append(account1)
    account_list.append(account2)

    order_list = []
    order_data1 = ('36540447901', 'KR6150351D32', 10200, 200, 'SELL', None, '2023-04-19 14:32:44')
    order_data2 = ('36540453101', 'KR6150351D32', 10200, 200, 'SELL', None, '2023-04-19 14:32:44')
    order_list.append(order_data1)
    order_list.append(order_data2)

    async with aiohttp.ClientSession() as session:
        for data in order_list:
            try:
                account_number, security_code, price, quantity, order_type, bought_date, created_at = data
                account = next(account for account in account_list if account.account_number == account_number)
                if order_type == 'BUY':  # 매수이면 body에 boughtDate param이 없어도 상관x
                    return
                    # await self.fetch_order_api(account, security_code, quantity, price,
                    #                            order_type, session, bought_date)
                if order_type == 'SELL':  # 매도이면 채권 매수한 날짜 boughtDate에 넣어야 함
                    bought_dates_response = await get_bought_dates(account, session)
                    sell_infos = parse_bought_dates_response(bought_dates_response)  # bought dates as a list
                    print(f"account_number is: {account.account_number}")
                    for sell_info in sell_infos:
                        if sell_info["security_code"] == security_code:
                            print(sell_info["quantity"])
                            print(sell_info["bought_date"])
            except Exception as e:
                print(f"Error: {e}")


async def main():
    # bond_orderbook = BondOrderBook()
    # order_log_list, account_number_list = select_recent_order_log_data(bond_orderbook)

    # order_log_list = \
    #  [('36648099801', 'KR6079161BC0', 1, '0080090285', datetime.datetime(2023, 4, 28, 10, 0, 48)),
    #  ('36650211401', 'KR6079161BC0', 1, '0020097621', datetime.datetime(2023, 4, 28, 10, 0, 48))]
    #
    # # account_number_list = ['36648099801', '36650211401', '36646117601', '36646486501']
    # # account_list = set_account_information_for_modify(account_number_list)
    #
    # account_list = \
    #     {'36648099801': {'accountNumber': '36648099801', 'csNo': '1001434125', 'pinNo': 'H000637580699'},
    #     '36650211401': {'accountNumber': '36650211401', 'csNo': '1009779409', 'pinNo': 'H003431764691'}}
    #
    # # =============================================================================

    # bond_balances = await fetch_all_recent_bond_order_status(order_log_list, account_list)
    # print(bond_balances)

    # bond_balances = {
    # '36648394101': [[]],
    # '36651552901': [[{'originOrderNumber': '0000000000', 'orderNumber': '0040088900', 'securityCode': 'B079161BC',
    #                  'securityNameInKorean': '씨제이 씨지브이신종','transactionTypeName': '매수', 'orderTypeName': '채권매수','quantity': 10040,
    #                  'totalFilledQuantity': 0, 'unfilledQuantity': 1029, 'price': 9930, 'filledPrice': 0}]]
    # }

    # modify_information_for_accounts = bond_orderbook.parse_bond_balance_data(bond_balances, order_log_list)
    # modify_information_for_accounts = {
    #     '36648099801': [], '36650211401': [], '36646117601': [], '36646486501': [], '36645470501': [],
    #     '36648394101': [],
    #     '36651552901': [
    #         {'origin_order_number': '0000000000', 'order_number': '0040088900', 'security_code': 'KR6079161BC0',
    #          'unfilled_quantity': 1029}],
    #     '36623638301': [
    #         {'origin_order_number': '0000000000', 'order_number': '0060088290', 'security_code': 'KR6079161BC0',
    #          'unfilled_quantity': 7015}],
    #     '36646235601': [
    #         {'origin_order_number': '0000000000', 'order_number': '0000088334', 'security_code': 'KR6079161BC0',
    #          'unfilled_quantity': 5020}]
    # }
    # print(modify_information_for_accounts)
    # test()

    # etf_orderbook = EtfOrderBook()
    # account_list, matching_securities = await test_order_book_for_selling_securities(bond_orderbook)
    # order_book = order_book_for_selling_securities(matching_securities)
    # generate_sell_csv_file(account_list, order_book)
    # test_created_at()
    # await test_save_order_response(etf_orderbook)
    # test2()
    # test_scheduling()
    # contents = json.dumps(
    #     {"KR6079161BC0": {"quantity": 441, "security_name": "씨제이 씨지브이신종자본증권 33", "buy_total_price": 437516.1},
    #      "KR6150351D32": {"quantity": 4592, "security_name": "삼척블루파워8", "buy_total_price": 4619552.0}})
    # print(contents[0])
    await test_sell_logic()


if __name__ == "__main__":
    # print(order_log_list)
    # print(account_number)
    # print(select_account_list())
    # check_todays_date_and_timestamp()
    # select_recent_order_number_and_quantity()
    # fetch_recent_order_status_api_for_accounts()
    # str_to_date()
    nest_asyncio.apply()
    asyncio.run(main())

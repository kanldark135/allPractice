import decimal

import aiohttp
import asyncio
import query
from database import Database
from dto import *


async def process_order_result():
    account_list = set_account_information()

    def should_execute_check_security_codes_to_sell(account: AccountData):
        return account.started_date is not None

    account_list = [account for account in account_list
                    if should_execute_check_security_codes_to_sell(account)]

    account_list_orders = set_today_order_account_list(account_list)

    max_retries = 5
    retry_delay = 1
    results = None

    for attempt in range(1, max_retries + 1):
        try:
            results = await check_order_balance_for_all_accounts(account_list_orders)  # 사용자별 잔고조회(가용금액, 평가금액, 채권, etf)
            #results = await check_order_balance_for_all_accounts(account_list)  # 사용자별 잔고조회(가용금액, 평가금액, 채권, etf)
            break
        except Exception as e:
            print(f"Attempt {attempt} to call check_order_balance_for_all_accounts() failed with error: {e}")
            if attempt < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                print("Maximum retries reached, aborting process_bond_orderbook.")
                return

    format_json(results)

def get_adjusted_principal():
    db = Database()
    db.connect_db()
    db.cur.execute(query.get_adjusted_principal())
    rows = db.cur.fetchall()   # {stock_account_id : adjusted_principal)
    db.disconnect_db()

    return {row[1]: row[0] for row in rows}


def set_account_information():
    """
    DB에 있는 사용자 정보를 불러와서 사용자 정보(*AccountData*) 객체 생성
    :return: account_list 사용자 정보 객체 리스트
    """
    account_list = []

    try:
        db = Database()
        db.connect_db()
        db.cur.execute(query.select_account_info())
        rows = db.cur.fetchall()
        for row in rows:  # 고객별 first_operation_started_date => row[6]
            account = AccountData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
            account_list.append(account)
    except Exception as e:
        print(f"failed to set account information: {e}")

    return account_list


def set_today_order_account_list(account_list):
    query = "select book.account_number from iruda_trade.order_book AS book " \
            "JOIN (" \
            "select MAX(created_at) AS latest_created_at " \
            "from iruda_trade.order_book" \
            ")AS latest_orders " \
            "ON book.created_at = latest_created_at;"

    account_list_orders = []

    db = Database()
    db.connect_db()
    db.cur.execute(query)
    rows = db.cur.fetchall()
    for row in rows:
        account_number = row[0]
        for account in account_list:
            if account.account_number == account_number:
                account_list_orders.append(account)

    return account_list_orders


# TODO 잔고조회(가용금액, 평가금액, 채권, etf 모두 조회 가능)
async def check_order_balance_for_account(account: AccountData, session: aiohttp.ClientSession):
    try:
        api_url = f"https://kb.iruda.io/kb/v1/accounts/{account.account_number}"
        params = {
            "userNumber": account.csNo,
            "userPinCode": account.pinNo,
        }
        async with session.get(api_url, params=params) as response:
            if response.status == 200:
                response_data = await response.json()
                if response_data["succeeded"]:
                    order_balance = response_data["result"]

                    return order_balance
    except Exception as e:
        print("잔고 조회 실패")


async def check_order_balance_for_all_accounts(account_list_orders):
    async def check_order_balance(account, adjusted_principals):
        async with aiohttp.ClientSession() as session:
            for stock_account_id, adjusted_principal in adjusted_principals.items():
                if str(stock_account_id) == str(account.account_id):
                    corresponding_adjusted_principal = adjusted_principal
                    order_balance = await check_order_balance_for_account(account, session)

                    order_balance["cashBalances"][0]["depositOfAfterTwoDays"] = float(order_balance["cashBalances"][0]["depositOfAfterTwoDays"] \
                                                                                - (corresponding_adjusted_principal * decimal.Decimal(0.001)))
                                                                          # 수수료(원금*0.001) 고려한 D+2
                    order_balance["cashBalances"][0]["possibleWithdrawalAmount"] = float(order_balance["cashBalances"][0][
                                                                                "possibleWithdrawalAmount"] \
                                                                            - (corresponding_adjusted_principal * decimal.Decimal('0.001')))  # 수수료(원금*0.001) 고려한 실제 인출가능금액
                    return {'account_number': account.account_number, 'order_balance': order_balance["stockBalances"],
                            'cash_balance': order_balance["cashBalances"]}

    adjusted_principals = get_adjusted_principal()
    tasks = [check_order_balance(account, adjusted_principals) for account in account_list_orders]
    results = await asyncio.gather(*tasks)

    return results
# async def check_order_balance_for_all_accounts(account_list_orders):
#     async def check_order_balance(account):
#         async with aiohttp.ClientSession() as session:
#             order_balance = await check_order_balance_for_account(account, session)
#             return {'account_number': account.account_number, 'order_balance': order_balance["stockBalances"],
#                     'cash_balance': order_balance["cashBalances"]}
#
#     tasks = [check_order_balance(account) for account in account_list_orders]
#     results = await asyncio.gather(*tasks)
#
#     return results


def format_json(results):
    for account in results:
        print(f"Account Number: {account['account_number']}")
        for cash in account['cash_balance']:
            print(f"  depositOfNextDay: {cash['depositOfNextDay']}")
            print(f"  depositOfAfterTwoDays: {cash['depositOfAfterTwoDays']}")
            print(f"  amount: {cash['amount']}")
            print(f"  possibleBuyAmount: {cash['possibleBuyAmount']}")
            print(f"  possibleWithdrawalAmount: {cash['possibleWithdrawalAmount']}")
        print("\n")

        for order in account['order_balance']:
            print(f"  Security Code: {order['securityCode']}")
            print(f"  Name: {order['name']}")
            print(f"  Currency: {order['currency']}")
            print(f"  Quantity: {order['quantity']}")
            print(f"  Possible Order Quantity: {order['possibleOrderQuantity']}")
            print(f"  Current Price: {order['currentPrice']}")
            print(f"  Buy Unit Price: {order['buyUnitPrice']}")
            print(f"  Evaluated Amount: {order['evaluatedAmount']}")
            print(f"  Profit Unit Price: {order['profitUnitPrice']}")
            print(f"  Profit Amount: {order['profitAmount']}")
            print(f"  Profit Rate: {order['profitRate']}")
        print("\n")


if __name__ == "__main__":
    asyncio.run(process_order_result())

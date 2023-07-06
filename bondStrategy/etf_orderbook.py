import requests
import json
import time
import logging
from database import Database
from datetime import datetime, timedelta
from datetime import time as tm
import tzlocal
from dto import *
import asyncio
import os
import aiohttp
import query
from dotenv import load_dotenv
from unhandle import GlobalException
from apscheduler.schedulers.asyncio import AsyncIOScheduler


class EtfOrderBook:
    def __init__(self):
        load_dotenv()
        self.endpoint = os.getenv("ENDPOINT")
        self.db = Database()
        self.account_list = None
        self.previous_etf_price = None
        self.etf_price = None
        self.etf_task = False
        self.is_running = None
        self.is_first_run = True
        self.order_balance_for_accounts = None  # 매도 고객 제외 계좌별 잔고
        self.new_data_event = asyncio.Event()
        self.condition = asyncio.Event()  # orderbook 형성되기도 전에 cron에 의해 process_etf_order_book 실행 방지
        self.semaphore = asyncio.Semaphore(1)   # 동시성 문제 방지(start_order, cron에 의해 process_etf_order_book 실행 동시에 가능)

        self.scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))    # Asia/Seoul
        #정각마다 가격 데이터 새로 가져와서 실행(지정된 시간! => async 사용했더니 밀림)
        self.scheduler.add_job(self.process_etf_order_book, 'cron', day_of_week='mon-fri', hour='10-15',
                               minute='0', max_instances=20)
        self.scheduler.add_job(self.reset_values, 'cron', day_of_week='mon-fri', hour='15', minute='40')
        self.scheduler.add_job(self.reset_values, 'cron', day_of_week='mon-fri', hour='0', minute='0')
        self.scheduler.start()

    async def reset_values(self):
        logging.info("etf_order_book reset")
        self.account_list = None
        self.etf_price = None
        self.previous_etf_price = None
        self.order_balance_for_accounts = None
        self.etf_task = False
        self.is_first_run = True

    # TODO KBSTAR 단기종합채권(AA-이상) 액티브(385550) => 1시간마다 돌아갈 수 있도록
    async def process_etf_order_book(self):
        async with self.semaphore:
            if self.is_first_run:
                await self.condition.wait()
                self.condition.clear()

                # ETF orderbook logic starts
                logging.info("process_etf_order_book started")
                etf_order_data_list = self.get_recent_etf_order_book()  # 저장한 etf quantity 가져오기
                if not etf_order_data_list:    # 오늘자 etf 오더북 없음
                    return

                else:
                    self.etf_price = await self.fetch_etf_price()  # fetch ETF price from Naver API
                    if self.etf_price != self.previous_etf_price:
                        self.previous_etf_price = self.etf_price

                    self.set_account_information()  # initialize self.account_list

                    def should_execute_check_security_codes_to_sell(account: AccountData):
                        return account.started_date is not None

                    self.account_list = [account for account in self.account_list
                                         if should_execute_check_security_codes_to_sell(account)]

                    etf_order_data_list = self.get_recent_etf_order_book()  # 저장한 etf quantity 가져오기
                    etf_data_for_accounts = self.get_pk(
                        etf_order_data_list)  # get PK for each account and make it as key
                    await self.fetch_order_api_for_all_accounts(etf_data_for_accounts)

                self.is_first_run = False
                self.etf_task = True

            # 호가 데이터가 변했을 때만 아래의 로직 실행
            else:
                self.etf_price = await self.fetch_etf_price()
                if self.previous_etf_price != self.etf_price:
                    self.previous_etf_price = self.etf_price
                    logging.info("etf price changed. etf_order_book re-processed")

                    # DB iruda_trade.etf_order_book_log fetch(orderNumber, quantity)
                    recent_order_data = self.select_recent_order_number_and_quantity()

                    if not recent_order_data:   # 오늘 나간 주문 없음
                        logging.info("etf 가격은 변했으나 오늘 나간 주문이 없었으므로 etf 정정주문 생략")
                        return

                    else:   # 실시간 채권 체결 조회 API 호출
                        account_data_and_responses = await self.fetch_recent_order_status_api_for_accounts(
                            recent_order_data)

                        # orderNumber에 해당하는 unfilledQuantity 있나 확인 후 정정주문
                        await self.fetch_modify_api_for_unfilled_quantity(account_data_and_responses)

    def set_account_information(self):
        """
        DB에 있는 사용자 정보를 불러와서 사용자 정보(*AccountData*) 객체 생성
        :return: account_list 사용자 정보 객체 리스트
        """
        account_list = []
        # account1 = AccountData('1004295416', 'H002750061297', 18000, 1, '36540447901',
        #                        decimal.Decimal('4.00'), 'ACTIVE', '2023-04-19 14:32:44')  # LIZ
        # account2 = AccountData('1007633319', 'H001105129993', 18001, 2, '36540453101', decimal.Decimal('2.80'), 'ACTIVE')   #MINO
        # account_list.append(account1)
        # account_list.append(account2)

        try:
            self.db.connect_db()
            self.db.cur.execute(query.select_account_info())
            rows = self.db.cur.fetchall()
            for row in rows:  # 고객별 first_operation_started_date => row[6]
                account = AccountData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
                account_list.append(account)
        except Exception as e:
            raise GlobalException(f"Error: fetch_account_information: {e}")
        finally:
            self.db.disconnect_db()

        self.account_list = account_list

    async def fetch_etf_price(self) -> int:
        url = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:385550"
        response = requests.get(url)
        data = json.loads(response.text)
        current_price = data["result"]["areas"][0]["datas"][0]["nv"]
        logging.info(f"현재가 {current_price}")

        return current_price

    def get_recent_etf_order_book(self):
        etf_order_data_list = []
        # 오늘 나간 주문에 한해서 정정주문 할 것이므로 created_at이 오늘인지 확인(전날 MAX(created_at) 가져오지 않도록 방지)
        today = datetime.today().strftime('%Y-%m-%d')

        try:
            self.db.connect_db()
            self.db.cur.execute(query.select_recent_etf_data())
            rows = self.db.cur.fetchall()
            for row in rows:
                if today == row[3]:
                    etf_order_data_list.append({"account_number": row[0], "quantity": row[2]})
            if not etf_order_data_list:
                logging.info("오늘 주문나가야 할 etf 수량 없음")
        except Exception as e:
            raise GlobalException(f"Error: failed to get recent etf order book from the database: {e}")
        finally:
            self.db.disconnect_db()

        return etf_order_data_list

    # Get PK to send the API call(use pk as managementId)
    def get_pk(self, etf_order_data_list):
        try:
            self.db.connect_db()
            for account_data in etf_order_data_list:
                account_number = account_data["account_number"]
                self.db.cur.execute(query.select_primary_key(),
                                    (account_number, datetime.now().strftime("%Y-%m-%d")))
                result = self.db.cur.fetchone()
                if result:
                    pk = result[0]
                    account_data["pk"] = pk

            return etf_order_data_list

        except Exception as e:
            raise GlobalException(f"Error: pk not exists: {e}")
        finally:
            self.db.disconnect_db()

    # 하나의 계좌에 대해 call API(BUY)
    async def fetch_order_api(self, account, account_data, session: aiohttp.ClientSession):
        if account_data["quantity"] > 0:
            account_number = account_data["account_number"]

            api_url = f"{self.endpoint}/kb/v1/accounts/{account_number}/orders"
            headers = {"Content-Type": "application/json"}
            params = {
                "price": self.etf_price,
                "quantity": account_data["quantity"],
                "securityCode": "385550",
                "orderPriceType": "LIMIT",
                "marketTimeType": "REGULAR",
                "userNumber": account.csNo,
                "userPinCode": account.pinNo,
                "orderType": "BUY",
                "managementId": account_data["pk"]
            }

            async with session.post(api_url, data=json.dumps(params, default=str), headers=headers) as response:
                if response.status == 200:
                    response_data = await response.json()
                    if response_data["succeeded"]:
                        buy_order_response = response_data["result"]

                        account_number = buy_order_response["accountNumber"]
                        succeeded = True
                        management_id = account_data["pk"]
                        order_number = buy_order_response["orderNumber"]
                        parent_order_number = buy_order_response["parentOrderNumber"]
                        security_code = buy_order_response["securityCode"]
                        quantity = buy_order_response["quantity"]
                        price = buy_order_response["price"]

                        await self.save_order_response(account_number, succeeded, management_id,
                                                       order_number, parent_order_number, security_code,
                                                       quantity, price, '정상적으로 매수주문 완료되었습니다.')

                else:
                    response_data = await response.json()

                    # account_number = account_data["account_number"]
                    succeeded = False
                    management_id = account_data["pk"]
                    security_code = "385550"
                    message = response_data["error"]["exchangeMessage"]

                    # await self.save_order_response(account_number, succeeded, management_id,
                    #                                security_code=security_code, message=message)
                    await self.save_order_response(account_number, succeeded, management_id,
                                                   None, None, security_code, None, None, message)

                    logging.error(f"Error: order failed for account_number: {account_number}")

    # 모든 계좌에 대해 API(BUY) 비동기로 call
    async def fetch_order_api_for_all_accounts(self, etf_data_for_accounts):
        try:
            fetch_tasks = []

            async with aiohttp.ClientSession() as session:
                for account_data in etf_data_for_accounts:
                    if account_data["quantity"] > 0:
                        for acc in self.account_list:
                            if acc.account_number == account_data["account_number"]:
                                fetch_tasks.append(self.fetch_order_api(acc, account_data, session))
                                break
                await asyncio.gather(*fetch_tasks)

        except Exception as e:
            logging.error(f"fetch_order_api_for_all_accounts 실패: {e}")

    # API(BUY)에 대한 응답값 기록(Etf order response 저장)
    async def save_order_response(self, account_number, succeeded, management_id, order_number,
                                  parent_order_number, security_code, quantity, price, message=None):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.insert_etf_order_response(),
                                (account_number, succeeded, management_id, order_number,
                                 parent_order_number, security_code, quantity, price, message))
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: failed to insert bond buy order_log: {e}")
        finally:
            self.db.disconnect_db()

    # fetch the most recent order_number, quantity(today's date으로 골라오는 로직 추가)
    def select_recent_order_number_and_quantity(self):
        order_data_list = []
        # 오늘 나간 주문에 한해서 정정주문 할 것이므로 created_at이 오늘인지 확인(전날 MAX(created_at) 가져오지 않도록 방지)
        today = datetime.today().strftime('%Y-%m-%d')

        try:
            self.db.connect_db()
            self.db.cur.execute(query.select_order_number_and_quantity())
            rows = self.db.cur.fetchall()
            for row in rows:
                if today in row[3].strftime('%Y-%m-%d'):
                    order_data_list.append({"account_number": row[0], "order_number": row[1],
                                            "quantity": row[2], "succeeded": row[4]})
                else:
                    logging.info("today's etf order not exists")
        except Exception as e:
            raise GlobalException(f"Error: failed to fetch order data from the database: {e}")
        finally:
            self.db.disconnect_db()

        return order_data_list

    # 실시간 체결 주문 조회
    async def fetch_recent_order_status_api(self, account: AccountData, session: aiohttp.ClientSession):
        api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}/orders"
        params = {
            "userNumber": account.csNo,
            "userPinCode": account.pinNo,
            "orderStatus": "ALL",
            "orderType": "ALL",
            "orderBy": "DESC",
            "date": datetime.today().strftime("%Y%m%d")
        }

        async with session.get(api_url, params=params) as response:
            if response.status == 200:
                response_data = await response.json()
                if response_data["succeeded"]:
                    recent_order_response = response_data["result"]["orderHistories"]

                    return recent_order_response
            else:
                logging.info(f"Error: failed to fetch recent order status for account_number: {account.account_number}")

    # 모든 계좌에 대해 실시간 주문 체결 조회
    async def fetch_recent_order_status_api_for_accounts(self, recent_order_data):
        try:
            fetch_tasks = []

            async with aiohttp.ClientSession() as session:
                for account_data in recent_order_data:
                    if account_data["succeeded"]:  # 첫번째 주문을 성공했을시에만 정정주문 call
                        account_number = account_data["account_number"]
                        matching_account = next(
                            (acc for acc in self.account_list if acc.account_number == account_number),
                            None)

                        if matching_account:
                            fetch_tasks.append(self.fetch_recent_order_status_api(matching_account, session))

                    recent_order_responses = await asyncio.gather(*fetch_tasks)

                # Pair account_data with the corresponding response
                account_data_and_responses = []
                for account_data, recent_order_response in zip(recent_order_data, recent_order_responses):
                    account_data_and_responses.append(
                        {"account_data": account_data, "recent_order_response": recent_order_response})

                return account_data_and_responses

        except Exception as e:
            raise GlobalException(f"Error: failed to fetch recent order status for all accounts: {e}")

    # Response data와 recent_order_data에서 매칭되는 orderNumber 찾아서 unfilled_quantity 확인
    async def fetch_modify_api_for_unfilled_quantity(self, account_data_and_responses):
        try:
            if not account_data_and_responses:
                logging.info("Skipping fetch modify API due to missing account_data_and_responses.")
                return

            async with aiohttp.ClientSession() as session:
                modify_tasks = []

                for account_and_response in account_data_and_responses:
                    account_data = account_and_response["account_data"]
                    recent_order_response = account_and_response["recent_order_response"]

                    if recent_order_response is None:
                        logging.info(
                            f"No recent order response for account number {account_data['account_number']}. Skipping.")
                        return

                    for order_history in recent_order_response:
                        if order_history["orderNumber"] == account_data["order_number"]:
                            unfilled_quantity = order_history["quantity"] - order_history["filledQuantity"]
                            logging.info(
                                f"Unfilled quantity for order number {account_data['order_number']}: {unfilled_quantity}")

                            if unfilled_quantity > 0:
                                modify_tasks.append(self.fetch_modify_order_api(account_data, session))
                            break

                await asyncio.gather(*modify_tasks)

        except Exception as e:
            raise GlobalException(f"Error: failed to fetch modify API for unfilled quantity: {e}")

    # 정정 주문 API
    async def fetch_modify_order_api(self, account_data, session: aiohttp.ClientSession):
        try:
            account_number = account_data["account_number"]
            order_number = account_data["order_number"]

            matching_account = next((acc for acc in self.account_list if acc.account_number == account_number), None)

            api_url = f"{self.endpoint}/kb/v1/accounts/{account_number}/orders/385550/{order_number}"
            headers = {"Content-Type": "application/json"}
            params = {
                "userNumber": matching_account.csNo,
                "userPinCode": matching_account.pinNo,
                "price": self.etf_price
            }

            async with session.put(api_url, params=params, headers=headers) as response:
                if response.status == 200:
                    response_data = await response.json()
                    if response_data["succeeded"]:
                        modify_response = response_data["result"]

                        account_number = modify_response["accountNumber"]
                        succeeded = True
                        recent_order_number = modify_response["orderNumber"]
                        parent_order_number = modify_response["parentOrderNumber"]
                        security_code = modify_response["securityCode"]
                        quantity = modify_response["quantity"]
                        price = modify_response["price"]
                        message = "정상적으로 정정주문 완료되었습니다."

                        pk = self.get_recent_management_id(account_number, parent_order_number)

                        await self.save_modify_order_response(account_number, succeeded, pk, recent_order_number,
                                                              parent_order_number,
                                                              security_code, quantity, price, message)

                else:
                    response_data = await response.json()
                    succeeded = False
                    message = response_data["error"]["exchangeMessage"]

                    pk = self.get_recent_management_id(account_number, order_number)

                    await self.save_modify_order_response(account_number, succeeded, pk, None, None, '385550',
                                                          None, None, message)

                    logging.info(
                        f"Error: order failed for account_number: {matching_account.account_number}")

        except Exception as e:
            raise GlobalException(f"Error: failed to modify order: {e}")

    # modify order response의 order_number가 가장 최근 주문 데이터의 parent_order_number
    def get_recent_management_id(self, account_number, parent_order_number):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.get_recent_management_id(), (account_number, parent_order_number))
            result = self.db.cur.fetchone()
            if result:
                pk = result[0]
                return pk
            else:
                logging.info(
                    f"No matching management_id found for account_number: {account_number} and parent_order_number: {parent_order_number}")
                return None
        except Exception as e:
            logging.error(f"Error: get_management_id: {e}")
        finally:
            self.db.disconnect_db()

    async def save_modify_order_response(self, account_number, succeeded, management_id, order_number,
                                         parent_order_number, security_code, quantity, price, message=None):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.insert_etf_order_response(),
                                (account_number, succeeded, management_id, order_number,
                                 parent_order_number, security_code, quantity, price, message))
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: failed to insert bond buy order_log: {e}")
        finally:
            self.db.disconnect_db()

    # def insert_modify_response(self, modify_response_for_accounts):
    #     try:
    #         self.db.connect_db()
    #
    #         for modify_response in modify_response_for_accounts:
    #             account_number = modify_response["accountNumber"]
    #             order_number = modify_response["orderNumber"]
    #             parent_order_number = modify_response["parentOrderNumber"]
    #             security_code = modify_response["securityCode"]
    #             quantity = modify_response["quantity"]
    #             price = modify_response["price"]
    #
    #             # TODO 가장 recent managementId만 골라야 함.
    #             management_id = self.get_recent_management_id(account_number, parent_order_number, self.db)
    #
    #             if management_id is not None:
    #                 self.db.cur.execute(query.insert_modify_order_response(),
    #                                     (account_number, management_id, order_number, parent_order_number,
    #                                      security_code, quantity, price))
    #                 self.db.con.commit()
    #             else:
    #                 print(f"Skipping insert for account_number: {account_number} due to missing management_id")
    #
    #     except Exception as e:
    #         raise GlobalException(f"Error: insert_modify_response: {e}")
    #     finally:
    #         self.db.disconnect_db()

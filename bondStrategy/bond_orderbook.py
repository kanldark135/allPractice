import decimal
import math
import os
import query
import aiohttp
import asyncio
import csv
import json
import logging
import requests
from datetime import datetime
from database import Database
from dotenv import load_dotenv
from unhandle import GlobalException
from dto import *


class BondOrderBook:
    def __init__(self):
        load_dotenv()
        self.endpoint = os.getenv("ENDPOINT")
        self.account_list = None
        self.db = Database()
        self.telegram_datas = None

    # TODO 채권 오더북 생성
    async def process_bond_orderbook(self):
        try:
            self.set_account_information()  # 고객별 상품 시작 일자 포함

            # TODO 운용개시일이 있거나, 운용개시일이 없다면 전체 평가금이 >= 500이어야 진행
            def should_execute_check_security_codes_to_sell(account: AccountData):
                return account.started_date is not None

            self.account_list = [account for account in self.account_list
                                 if should_execute_check_security_codes_to_sell(account)]

            if not self.account_list:
                logging.info("운용개시일이 있거나, 전체 평가액이 500만원 이상인 고객 없음")
                return  # No accounts meet the condition, skip the rest of the process

            max_retries = 5
            retry_delay = 1
            order_balance_for_all_accounts = None

            for attempt in range(1, max_retries + 1):
                try:
                    order_balance_for_all_accounts = await self.check_order_balance_for_all_accounts()  # 사용자별 잔고조회(가용금액, 평가금액, 채권, etf)
                    break
                except Exception as e:
                    logging.error(
                        f"Attempt {attempt} to call check_order_balance_for_all_accounts() failed with error: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                    else:
                        logging.warning("Maximum retries reached, aborting process_bond_orderbook.")
                        return

            security_codes_to_sell = self.check_security_codes_to_sell()  # 한투운 매도종목들 DB에서 조회
            order_balance_for_accounts_to_check = [order_balance for order_balance in order_balance_for_all_accounts if
                                                   order_balance["account"] in self.account_list]

            # {stock_account_id: adjusted_principals}
            adjusted_principal = self.get_adjusted_principal()  # for tc(adjusted_principal * 0.001)
            securities_to_sell_with_account, securities_except_to_sell_with_account = \
                self.filter_securities_to_sell(adjusted_principal, order_balance_for_accounts_to_check,
                                               security_codes_to_sell)  # 매도/매수(보유) 해야될 사용자들 따로 분리

            selling_orderbook = {}

            if securities_to_sell_with_account:
                matching_securities_by_account = {}
                for account_number, securities in securities_to_sell_with_account.items():
                    matching_account = None
                    for account in self.account_list:
                        if account.account_number == account_number:
                            matching_account = account
                            break

                    matching_securities = await self.fetch_bond_balance_info(matching_account, securities)
                    matching_securities_by_account.update(matching_securities)

                # Modify the function order_book_for_selling_securities to accept matching_securities_by_account as a parameter
                selling_orderbook = self.order_book_for_selling_securities(matching_securities_by_account)

            etf_purchase_list = None
            account_amounts = None
            cash = None
            adjusted_amounts_to_buy = {}
            if securities_except_to_sell_with_account:
                cash, account_amounts, adjusted_amounts_to_buy, etf_purchase_list \
                    = await self.order_book_for_buying_securities(securities_except_to_sell_with_account)
                logging.info(f"매수 오더북 생성 완료")
            self.save_order_book(adjusted_amounts_to_buy, selling_orderbook)

            buy_order_book = self.generate_buy_csv_file(cash, account_amounts,
                                                        adjusted_amounts_to_buy,
                                                        etf_purchase_list)  # generate buy csv file
            sell_order_book = self.generate_sell_csv_file(selling_orderbook)  # generate sell csv file

            return buy_order_book, sell_order_book

        except Exception as e:
            logging.error(f"global error in process_bond_order_book: {e}")

    async def process_modify_order(self):
        order_log_list, account_number_list = self.select_recent_order_log_data()

        # 정정주문할 때 필요한 데이터들(csNo, pinNo etc)
        account_information = self.set_account_information_for_modify(account_number_list)
        bond_balanes = \
            await self.fetch_all_recent_bond_order_status(order_log_list, account_information)  # 실시간 체결 API
        modify_information_for_accounts = self.parse_bond_balance_data(bond_balanes, order_log_list)

        # 정정주문 API 호출 및 응답 log 저장
        await self.fetch_modify_api_for_all_accounts(account_information, modify_information_for_accounts)

    def set_account_information(self):
        """
        DB에 있는 사용자 정보를 불러와서 사용자 정보(*AccountData*) 객체 생성
        :return: account_list 사용자 정보 객체 리스트
        """
        account_list = []
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

    # TODO 잔고조회(가용금액, 평가금액, 채권, etf 모두 조회 가능)
    async def check_order_balance_for_account(self, account: AccountData, session: aiohttp.ClientSession):
        try:
            api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}"
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
                else:
                    raise GlobalException(f"Error: API call failed with status code {response.status}")
        except Exception as e:
            raise GlobalException(f"Error: check_unexecuted_sell: {e}")

    async def check_order_balance_for_all_accounts(self):
        async def check_order_balance(account):
            async with aiohttp.ClientSession() as session:
                order_balance = await self.check_order_balance_for_account(account, session)
                return {'account': account, 'order_balance': order_balance}

        tasks = [check_order_balance(account) for account in self.account_list]
        results = await asyncio.gather(*tasks)

        return results

    # TODO 매도종목 DB에서 조회
    def check_security_codes_to_sell(self):
        """
        매도종목 DB에서 조회
        :return:
        """
        security_codes = []
        try:
            self.db.connect_db()
            self.db.cur.execute(query.select_sell_security(), "매도")
            rows = self.db.cur.fetchall()
            for row in rows:
                security_codes.append(row[0])  # TODO row[0] securityCode 데이터 맞는지 확인해보기(KR***)
            return security_codes
        except Exception as e:
            raise GlobalException(f"Error: fetch_account_information: {e}")
        finally:
            self.db.disconnect_db()

    # TODO 미체결 매도종목 있는 사용자 체크(매도해야 할 사용자와 매수해야 할 사용자 구분)
    def filter_securities_to_sell(self, adjusted_principals, order_balance_for_all_accounts, security_codes_to_sell):
        """
        :return:
        securities_to_sell => stockBalances(채권/주식 정보들만)
        securities_to_buy => ["result"] 전체 내용(cashBalances + stockBalances)
        """
        try:
            tc = decimal.Decimal('0.001')   # DNDN tc

            securities_to_sell_with_account = {}
            securities_except_to_sell_with_account = {}

            for order_balance in order_balance_for_all_accounts:
                account_number = order_balance["account"].account_number
                stocks = order_balance["order_balance"]

                securities_to_sell = [stock for stock in stocks["stockBalances"] if
                                      stock["securityCode"] in security_codes_to_sell and stock["evaluatedAmount"] != 0]

                if securities_to_sell:
                    securities_to_sell_with_account[account_number] = securities_to_sell
                else:
                    try:

                        corresponding_account = next(
                            (acc for acc in self.account_list if acc.account_number == account_number),
                            None)
                        corresponding_adjusted_principal = None
                        for stock_account_id, adjusted_principal in adjusted_principals.items():
                            if str(stock_account_id) == str(corresponding_account.account_id):
                                corresponding_adjusted_principal = adjusted_principal
                                break

                        if corresponding_adjusted_principal is None:
                            continue

                    except StopIteration:
                        break

                    cash_balance = stocks["cashBalances"][0]
                    tc_fee = corresponding_adjusted_principal * tc

                    # 수수료(원금 * 0.001) 고려한 D+2/인출가능금액
                    for key in ["depositOfAfterTwoDays", "possibleWithdrawalAmount"]:
                        cash_balance[key] = max(0.0, float(decimal.Decimal(cash_balance[key]) - tc_fee))

                    securities_except_to_sell_with_account[account_number] = stocks  # 보유/매수해야할 종목들 모두 포함

            return securities_to_sell_with_account, securities_except_to_sell_with_account

        except Exception as e:
            raise GlobalException(f"failed to execute filter_securities_to_sell: {e}")

    # 채권잔고만 부르는 API
    async def fetch_bond_balance_info(self, account: AccountData, securities):
        async with aiohttp.ClientSession() as session:
            api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}/bond"
            params = {
                "userNumber": account.csNo,
                "userPinCode": account.pinNo,
            }
            async with session.get(api_url, params=params) as response:
                if response.status == 200:
                    response_data = await response.json()

                    if response_data["succeeded"]:
                        stock_balances = response_data["result"]["stockBalances"]
                        matching_securities = {}

                        for security in securities:  # 팔아야 할 채권 목록(전체 잔고조회하는 API에서 갖고 온 data)
                            security_code = security["securityCode"]

                            for stock_balance in stock_balances:
                                if stock_balance["securityCode"] == security_code:
                                    bought_date = stock_balance["boughtDate"]
                                    quantity = stock_balance["quantity"]
                                    security_name = stock_balance["bondName"]

                                    matching_securities.setdefault(account.account_number, []).append({
                                        "securityCode": security_code,
                                        "securityName": security_name,
                                        "boughtDate": bought_date,
                                        "quantity": quantity
                                    })

                        return matching_securities
                else:
                    raise Exception(f"Error: API call failed with status code {response.status}")

    # TODO 매도 case => 가져온 채권 잔고를 바탕으로 date, quantity를 security_code에 대해 저장하기
    def order_book_for_selling_securities(self, matching_securities_by_account):
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

    async def order_book_for_buying_securities(self, securities_except_to_sell_with_account):
        # 현금(cash - possibleWithdrawlAmount(csv file 생성용)
        cash = {}
        for account_number, balance in securities_except_to_sell_with_account.items():
            possible_buy_amount = balance["cashBalances"][0]["possibleWithdrawalAmount"]  # 실제 인출가능금액
            cash[account_number] = possible_buy_amount

        # 평가액 계산(account.evaluation_balance)
        account_amounts, possible_buy_amount_for_account = \
            self.get_evaluation_amount(securities_except_to_sell_with_account)

        # 투자성향별로 비율 나눠 -> 채권, etf의 평가금액 계산
        self.consider_intent_type()

        # ETF 사야하는 수량, 양 계산
        etf_price, etf_purchase_list = await self.generate_etf_amount_and_quantity(
            securities_except_to_sell_with_account)

        # possible_buy_amount에서 (etf_price * etf_quantity) * 1.001만큼 빼주기
        for etf_purchase_data in etf_purchase_list:
            account_number = etf_purchase_data["account_number"]
            etf_quantity = etf_purchase_data["quantity"]
            etf_cost = etf_quantity * etf_price * 1.001

            original_possible_buy_amount = possible_buy_amount_for_account[account_number]
            updated_possible_buy_amount = original_possible_buy_amount - etf_cost

            if updated_possible_buy_amount < 0 and etf_quantity <= 0:
                possible_buy_amount_for_account[account_number] = original_possible_buy_amount
            else:
                possible_buy_amount_for_account[account_number] = updated_possible_buy_amount

        # 평가금액에서 MP의 종목이 아닌 평가금액은 제외하고 매수해야 하는 채권 종목들 평가금액 차등 계산
        filtered_stocks_with_account = self.filter_stocks_not_in_telegram_buy_state(
            securities_except_to_sell_with_account)
        evaluated_amount_with_account = \
            self.calculate_evaluated_amount_for_buy_status(filtered_stocks_with_account)

        # 매수해야 하는 채권 종목들의 평가금액과 average_evaluated_amount_for_buy_securities 비교하여 +/- 따지기(수수료 고려)
        amounts_to_buy = self.calculate_amount_to_buy(securities_except_to_sell_with_account,
                                                      evaluated_amount_with_account)
        adjusted_amounts_to_buy = self.adjust_quantity_and_amount(amounts_to_buy,
                                                                  possible_buy_amount_for_account)  # adjusted cash pass

        return cash, account_amounts, adjusted_amounts_to_buy, etf_purchase_list

    # Etf Orderbook strategy
    async def generate_etf_amount_and_quantity(self, securities_except_to_sell_with_account):
        etf_price = await self.fetch_etf_price()
        etf_purchase_list = self.calculate_etf_amount_to_buy(etf_price, securities_except_to_sell_with_account)
        self.save_etf_order_book(etf_price, etf_purchase_list)  # save orderbook data to DB

        return etf_price, etf_purchase_list

    # Naver etf 현재가 불러오기
    async def fetch_etf_price(self) -> int:
        url = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:385550"
        response = requests.get(url)
        data = json.loads(response.text)
        current_price = data["result"]["areas"][0]["datas"][0]["nv"]
        logging.info(f"현재가 {current_price}")

        return current_price

    def filter_accounts_by_security_code(self, securities_except_to_sell_with_account, security_code: str = "A385550"):
        try:
            filtered_accounts = []

            for account_number, account_balance in securities_except_to_sell_with_account.items():
                possible_buy_amount = account_balance["cashBalances"][0]["possibleWithdrawalAmount"]
                order_balance = account_balance["stockBalances"]

                securities = []
                for security in order_balance:
                    if security["securityCode"] == security_code and security["evaluatedAmount"] > 0:
                        securities.append(security)

                filtered_accounts.append({"account_number": account_number, "securities": securities,
                                          "possible_buy_amount": possible_buy_amount})

            return filtered_accounts

        except Exception as e:
            logging.error(f"Error in filter accounts by security code: {e}")

    # 사야하는 ETF 금액, 수량 계산
    def calculate_etf_amount_to_buy(self, etf_price, securities_except_to_sell_with_account):
        try:
            etf_purchase_list = []

            for account_data in self.filter_accounts_by_security_code(securities_except_to_sell_with_account):
                account_number = account_data["account_number"]
                securities = account_data["securities"]
                possible_buy_amount = account_data["possible_buy_amount"]

                amount = None
                if securities:
                    # case1) A385550을 갖고 있는 경우, etf_amounts > evaluatedAmount 일때만 구매
                    total_evaluated_amount = sum(security["evaluatedAmount"] for security in securities)
                    for account in self.account_list:
                        if account.account_number == account_number:
                            amount = max(account.etf_amount - total_evaluated_amount, 0)
                            break
                else:
                    for account in self.account_list:
                        if account.account_number == account_number:
                            amount = account.etf_amount  # case2) A385550을 갖고 있지 않다면, etf_amounts만큼 전량 구매
                            break

                # Compare amount with possible_buy_amount and update accordingly
                if amount < possible_buy_amount:
                    buy_amount = amount
                else:
                    buy_amount = possible_buy_amount

                quantity = math.floor(buy_amount / etf_price)
                etf_purchase_list.append({"account_number": account_number, "amount": buy_amount, "quantity": quantity})

            return etf_purchase_list

        except Exception as e:
            logging.error(f"Error in calculating etf_amount and quantity: {e}")

    # save in database(Etf OrderBook)
    def save_etf_order_book(self, etf_price, etf_data_for_accounts):
        """
        account_number, security_name, security_code, etf_amounts, price, amount, quantity, status, created_at
        :param etf_data_for_accounts: etf_purchase_list
        :return:
        """
        try:
            self.db.connect_db()
            etf_amounts = None
            for account_data in etf_data_for_accounts:
                account_number = account_data["account_number"]
                for account in self.account_list:
                    if account.account_number == account_number:
                        etf_amounts = account.etf_amount
                amount = account_data["amount"]
                quantity = account_data["quantity"]
                market_day = datetime.now().strftime("%Y-%m-%d")

                if quantity > 0:
                    self.db.cur.execute(query.insert_etf_order_book(),
                                        (account_number, 'KBSTAR 단기종합채권(AA-이상)액티브', '385550',
                                         etf_amounts, etf_price, amount, quantity, '매수', market_day))
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: insert_etf_order_book_datas: {e}")
        finally:
            self.db.disconnect_db()

    # stock_account_id에 따른 조정원금(수수료 수취 목적)
    def get_adjusted_principal(self):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.get_adjusted_principal())
            rows = self.db.cur.fetchall()  # {stock_account_id : adjusted_principal)

            return {row[1]: row[0] for row in rows}
        except Exception as e:
            raise GlobalException(f"Error: failed to get_adjusted_principals: {e}")
        finally:
            self.db.disconnect_db()

    # 고객별로 갖고 있는 채권들 평가금액 먼저 확인
    def get_evaluation_amount(self, securities_except_to_sell_with_account):

        try:
            account_amounts = {}
            possible_buy_amount_for_account = {}

            for account_number, balance in securities_except_to_sell_with_account.items():
                deposit_after_two_days = balance["cashBalances"][0]["depositOfAfterTwoDays"]  # 현금
                stocks = balance["stockBalances"]
                possible_buy_amount = balance["cashBalances"][0]["possibleWithdrawalAmount"]  # 실제 인출가능금액

                evaluation_amount = 0
                for stock in stocks:
                    evaluation_amount += stock["evaluatedAmount"]  # 주식/채권 평가금액

                for account in self.account_list:
                    if account.account_number == account_number:
                        total_evaluation_amount = deposit_after_two_days + evaluation_amount
                        account_amounts[account_number] = {"total_evaluation_amount": total_evaluation_amount,
                                                           "possible_buy_amount": possible_buy_amount}

                        possible_buy_amount_for_account[account_number] = possible_buy_amount

                        account.evaluation_balance = total_evaluation_amount
                        break

            return account_amounts, possible_buy_amount_for_account

        except Exception as e:
            logging.error(f"failed to get evaluation amount: {e}")

    # 투자성향별로 채권, etf 사야하는 비율 나누기
    def consider_intent_type(self):
        """
        투자 성향별로 비율 나누기
        :param order_balance_for_all_accounts:
        :param account_list: 사용자 객체
        :return: etf_amounts etf 구매비율
        """
        try:
            for account in self.account_list:
                bond_ratio = 1
                etf_ratio = 0
                if account.risk_score == 4:
                    bond_ratio = 0.7
                    etf_ratio = 0.3
                elif account.risk_score == 3:
                    bond_ratio = 0.9
                    etf_ratio = 0.1

                if account.evaluation_balance:
                    bond_amount = account.evaluation_balance * bond_ratio
                    etf_amount = account.evaluation_balance * etf_ratio
                    account.evaluation_balance = bond_amount
                    account.etf_amount = etf_amount

        except Exception as e:
            logging.error(f"failed to consider intent type: {e}")

    # 채권 평가금액에서 MP의 종목이 아닌 평가금액 제외
    def filter_stocks_not_in_telegram_buy_state(self, securities_except_to_sell_with_account):
        try:
            filtered_stocks_with_account = {}

            for account_number, stocks in securities_except_to_sell_with_account.items():
                filtered_stocks = []
                total_evaluated_amount = 0
                current_account = None

                for account in self.account_list:
                    if account.account_number == account_number:
                        total_evaluated_amount = account.evaluation_balance
                        current_account = account
                        break

                stock_balances = stocks["stockBalances"]

                for stock in stock_balances:
                    security_code = stock["securityCode"]
                    is_stock_in_telegram_data = False

                    for telegram_data in self.telegram_datas:
                        if telegram_data.security_code == security_code:
                            is_stock_in_telegram_data = True
                            break

                    if not is_stock_in_telegram_data:  # 보유종목
                        filtered_stocks.append(stock)
                        if security_code != "A385550":  # 최신 MP에 없는 종목들의 평가액 빼주기
                            total_evaluated_amount -= stock["evaluatedAmount"]

                            if total_evaluated_amount < 0:  # 평가액이 단시간에 갑자기 오르면 <0 일수도
                                total_evaluated_amount = stocks["cashBalances"][0]["possibleWithdrawalAmount"]

                current_account.total_evaluation_balance = total_evaluated_amount

                filtered_stocks_with_account[account_number] = {
                    "filtered_stocks": filtered_stocks,  # MP에 없는 종목들(보유)
                    "total_evaluated_amount": total_evaluated_amount
                }

            return filtered_stocks_with_account

        except Exception as e:
            logging.error(f"failed to filter stocks not in telegram buy state: {e}")

    # total_evaluated_amount를 매수해야 할 종목들의 개수로 비율 차등 부과
    def calculate_evaluated_amount_for_buy_status(self, filtered_stocks_with_account):
        try:
            evaluated_amount_with_account = {}
            total_ratio = sum(
                telegram_data.ratio for telegram_data in self.telegram_datas if telegram_data.status == "매수")

            for account_number, data in filtered_stocks_with_account.items():
                total_evaluated_amount = float(data["total_evaluated_amount"])
                evaluated_amount_for_security = []

                for telegram_data in self.telegram_datas:
                    if telegram_data.status == '매수':  # status bond_portfolio에 '매수', '채권매수' 확인
                        security_code = telegram_data.security_code
                        security_name = telegram_data.security_name
                        evaluated_amount = (float(telegram_data.ratio) / float(
                            total_ratio)) * total_evaluated_amount  # 사야하는 금액
                        evaluated_amount_for_security.append({"security_code": security_code,
                                                              "security_name": security_name,
                                                              "evaluated_amount": evaluated_amount})
                evaluated_amount_with_account[account_number] = evaluated_amount_for_security

            return evaluated_amount_with_account
        except Exception as e:
            logging.error(f"failed to calculate average evaluated amount for buy status: {e}")
            raise GlobalException(f"failed to calculate average evaluated amount for buy status: {e}")

    # 매수해야 하는 채권 종목들의 평가금액과 evaluated_amount_for_buy_securities 비교하여 +/- 따지기
    def calculate_amount_to_buy(self, securities_except_to_sell_with_account, evaluated_amount_with_account):
        try:
            amounts_to_buy = {}
            tc = 0.003  # 수수료 0.3%

            for account_number, stocks in securities_except_to_sell_with_account.items():
                amounts_to_buy[account_number] = {}

                # 잔고에 같은 보유 종목코드 있을 경우, evaluationAmount의 sum 구하기
                stock_balances = stocks["stockBalances"]  # hv 매수/보유 종목
                stock_balances_dict = {}
                for stock in stock_balances:
                    security_code = stock["securityCode"]
                    if security_code not in stock_balances_dict:
                        stock_balances_dict[security_code] = stock
                    else:
                        stock_balances_dict[security_code]["evaluatedAmount"] += stock["evaluatedAmount"]

                for telegram_data in self.telegram_datas:
                    security_code = telegram_data.security_code
                    security_name = telegram_data.security_name
                    status = telegram_data.status
                    price = telegram_data.price

                    if status == '매수':  # DB status '채권매수'인지 '매수'인지 check
                        evaluated_amount_for_security = None
                        for security in evaluated_amount_with_account[account_number]:
                            if security["security_code"] == security_code:
                                evaluated_amount_for_security = security["evaluated_amount"]
                                break

                        if evaluated_amount_for_security is not None:
                            if security_code not in stock_balances_dict:
                                # If the security_code is not in the account balance, store the amount to buy
                                amounts = evaluated_amount_for_security * (1 - tc)
                                quantity_to_buy = math.floor(amounts / (price / float(10)))
                                amounts_to_buy[account_number][security_code] = {
                                    "securityName": security_name,
                                    "amounts_to_buy": amounts,
                                    "quantity_to_buy": quantity_to_buy,
                                    "price": price,
                                }

                            else:
                                # If the security_code is in the account balance, compare the evaluated amounts
                                current_evaluated_amount = stock_balances_dict[security_code]["evaluatedAmount"]

                                if evaluated_amount_for_security > current_evaluated_amount:
                                    amounts = (evaluated_amount_for_security - current_evaluated_amount) * (1 - tc)
                                    quantity_to_buy = math.floor(amounts / (price / float(10)))
                                    amounts_to_buy[account_number][security_code] = {
                                        "securityName": security_name,
                                        "amounts_to_buy": amounts,
                                        "quantity_to_buy": quantity_to_buy,
                                    }
                                else:
                                    # KB 수수료(0.003)
                                    amounts = stocks["cashBalances"][0]["possibleWithdrawalAmount"] * (1 - tc)
                                    quantity_to_buy = math.floor((amounts / (price / float(10))))
                                    amounts_to_buy[account_number][security_code] = {
                                        "securityName": security_name,
                                        "amounts_to_buy": amounts,
                                        "quantity_to_buy": quantity_to_buy
                                    }
            return amounts_to_buy

        except Exception as e:
            logging.error(f"failed to calculate amount to buy: {e}")

    def adjust_quantity_and_amount(self, amounts_to_buy, possible_buy_amount_for_account):
        tc = 0.003  # 수수료 0.3%

        try:
            # Sort the securities by their ratio in descending order
            sorted_telegram_data = sorted(self.telegram_datas, key=lambda x: x.ratio, reverse=True)

            # Create a new dictionary to store the adjusted amounts and quantities to buy
            adjusted_amounts_to_buy = {}

            # Function to adjust the amount and quantity to buy for a security_code
            def adjust_security(account_number, securities, security_code, price):
                nonlocal possible_buy_amount_for_account
                amount_to_buy = min(securities[security_code]["amounts_to_buy"],
                                    possible_buy_amount_for_account[account_number] * (1 - tc))
                quantity_to_buy = math.floor(amount_to_buy / (price / float(10)))

                if account_number not in adjusted_amounts_to_buy:
                    adjusted_amounts_to_buy[account_number] = {}

                adjusted_amounts_to_buy[account_number][security_code] = {
                    "securityName": securities[security_code]["securityName"],
                    "amounts_to_buy": amount_to_buy,
                    "quantity_to_buy": quantity_to_buy,
                    "price": price,
                }

                # update possible_buy_amount
                possible_buy_amount_for_account[account_number] -= amount_to_buy * (1 + tc)

            # First, handle the securities in telegram_data
            for telegram_data in sorted_telegram_data:
                security_code = telegram_data.security_code
                price = telegram_data.price

                for account_number, securities in amounts_to_buy.items():
                    if security_code in securities:
                        adjust_security(account_number, securities, security_code, price)

            # If there's still remaining amount in possible_buy_amount_for_account, allocate it to the security with highest ratio
            for account_number, amount_left in possible_buy_amount_for_account.items():
                if amount_left > 0 and sorted_telegram_data:
                    highest_ratio_security = sorted_telegram_data[0].security_code
                    price = sorted_telegram_data[0].price

                    if highest_ratio_security in amounts_to_buy[account_number]:
                        adjusted_amounts_to_buy[account_number][highest_ratio_security]["amounts_to_buy"] \
                            += amount_left * (1 - tc)
                        adjusted_amounts_to_buy[account_number][highest_ratio_security]["quantity_to_buy"] = \
                            math.floor(adjusted_amounts_to_buy[account_number][highest_ratio_security][
                                           "amounts_to_buy"] / (price / float(10)))

            return adjusted_amounts_to_buy

        except Exception as e:
            logging.error(f"Failed to adjust quantity and amount: {e}")

    def generate_buy_csv_file(self, cash, account_amounts,
                              adjusted_amounts_to_buy, etf_purchase_list, file_name="매수 orderbook.csv"):
        try:
            with open(file_name, mode="w", newline='') as file:
                csv_writer = csv.writer(file)

                # Write buying_orderbook header
                csv_writer.writerow(["매수 오더북"])

                # Find unique security names in the amounts_to_buy dictionary
                buying_security_names = set()
                for account_number, securities in adjusted_amounts_to_buy.items():
                    for security in securities.values():
                        buying_security_names.add(security["securityName"])

                # Write security codes header for buying_orderbook
                csv_writer.writerow(
                    [""] + ["투자성향"] + ["전체평가금액"] + ["전체평가금액*투자성향비율"] + ["현금"] +
                    ["전체평가금액 - MP에 없는 종목 평가금"] + [name for name in buying_security_names for _ in range(2)] +
                    ["Etf amount"] + ["Etf quantity"])

                # Write sub-headers for quantities and amounts
                csv_writer.writerow([""] + [""] + [""] + [""] + [""] + [""] + ["quantity", "amounts to buy"] * len(
                    buying_security_names)
                                    + ["", ""])

                # Write buying_orderbook rows with account numbers, balances, and quantities
                for account_number, securities in adjusted_amounts_to_buy.items():
                    row = [account_number]

                    # 투자성향
                    account_risk_score = next(account.risk_score for account in self.account_list if
                                              account.account_number == account_number)
                    if account_risk_score == 1:
                        row.append('성장추구형')
                    elif account_risk_score == 2:
                        row.append('성장형')
                    elif account_risk_score == 3:
                        row.append('위험중립형')
                    elif account_risk_score == 4:
                        row.append('안정추구형')

                    # 전체평가금액
                    account_total_evaluation_amount = account_amounts[account_number]["total_evaluation_amount"]
                    row.append(account_total_evaluation_amount)

                    # 전체평가금액 * 투자성향별 비율(전체평가금액에서 투자성향 고려한 금액/MP에 없는 종목들을 빼지는 않음)
                    account_evaluation_balance = next(account.evaluation_balance for account in self.account_list if
                                                      account.account_number == account_number)
                    row.append(account_evaluation_balance)

                    # 현금
                    row.append(cash[account_number])

                    # 조정 평가금액(실제 맞춰야하는 평가금: MP에 없는 종목들 제함)
                    adjusted_evaluation_balance = next(
                        account.total_evaluation_balance for account in self.account_list if
                        account.account_number == account_number)
                    row.append(adjusted_evaluation_balance)

                    for security_name in buying_security_names:
                        matching_security = next(
                            (security for security in securities.values() if security["securityName"] == security_name),
                            None)
                        if matching_security:
                            row.extend([matching_security["quantity_to_buy"], matching_security["amounts_to_buy"]])
                        else:
                            row.extend(["", ""])

                    # Add ETF amount and ETF quantity to the row
                    etf_purchase_data = next(
                        (data for data in etf_purchase_list if data["account_number"] == account_number), None)
                    if etf_purchase_data:
                        row.extend([etf_purchase_data["amount"], etf_purchase_data["quantity"]])
                    else:
                        row.extend(["", ""])

                    csv_writer.writerow(row)

            return file_name

        except Exception as e:
            logging.error(f"csv 생성 실패: {e}")

    def generate_sell_csv_file(self, order_book, file_name="매도 orderbook.csv"):
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
                    account_evaluation_balance = next(account.evaluation_balance for account in self.account_list if
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
            logging.error(f"csv 생성 실패: {e}")

    def save_order_book(self, adjusted_amounts_to_buy, selling_order_book):
        """
        account_number, security_code, price, quantity, status
        :return:
        """
        try:
            self.db.connect_db()

            # Sort self.account_list by latest_deposited_at in ascending order(입금순대로 저장될 수 있게끔)
            sorted_account_list = sorted(self.account_list, key=lambda x: x.latest_deposited_at)

            # save buying_orderbook data
            for account in sorted_account_list:
                account_number = account.account_number
                if account_number in adjusted_amounts_to_buy:
                    securities = adjusted_amounts_to_buy[account_number]
                    for security_code, security_info in securities.items():
                        matching_telegram_data = next(telegram_data for telegram_data in self.telegram_datas if
                                                      telegram_data.security_code == security_code)
                        price = matching_telegram_data.price
                        quantity = security_info["quantity_to_buy"]
                        order_type = "BUY"

                        if quantity > 0:
                            self.db.cur.execute(query.insert_order_book(),
                                                (account_number, security_code, price, quantity, order_type, None))

            # save selling_orderbook data
            for account_number, securities in selling_order_book.items():
                for security_code, security_info in securities.items():
                    matching_telegram_data = next(telegram_data for telegram_data in self.telegram_datas if
                                                  telegram_data.security_code == security_code)
                    price = matching_telegram_data.price
                    bought_dates = security_info["boughtDatesAndQuantity"]
                    order_type = "SELL"

                    for bought_date_info in bought_dates:
                        bought_date = bought_date_info["date"]
                        quantity = bought_date_info["quantity"]

                        self.db.cur.execute(query.insert_order_book(),
                                            (account_number, security_code, price, quantity, order_type, bought_date))

            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: insert_bond_order_book_datas: {e}")
        finally:
            self.db.disconnect_db()

    def select_order_data(self):
        order_list = []

        today = datetime.today().strftime('%Y-%m-%d')
        self.db.connect_db()
        try:
            self.db.cur.execute(query.select_order_data())
            rows = self.db.cur.fetchall()
            for row in rows:
                if today in row[6].strftime('%Y-%m-%d'):
                    order_list.append(row)

            if not order_list:
                logging.info("오늘 주문해야하는 bond 수량 없음")

            return order_list
        except Exception as e:
            raise GlobalException(f"Error: fetch_order_data_from_db: {e}")
        finally:
            self.db.disconnect_db()

    # 매수/매도 주문 call
    async def fetch_order_api(self, account: AccountData, security_code, quantity, price, order_type,
                              session: aiohttp.ClientSession, bought_date=None):
        if quantity > 0:
            api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}/orders/bond/LISTED"
            headers = {"Content-Type": "application/json"}
            params = {
                "orderType": order_type,
                "userNumber": account.csNo,
                "userPinCode": account.pinNo,
                "securityCode": security_code,
                "quantity": quantity,
                "price": price,
                "boughtDate": bought_date if bought_date else ""
            }

            async with session.post(api_url, data=json.dumps(params, default=str), headers=headers) as response:
                if response.status == 200:
                    order_response = await response.json()
                    if order_response["succeeded"]:
                        buy_order_response = order_response["result"]
                        message = buy_order_response["message"]
                        order_number = buy_order_response["orderNumber"]
                        succeeded = True

                        await self.save_order_response(account.account_number, security_code, succeeded, message,
                                                       order_number)

                else:
                    order_response = await response.json()
                    message = order_response["error"]["exchangeMessage"]
                    succeeded = False

                    await self.save_order_response(account.account_number, security_code, succeeded, message)
                    logging.info(
                        f"Error: order failed for account_number: {account.account_number}, security_code: {security_code}")

    # 채권 매수한 날짜들 조회하는 api
    async def get_bought_dates(self, account: AccountData, session: aiohttp.ClientSession):
        api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}/bond"
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

    # return the bought dates and corresponding quantity info
    def parse_bought_dates_response(self, bought_dates_response):
        sell_infos = []
        for bought_date_response in bought_dates_response:
            bought_date = bought_date_response["boughtDate"]
            quantity = bought_date_response["quantity"]
            security_code = bought_date_response["security_code"]
            sell_info = {"bought_date": bought_date, "quantity": quantity, "security_code": security_code}
            sell_infos.append(sell_info)

        return sell_infos

    async def execute_orders(self, update):  # db에서 주문지 select
        order_data = self.select_order_data()
        self.set_account_information()  # 입금 빨리 한 사람부터 주문 넣기

        def should_execute_check_security_codes_to_sell(account: AccountData):
            return account.started_date is not None

        self.account_list = [account for account in self.account_list
                             if should_execute_check_security_codes_to_sell(account)]

        if not self.account_list:
            logging.info("운용개시일이 있거나, 전체 평가액이 500만원 이상인 고객 없음")
            return  # No accounts meet the condition, skip the rest of the process

        async with aiohttp.ClientSession() as session:
            for data in order_data:
                try:
                    account_number, security_code, price, quantity, order_type, bought_date, created_at = data
                    account = next(account for account in self.account_list if account.account_number == account_number)
                    if order_type == 'BUY':  # 매수이면 body에 boughtDate param이 없어도 상관x
                        await self.fetch_order_api(account, security_code, quantity, price,
                                                   order_type, session, bought_date)
                    if order_type == 'SELL':  # 매도이면 채권 매수한 날짜 boughtDate에 넣어야 함
                        bought_dates_response = await self.get_bought_dates(account, session)
                        sell_infos = self.parse_bought_dates_response(bought_dates_response)  #bought dates as a list
                        for sell_info in sell_infos:
                            # 채권 잔고에 있는 종목코드가 매도해야 하는 종목코드와 같을 때만 quantity, bought_date으로 API 호출
                            if sell_info["security_code"] == security_code:
                                await self.fetch_order_api(account, security_code, sell_info["quantity"], price,
                                                           order_type, session, sell_info["bought_date"])

                except StopIteration:
                    break

            await update.message.reply_text("주문이 정상적으로 처리되었습니다.")

    # order response db 저장
    async def save_order_response(self, account_number, security_code, succeeded, message, order_number=None):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.insert_bond_order_response(),
                                (account_number, security_code, succeeded, message, order_number))
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: failed to insert bond buy order_log: {e}")
        finally:
            self.db.disconnect_db()

    # 정정주문 / DB order_book_log에서  가져오기(account_number, security_code,
    #  latest_created_at(order_book), succeeded, latest_log_created_at(order_book_log)
    def select_recent_order_log_data(self):
        account_number_list = []
        order_log_list = []

        self.db.connect_db()

        try:
            self.db.cur.execute(query.select_order_data_for_modify())
            rows = self.db.cur.fetchall()
            for row in rows:
                order_log_list.append(row)
                account_number_list.append(row[0])

            if not order_log_list:
                logging.info("오늘 첫번째로 나간 채권 주문 없으므로 정정주문 필요 없음")

            return order_log_list, account_number_list

        except Exception as e:
            logging.error(f"failed to select recent order log data from db: {e}")
        finally:
            self.db.disconnect_db()

    # 정정주문용 account_list set하기
    def set_account_information_for_modify(self, account_number_list):
        account_information = {}

        if account_number_list:
            self.db.connect_db()

            try:
                for account_number in account_number_list:
                    account_information[account_number] = {}
                    account_information[account_number]["accountNumber"] = account_number
                    self.db.cur.execute(query.select_account_info_for_modify(), account_number)
                    rows = self.db.cur.fetchall()
                    for row in rows:
                        account_information[account_number]["csNo"] = row[0]
                        account_information[account_number]["pinNo"] = row[1]

                return account_information  # order API 호출할 때 필요한 정보들

            except Exception as e:
                logging.error(f"정정주문용 account_information set 실패: {e}")
            finally:
                self.db.disconnect_db()

        else:
            logging.info(f"오늘 나간 채권 주문이 없으므로 정정주문할 account 존재하지 않음")

    async def fetch_recent_bond_order_status(self, account, session: aiohttp.ClientSession, next_key=None):
        account_number = account["accountNumber"]

        api_url = f"{self.endpoint}/kb/v1/accounts/{account_number}/orders/bond"
        params = {
            "userNumber": account["csNo"],
            "userPinCode": account["pinNo"],
            "inquiryType": "BOND",
            "date": datetime.today().strftime("%Y%m%d"),
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
                logging.info(
                    f"Error: failed to fetch recent bond order status for account_number: {account.account_number}")

    # 실시간 체결 API 불러오기(채권 잔고 조회 API)
    async def fetch_all_recent_bond_order_status(self, order_log_list, account_information):

        bond_balances = {}
        next_key = None
        processed_accounts = set()

        try:
            async with aiohttp.ClientSession() as session:
                for order_log in order_log_list:
                    account_number = order_log[0]

                    if account_number in processed_accounts:
                        continue

                    bond_balances[account_number] = []

                    security_code = order_log[1]
                    succeeded = order_log[2]

                    if succeeded:  # 주문이 성공했을시에만 채권 실시간 데이터 조회
                        while True:  # nextKey 있을시 다음 페이지 조회
                            bond_balance_data = await self.fetch_recent_bond_order_status \
                                (account_information[account_number], session, next_key)
                            bond_balances[account_number].append(bond_balance_data)

                            if "result" in bond_balance_data and "nextKey" in bond_balance_data["result"]:
                                next_key = bond_balance_data["result"]["nextKey"]
                                if not next_key.strip():
                                    break
                            else:
                                break

                    else:
                        logging.info(f"{account_number}의 첫번째 {security_code}가 실패하였으므로 정정 주문 불가")

                return bond_balances

        except Exception as e:
            logging.error(f"bond_balance fetch failed: {e}")

    # order_number에 해당하는 unfilledQuantity 있나 parsing(bond_balances: 첫번째 주문 성공한 계좌만 실시간 체결 조회)
    def parse_bond_balance_data(self, bond_balances, order_log_list):
        try:
            modify_information_for_accounts = {}
            for order_log in order_log_list:
                account_number = order_log[0]
                security_code = order_log[1]
                order_number = order_log[3]

                if bond_balances[account_number]:  # bond_balances[account_number] 없을 시, 해당 일자에 주문 실패한 것
                    bond_balance = bond_balances[account_number]
                    bonds = [bond for item in bond_balance for bond in item]
                    all_bonds = [
                        {
                            "origin_order_number": bond["originOrderNumber"],
                            "order_number": bond["orderNumber"],
                            "security_code": security_code,
                            "filled_quantity": bond["filledQuantity"],
                            "unfilled_quantity": bond["unfilledQuantity"]
                        } for bond in bonds
                        if bond["orderNumber"] == order_number and bond["unfilledQuantity"] > 0
                    ]
                    modify_information_for_accounts[account_number] = all_bonds

                else:
                    continue

            return modify_information_for_accounts

        except Exception as e:
            logging.error(f"failed to parse bond balance data: {e}")

    # call Modify API
    async def fetch_modify_api(self, account, security_code, unfilled_quantity, filled_quantity, order_number,
                               session: aiohttp.ClientSession):

        # Get new price from telegram datas
        price = None
        correction_type = None
        correction_quantity = None

        for td in self.telegram_datas:
            if td.security_code == security_code:
                price = td.price
            else:
                logging.info("정정주문용 security_code에 해당하는 가격 데이터 없음")

        account_number = account['accountNumber']

        if unfilled_quantity > 0:
            if filled_quantity != 0:  # 부분 미체결인 상태
                correction_type = "PARTIAL"
                correction_quantity = unfilled_quantity
            else:  # 전량 미체결인 상태
                correction_type = "ALL"
                correction_quantity = 0

        api_url = f"{self.endpoint}/kb/v1/accounts/{account_number}/orders/bond/LISTED/{security_code}/{order_number}"
        headers = {"Content-Type": "application/json"}
        params = {
            "userNumber": account["csNo"],
            "userPinCode": account["pinNo"],
            "correctionType": correction_type,
            "correctionQuantity": correction_quantity,
            "correctionPrice": price  # 나중에 받을 가격 데이터로 교체
        }

        async with session.put(api_url, params=params, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                if response_data["succeeded"]:
                    modify_response = response_data["result"]
                    message = modify_response["message"]
                    order_number = modify_response["orderNumber"]
                    succeeded = True

                    await self.save_modify_order_response(account_number, security_code, succeeded, message,
                                                          order_number)

            else:
                response_data = await response.json()
                message = response_data["error"]["exchangeMessage"]
                succeeded = False

                await self.save_modify_order_response(account_number, security_code, succeeded, message, )
                logging.info(
                    f"Error: Modify bond order failed for account_number: {account_number}, "
                    f"security_code: {security_code}")

    async def fetch_modify_api_for_all_accounts(self, account_information, modify_information_for_accounts):
        try:
            async with aiohttp.ClientSession() as session:
                for account_number, bond_balance_response in modify_information_for_accounts.items():
                    account_info = account_information[account_number]
                    if bond_balance_response:
                        for single_response in bond_balance_response:
                            security_code = single_response["security_code"]
                            unfilled_quantity = single_response["unfilled_quantity"]
                            filled_quantity = single_response["filled_quantity"]
                            order_number = single_response["order_number"]
                            # 정정주문도 입금순대로 들어가도록 변경
                            await self.fetch_modify_api(account_info, security_code, unfilled_quantity, filled_quantity,
                                                        order_number, session)

        except Exception as e:
            logging.error(f"채권 정정주문 실패: {e}")

    # order response order_book_log에 저장
    async def save_modify_order_response(self, account_number, security_code, succeeded, message, order_number=None):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.insert_bond_order_response(),
                                (account_number, security_code, succeeded, message, order_number))
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: failed to insert bond buy order_log: {e}")
        finally:
            self.db.disconnect_db()

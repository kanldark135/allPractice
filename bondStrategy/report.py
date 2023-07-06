import decimal
import logging

from dotenv import load_dotenv
from unhandle import GlobalException
from database import Database
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dto import *

import os
import json
import tzlocal
import asyncio
import aiohttp
import pandas as pd
import nest_asyncio


class Report:

    def __init__(self, application):
        load_dotenv()
        self.endpoint = os.getenv("ENDPOINT")
        self.db = Database()
        self.application = application
        self.account_list = None
        self.scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))

        self.scheduler.add_job(self.batch_process_api_call, 'cron', day_of_week='mon-fri', hour='8, 16',
                               minute='0')
        self.scheduler.add_job(self.process_generating_report, 'cron', day_of_week='mon-fri', hour='9',
                               minute='0')  # 오전 9시 '채권일임'방에 채권현황 리포트 보내기
        self.scheduler.start()

    # 계좌(계약) 수 - 조회시
    def contract_number_now(self):
        total_contract_number_now_query = "select count(c.stock_account_id) " \
                                          "from iruda_member.contract c, iruda_trade.stock_account s, " \
                                          "iruda_service.portfolio pf, iruda_service.product p " \
                                          "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
                                          "and c.stock_account_id = s.stock_account_id and c.product_id=18 " \
                                          "and first_operation_started_date is not null " \
                                          "and c.expired_at is null " \
                                          "and c.status = 'ACTIVE' and s.status = 'ACTIVE'; "
        try:
            self.db.connect_db()
            self.db.cur.execute(total_contract_number_now_query)
            total_contract_number_now_row = self.db.cur.fetchone()
            total_contract_number_now = total_contract_number_now_row[0]
            print(f"조회시 계약 수: {total_contract_number_now}")

            return total_contract_number_now

        except Exception as e:
            raise GlobalException(f"Error: failed to execute contract_number_now: {e}")
        finally:
            self.db.disconnect_db()

    # 계좌(계약) 수 - 전일대비(해지된 전일자 고객 포함되어야 함)
    def contract_number_yesterday(self):
        # 어제 날짜까지(MAX = 어제날짜) 전체 운용중인 계약 수(first_operation_date not null)
        yesterday_total_contract_number_query = "select count(c.stock_account_id) " \
                                                "from iruda_member.contract c, iruda_trade.stock_account s, " \
                                                "iruda_service.portfolio pf, iruda_service.product p " \
                                                "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
                                                "and c.stock_account_id = s.stock_account_id and c.product_id=18 " \
                                                "and first_operation_started_date is not null " \
                                                "and c.first_operation_started_date " \
                                                "between '20230416' and DATE(NOW() - INTERVAL 1 DAY) " \
                                                "and c.status = 'ACTIVE' and s.status = 'ACTIVE';"

        # 어제 날짜(포함)까지 해지한 계약 수
        yesterday_expired_contract_number_query = "select count(c.stock_account_id) " \
                                                  "from iruda_member.contract c, iruda_trade.stock_account s, " \
                                                  "iruda_service.portfolio pf, iruda_service.product p " \
                                                  "where c.product_id = p.product_id " \
                                                  "and c.portfolio_id = pf.portfolio_id " \
                                                  "and c.stock_account_id = s.stock_account_id and c.product_id=18 " \
                                                  "and first_operation_started_date is not null " \
                                                  "and c.first_operation_started_date " \
                                                  "between '20230416' and DATE(NOW() - INTERVAL 1 DAY) " \
                                                  "and c.status = 'EXPIRED' and s.status = 'STOP' " \
                                                  "and EXISTS " \
                                                  "(SELECT 1 " \
                                                  "FROM iruda_member.contract ic " \
                                                  "WHERE ic.expired_at IS NOT NULL " \
                                                  "AND ic.first_operation_started_date IS NOT NULL " \
                                                  "AND ic.stock_account_id = c.stock_account_id);"

        try:
            self.db.connect_db()
            self.db.cur.execute(yesterday_total_contract_number_query)
            yesterday_total_contract_number = self.db.cur.fetchone()[0]

            self.db.cur.execute(yesterday_expired_contract_number_query)
            yesterday_expired_contract_number = self.db.cur.fetchone()[0]

            actual_total_contract_number = yesterday_total_contract_number + yesterday_expired_contract_number
            print(f"해지한 고객들 고려한 전일 전체 계약 수: {actual_total_contract_number}")

            return actual_total_contract_number

        except Exception as e:
            raise GlobalException(f"Error: failed to execute contract_number_yesterday: {e}")
        finally:
            self.db.disconnect_db()

    # 계약금액(조회시) => 채권 출금요청 이미 반영됨/select base_date order_by created_at desc limit 2이기 때문에 완전 '현재'를 반영한 것은 x
    def contract_amount_now(self):
        total_adjusted_amount_query = "SELECT SUM(dae.adjusted_principal) " \
                                      "FROM iruda_trade.daily_account_evaluation dae, iruda_member.contract c " \
                                      "WHERE dae.base_date = " \
                                      "   (SELECT MAX(base_date) FROM iruda_trade.daily_account_evaluation) " \
                                      "AND dae.stock_account_id = c.stock_account_id " \
                                      "AND c.product_id = 18 AND c.first_operation_started_date IS NOT NULL " \
                                      "AND c.expired_at IS NULL AND c.status = 'ACTIVE' AND EXISTS (" \
                                      "SELECT 1 FROM iruda_trade.stock_account s " \
                                      "WHERE s.stock_account_id = c.stock_account_id AND s.status = 'ACTIVE');"

        try:
            self.db.connect_db()
            self.db.cur.execute(total_adjusted_amount_query)
            total_adjusted_amount = self.db.cur.fetchone()[0]
            print(f"조회 시 계약 총액: {total_adjusted_amount}")

            return total_adjusted_amount
        except Exception as e:
            raise GlobalException(f"Error: failed to execute contract_amount_yesterday: {e}")
        finally:
            self.db.disconnect_db()

    # 계약금액(전일대비) => 채권 출금요청 이미 반영됨/select base_date order_by created_at desc limit 2이기 때문에 완전 '현재'를 반영한 것은 x
    def contract_amount_yesterday(self):
        base_date = "SELECT base_date " \
                    "FROM " \
                    "(SELECT DISTINCT base_date FROM iruda_trade.daily_account_evaluation " \
                    "ORDER BY base_date desc) temp " \
                    "LIMIT 2;"

        total_adjusted_amount_query = "SELECT SUM(dae.adjusted_principal) " \
                                      "FROM iruda_trade.daily_account_evaluation dae, iruda_member.contract c " \
                                      "WHERE dae.base_date = %s AND dae.stock_account_id = c.stock_account_id " \
                                      "AND c.product_id = 18 AND c.first_operation_started_date IS NOT NULL " \
                                      "AND c.expired_at IS NULL AND c.status = 'ACTIVE' AND EXISTS (" \
                                      "SELECT 1 FROM iruda_trade.stock_account s " \
                                      "WHERE s.stock_account_id = c.stock_account_id AND s.status = 'ACTIVE');"

        total_adjusted_amount_yesterday_query = "SELECT SUM(dae.adjusted_principal) " \
                                                "FROM iruda_trade.daily_account_evaluation dae, iruda_member.contract c " \
                                                "WHERE dae.base_date = %s " \
                                                "AND dae.stock_account_id = c.stock_account_id " \
                                                "AND c.product_id = 18 AND c.first_operation_started_date IS NOT NULL " \
                                                "AND c.expired_at IS NULL AND c.status = 'ACTIVE' AND EXISTS (" \
                                                "SELECT 1 FROM iruda_trade.stock_account s " \
                                                "WHERE s.stock_account_id = c.stock_account_id " \
                                                "AND s.status = 'ACTIVE');"

        try:
            self.db.connect_db()
            self.db.cur.execute(base_date)
            base_dates = self.db.cur.fetchall()
            first_max_base_date = base_dates[0][0]
            second_max_base_date = base_dates[1][0]

            self.db.cur.execute(total_adjusted_amount_query, first_max_base_date)
            total_adjusted_amount = self.db.cur.fetchone()[0]

            self.db.cur.execute(total_adjusted_amount_yesterday_query, second_max_base_date)
            total_adjusted_amount_yesterday = self.db.cur.fetchone()[0]

            difference_adjusted_amount = total_adjusted_amount - total_adjusted_amount_yesterday
            print(f"전일대비 계약금액: {difference_adjusted_amount}")

            return difference_adjusted_amount
        except Exception as e:
            raise GlobalException(f"Error: failed to execute contract_amount_yesterday: {e}")
        finally:
            self.db.disconnect_db()

    # 당일매수예정금액 (Q. DB를 바라본다 batch 30분/1시간 term으로?)
    def expected_total_buy_amount(self):
        sum_possible_buy_amount_query = "SELECT SUM(possible_buy_amount) FROM iruda_trade.bond_report " \
                                        "WHERE deleted_at is null;"
        try:
            self.db.connect_db()
            self.db.cur.execute(sum_possible_buy_amount_query)
            total_possible_buy_amount = self.db.cur.fetchone()[0]
            print(f"당일매수 예정 금액: {total_possible_buy_amount}")

            return total_possible_buy_amount

        except Exception as e:
            raise GlobalException(f"Error: failed to execute expected_total_buy_amount: {e}")
        finally:
            self.db.disconnect_db()

    # 채권규모 파악하기 위한 정보 => 어차피 매수/매도가 완료되어야 인출가능금액/D+1/D+2가 변하기 때문에 굳이 매도해야할 사용자들, 매수해야할 사용자들을 나누어 관리할 필요 x
    # 장중에 인출가능금액은 변할 수 있으나 체결이 완료되어야지만 인출가능금액/D+1/D+2 변하므로 장 시작 전, 장 시작 후에 cron으로 각각 1번씩 측정할 에정
    # 고객 정보 set
    def set_account_information(self):
        account_info_query = "SELECT stock_company_uid, stock_company_pin, c.stock_account_id, c.uid, " \
                             "iruda_member.decrypt(account_number, 'ACCOUNT') as account_number," \
                             "pf.risk_grade, c.first_operation_started_date " \
                             "FROM iruda_member.contract c, iruda_trade.stock_account s, " \
                             "iruda_service.portfolio pf, iruda_service.product p " \
                             "WHERE c.product_id = p.product_id AND c.portfolio_id = pf.portfolio_id " \
                             "AND c.stock_account_id = s.stock_account_id AND c.product_id=18 " \
                             "AND c.status = 'ACTIVE' AND s.status = 'ACTIVE'" \
                             "AND c.first_operation_started_date IS NOT NULL;"

        try:
            account_list = []

            self.db.connect_db()
            self.db.cur.execute(account_info_query)
            accounts = self.db.cur.fetchall()
            for account in accounts:
                account = AccountData(account[0], account[1], account[2], account[3], account[4], account[5],
                                      account[6])
                account_list.append(account)

            return account_list
        except Exception as e:
            raise GlobalException(f"Error: failed to execute contract_amount_yesterday: {e}")
        finally:
            self.db.disconnect_db()

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

    # check adjusted_principals(원금)
    def get_adjusted_principal(self):
        account_principal_query = "SELECT dae.adjusted_principal, dae.stock_account_id " \
                                  "FROM iruda_trade.daily_account_evaluation dae " \
                                  "JOIN(SELECT MAX(base_date) AS latest_base_date " \
                                  "FROM iruda_trade.daily_account_evaluation) AS temp " \
                                  "WHERE temp.latest_base_date = dae.base_date AND dae.stock_account_id IN (" \
                                  "SELECT c.stock_account_id " \
                                  "FROM iruda_member.contract c, iruda_trade.stock_account s " \
                                  "WHERE c.product_id IN (18) " \
                                  "AND first_operation_started_date IS NOT NULL " \
                                  "AND expired_at IS NULL AND c.status = 'ACTIVE' AND s.status = 'ACTIVE');"

        try:
            self.db.connect_db()
            self.db.cur.execute(account_principal_query)
            rows = self.db.cur.fetchall()  # {stock_account_id : adjusted_principal)

            return {row[1]: row[0] for row in rows}
        except Exception as e:
            raise GlobalException(f"Error: failed to get_adjusted_principals: {e}")
        finally:
            self.db.disconnect_db()

    # 잔고조회된 결과에서 인출금액, 수량, 종목코드, 종목이름, 평균매수가 parse
    def parse_order_balance(self, adjusted_principals, order_balance_for_all_accounts):
        tc = decimal.Decimal('0.001')  # DNDN tc
        accounts_data = {}

        for order_balance in order_balance_for_all_accounts:
            account = order_balance["account"]
            cash = order_balance["order_balance"]["cashBalances"][0]["possibleWithdrawalAmount"]
            stocks = order_balance["order_balance"]['stockBalances']

            corresponding_adjusted_principal = None
            for stock_account_id, adjusted_principal in adjusted_principals.items():
                if str(stock_account_id) == str(account.account_id):
                    corresponding_adjusted_principal = adjusted_principal
                    break

            if corresponding_adjusted_principal is None:
                continue

            account_number = account.account_number
            possible_buy_amount = cash - corresponding_adjusted_principal * tc
            if possible_buy_amount < 0:
                possible_buy_amount = 0

            if account_number not in accounts_data:
                accounts_data[account_number] = {"possible_buy_amount": possible_buy_amount, "stocks": {}}

            for stock in stocks:
                security_code = stock["securityCode"]
                security_name = stock["name"]
                quantity = stock["quantity"]
                buy_unit_price = stock["buyUnitPrice"]
                buy_total_price = (buy_unit_price / float(10)) * quantity

                if security_code not in accounts_data[account_number]["stocks"]:
                    accounts_data[account_number]["stocks"][security_code] = {
                        "security_name": security_name,
                        "quantity": quantity,
                        "buy_total_price": buy_total_price,
                    }
                else:
                    accounts_data[account_number]["stocks"][security_code]["quantity"] += quantity
                    accounts_data[account_number]["stocks"][security_code]["buy_total_price"] += buy_total_price

        return accounts_data

    # format data for insertion into database
    def format_data_for_insertion(self, accounts_data):
        formatted_data = []
        for account_number, account_data in accounts_data.items():
            possible_buy_amount = account_data["possible_buy_amount"]
            formatted_stocks = account_data["stocks"]
            formatted_data.append(
                (
                    account_number,
                    possible_buy_amount,
                    json.dumps(formatted_stocks)
                )
            )

        return formatted_data

    # 인출금액, 수량, 종목코드, 종목이름, 평균매수가 등 저장
    def save_accounts_info(self, accounts_info):
        save_account_info_query = "INSERT INTO iruda_trade.bond_report" \
                                  "(account_number, possible_buy_amount, contents) VALUES (%s, %s, %s)"

        try:
            self.db.connect_db()
            self.db.cur.execute("UPDATE iruda_trade.bond_report SET deleted_at=NOW() WHERE deleted_at iS NULL")
            for account_info in accounts_info:
                self.db.cur.execute(save_account_info_query, account_info)
            self.db.con.commit()
        except Exception as e:
            raise GlobalException(f"Error: failed to save_accounts_info: {e}")
        finally:
            self.db.disconnect_db()

    # API 호출 - DAEMON
    async def batch_process_api_call(self):
        account_list = self.set_account_information()  # API 호출에 필요한 객체 생성

        def should_execute_check_security_codes_to_sell(account: AccountData):
            return account.started_date is not None

        self.account_list = [account for account in account_list
                             if should_execute_check_security_codes_to_sell(account)]

        if not self.account_list:
            logging.info("운용개시일이 있거나, 전체 평가액이 500만원 이상인 고객 없음")
            return  # No accounts meet the condition, skip the rest of the process

        max_retries = 5
        retry_delay = 1
        order_balance_for_all_accounts = None

        # API call for check balance(possibleWithdrawlAmount-adjusted_principal*0.001, name, securityCode, quantity, buyUnitPrice, ..)
        for attempt in range(1, max_retries + 1):
            try:
                order_balance_for_all_accounts = await self.check_order_balance_for_all_accounts()
                break
            except Exception as e:
                logging.error(
                    f"Attempt {attempt} to call check_order_balance_for_all_accounts() failed with error: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
                else:
                    logging.warning("Maximum retries reached, aborting process_bond_orderbook.")
                    return

        # DNDN tc
        adjusted_principal = self.get_adjusted_principal()

        # parsing necessary info
        accounts_data = self.parse_order_balance(adjusted_principal, order_balance_for_all_accounts)
        formatted_accounts_info = self.format_data_for_insertion(accounts_data)

        # save in database(insert +  soft_delete)
        self.save_accounts_info(formatted_accounts_info)

    # Report csv 작성하기 위한 데이터 DB에서 select
    def select_datas_for_statistics(self):
        select_datas_query = "SELECT account_number, contents FROM iruda_trade.bond_report WHERE deleted_at is NULL"

        try:
            self.db.connect_db()
            self.db.cur.execute(select_datas_query)
            rows = self.db.cur.fetchall()

            account_info = {}
            for row in rows:
                account_number = row[0]
                content = json.loads(row[1])
                account_info[account_number] = content

            return account_info
        except Exception as e:
            raise GlobalException(f"Error: failed to select_datas_for_statistics: {e}")
        finally:
            self.db.disconnect_db()

    def analyze_data(self, accounts_info):
        total_contracts_per_security = {}  # 채권별 보유 계좌 수
        total_retaining_per_security = {}  # 채권별 보유 수량
        average_bought_price_per_security = {}  # 채권별 평균 매수가

        for account_number, account_info in accounts_info.items():
            for security_code, security_info in account_info.items():
                # if security_code == 'A385550':
                #     security_name = 'A385550'
                # else:
                #     security_name = security_info["security_name"]
                security_name = security_info["security_name"]

                # 채권별 보유 계좌 수 총 합계
                total_contracts_per_security[security_name] = \
                    total_contracts_per_security.get(security_name, 0) + 1

                total_retaining_per_security[security_name] = \
                    total_retaining_per_security.get(security_name, 0) + security_info["quantity"]

                # 채권별 평균 매수가 계산
                total_buy_price = security_info["buy_total_price"]

                if security_name in average_bought_price_per_security:
                    existing_total_buy_price, existing_quantity = \
                        average_bought_price_per_security[security_name]
                    average_bought_price_per_security[security_name] = (
                        existing_total_buy_price + total_buy_price, existing_quantity + security_info["quantity"])
                else:
                    # If the security is not in the dict yet, add it with the current total buy price and quantity
                    average_bought_price_per_security[security_name] = (total_buy_price, security_info["quantity"])

        # calculate the average buy price for each security
        for security_name, (total_buy_price, total_quantity) in average_bought_price_per_security.items():
            average_bought_price_per_security[security_name] = (total_buy_price / total_quantity) * float(10)

        return total_contracts_per_security, total_retaining_per_security, average_bought_price_per_security

    def generate_excel_file(self,
                            total_contract_number_now,
                            total_contract_number_yesterday,
                            total_adjusted_amount,
                            difference_adjusted_amount,
                            total_possible_buy_amount,
                            total_contracts_per_security,
                            total_retaining_per_security,
                            average_bought_price_per_security):

        try:
            # 국내 채권 일임 현황 Table(1st table)
            data_first_table = {
                "구분": ["계좌(계약) 수", "계약 금액", "당일 매수 예정 금액"],
                "조회 시": [total_contract_number_now, total_adjusted_amount, total_possible_buy_amount],
                "전일 대비": [total_contract_number_yesterday, difference_adjusted_amount, "-"]
            }

            df1 = pd.DataFrame(data_first_table)

            # 보유 종목(2nd table)
            security_code_contract = list(total_contracts_per_security.keys())
            security_code_retaining = list(total_retaining_per_security.keys())
            security_code_average_price = list(average_bought_price_per_security.keys())

            security_list = list(
                set(security_code_contract + security_code_retaining + security_code_average_price))

            data = []
            for security in security_list:
                total_contracts = total_contracts_per_security.get(security, 0)
                total_retaining = total_retaining_per_security.get(security, 0)
                average_price = average_bought_price_per_security.get(security, 0)
                data.append([security, total_contracts, average_price, total_retaining])

            df2 = pd.DataFrame(data, columns=['종목', '보유 계좌 수', '평균 매수가', '보유 수량'])

            # title the file name
            today_date = datetime.now().strftime('%Y%m%d')
            file_name = '업라이즈_투자자문' + str(today_date) + '.xlsx'
            file = open(file_name, 'w')

            # Write to Excel
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                df1.to_excel(writer, sheet_name='국내채권 일임 현황', index=False)
                df2.to_excel(writer, sheet_name='보유종목', index=False)

                workbook = writer.book

                # bold format
                header_format = workbook.add_format({'bold': True, 'align': 'center'})

                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for col_num, value in enumerate(df1.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                for col_num, value in enumerate(df2.columns.values):
                    worksheet.write(0, col_num, value, header_format)

                file.close()

            return file_name

        except (FileNotFoundError, IOError) as file_error:
            logging.error(f"Failed to generate report: {file_error}")
            return None

    async def send_report_to_telegram(self, file_name):
        if self.application is None:
            logging.error("Error: Application not set for Report")

        try:
            with open(file_name, 'rb') as file:
                await self.application.bot.send_document(chat_id=os.getenv("REPORT_CHAT_ID"), document=file)
                #await self.application.bot.send_document(chat_id=os.getenv("CHAT_ID"), document=file)     #practice
        except Exception as e:
            raise GlobalException(f"Error sending CSV file to Telegram: {e}")

    # generate excel file
    async def process_generating_report(self):
        try:
            total_contract_number_now = self.contract_number_now()  # 총 계약 수(조회 시)
            total_contract_number_yesterday = self.contract_number_yesterday()  # 총 계약 수(전일대비)
            total_adjusted_amount = self.contract_amount_now()  # 총 계약금액
            difference_adjusted_amount = self.contract_amount_yesterday()  # 전일대비 계약금액
            total_possible_buy_amount = self.expected_total_buy_amount()  # 매수 예정 금액

            account_info = self.select_datas_for_statistics()
            total_contracts_per_security, total_retaining_per_security, average_bought_price_per_security = \
                self.analyze_data(account_info)
            file_name = self.generate_excel_file(total_contract_number_now,
                                                 total_contract_number_yesterday,
                                                 total_adjusted_amount,
                                                 difference_adjusted_amount,
                                                 total_possible_buy_amount,
                                                 total_contracts_per_security,
                                                 total_retaining_per_security,
                                                 average_bought_price_per_security)

            await self.send_report_to_telegram(file_name)
            logging.info("채권 리포트 전송 완료")
        except Exception as e:
            logging.error(f"Error process_generating_report: {e}")


# 수동으로 API 데이터 집어넣고 싶을 때 사용
if __name__ == "__main__":
    report = Report()
    nest_asyncio.apply()
    asyncio.run(report.batch_process_api_call())

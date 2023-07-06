# import asyncio
# import datetime
#
# import query
# import logging
# import os
# import tzlocal
# import decimal
# import aiohttp
# import nest_asyncio
#
# from dotenv import load_dotenv
# from unhandle import GlobalException
# from database import Database
# from datetime import datetime
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from dto import *
#
#
# class BondBalance:
#     def __init__(self):
#         load_dotenv()
#         self.endpoint = os.getenv("ENDPOINT")
#         self.db = Database()
#         self.account_list = None
#         self.scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))
#
#         self.scheduler.add_job(self.collect_bond_balance_status, 'cron', day_of_week='mon-fri', hour='17',
#                                minute='0')
#         self.scheduler.start()
#
#     # 당일 주문 체결 수량
#     async def collect_bond_balance_status(self):
#         # self.set_account_information()  # 고객별 상품 시작 일자 포함
#         #
#         # #TODO 운용개시일이 있거나, 운용개시일이 없다면 전체 평가금이 >= 500이어야 진행
#         # def should_execute_check_security_codes_to_sell(account: AccountData):
#         #     return account.started_date is not None
#         #
#         # self.account_list = [account for account in self.account_list
#         #                      if should_execute_check_security_codes_to_sell(account)]
#         #
#         # if not self.account_list:
#         #     logging.info("운용개시일이 있거나, 전체 평가액이 500만원 이상인 고객 없음")
#         #     return  # No accounts meet the condition, skip the rest of the process
#
#         self.filter_today_order_account_list()  # set account_list for today's order
#         bond_balances = await self.check_bond_balance_for_all_accounts()
#         self.insert_bond_status(bond_balances)
#
#     def set_account_information(self):
#         """
#         DB에 있는 사용자 정보를 불러와서 사용자 정보(*AccountData*) 객체 생성
#         :return: account_list 사용자 정보 객체 리스트
#         """
#         account_list = []
#         try:
#             self.db.connect_db()
#             self.db.cur.execute(query.select_account_info())
#             rows = self.db.cur.fetchall()
#             for row in rows:  # 고객별 first_operation_started_date => row[6]
#                 account = AccountData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
#                 account_list.append(account)
#         except Exception as e:
#             raise GlobalException(f"Error: fetch_account_information: {e}")
#         finally:
#             self.db.disconnect_db()
#
#         self.account_list = account_list
#
#     # 당일 주문 나간 고객들(DB : iruda_trade.order_book)에서 가져오기
#     def filter_today_order_account_list(self):
#         today_order_list_query = "select stock_company_uid, stock_company_pin, c.stock_account_id, c.uid," \
#                                  "iruda_member.decrypt(account_number, 'ACCOUNT') as account_number, pf.risk_grade, " \
#                                  "c.first_operation_started_date " \
#                                  "from iruda_member.contract c, iruda_trade.stock_account s, " \
#                                  "     iruda_service.portfolio pf, iruda_service.product p " \
#                                  "where c.product_id = p.product_id and c.portfolio_id = pf.portfolio_id " \
#                                  "and c.stock_account_id = s.stock_account_id and c.product_id = 18 " \
#                                  "and account_number IN (" \
#                                  "select iruda_member.encrypt(account_number, 'ACCOUNT')" \
#                                  "from iruda_trade.order_book WHERE DATE(created_at)=DATE(NOW())) "
#
#         account_list = []
#         try:
#             self.db.connect_db()
#             self.db.cur.execute(today_order_list_query)
#             rows = self.db.cur.fetchall()
#             for row in rows:  # latest_deposited_at not necessary info here
#                 account = AccountData(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
#                 account_list.append(account)
#         except Exception as e:
#             logging.error(f"Error: filter_today_order_account_list {e}")
#         finally:
#             self.db.disconnect_db()
#
#         self.account_list = account_list
#
#     # 장 종료 후 체결/미체결 수집
#     async def check_bond_balance_for_account(self, account: AccountData, session: aiohttp.ClientSession):
#         try:
#             api_url = f"{self.endpoint}/kb/v1/accounts/{account.account_number}/orders/bond"
#             params = {
#                 "userNumber": account.csNo,
#                 "userPinCode": account.pinNo,
#                 "inquiryType": "BOND",
#                 "date": datetime.today().strftime('%Y%m%d'),
#                 "bondMarketType": "Listed",
#                 "orderStatus": "FILLED"
#             }
#             async with session.get(api_url, params=params) as response:
#                 if response.status == 200:
#                     response_data = await response.json()
#                     if response_data["succeeded"]:
#                         order_balance = response_data["result"]
#                         return order_balance
#                 else:
#                     logging.error(f"Error: check_bond_balance_for_account for status code: {response.status}")
#         except Exception as e:
#             logging.error(f"Error: check_bond_balance_for_account: {e}")
#
#     # 오늘 주문 나간 고객들에 대해서만 잔고 불러오기
#     async def check_bond_balance_for_all_accounts(self):
#         async def check_bond_balance(account):
#             async with aiohttp.ClientSession() as session:
#                 bond_balances = await self.check_bond_balance_for_account(account, session)
#                 return {'account': account, 'bond_balances': bond_balances}
#
#         tasks = [check_bond_balance(account) for account in self.account_list]
#         results = await asyncio.gather(*tasks)
#
#         return results
#
#     # 잔고 DB insert
#     def insert_bond_status(self, account_bonds):
#         bond_status_query = "INSERT INTO iruda_trade.bond_balance " \
#                             "(account_number, security_name, security_code, ordered_date, quantity," \
#                             "filled_quantity, unfilled_quantity, price, filled_price, transaction_type) " \
#                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#
#         try:
#             self.db.connect_db()
#
#             for account_bond in account_bonds:
#                 account = account_bond['account']
#                 bond_balances = account_bond['bond_balances']
#
#                 if bond_balances['message'].strip() == '정상적으로 조회되었습니다.':
#                     account_number = account.account_number     #계좌번호
#                     orders = bond_balances['orderStatusList']
#                     ordered_date = bond_balances["orderedDate"]     #매수날짜
#                     for order in orders:
#                         security_name = order['securityNameInKorean']   #채권이름
#                         security_code = order['securityCode']   #채권코드
#                         quantity = order['quantity']    #수량
#                         filled_quantity = order['totalFilledQuantity']  #매수/매도 체결 수량
#                         unfilled_quantity = order['unfilledQuantity']   #매수/매도 미체결 수량
#                         price = order['price']      #주문가
#                         filled_price = order['filledPrice']    #체결가
#                         transaction_type = order['transactionTypeName']     #거래타입(매수/매도)
#
#                         self.db.cur.execute(bond_status_query,
#                                            (account_number, security_name, security_code, ordered_date, quantity,
#                                            filled_quantity, unfilled_quantity, price, filled_price, transaction_type))
#
#                 elif bond_balances['message'].strip() == '조회할 자료가 없습니다.':
#                     continue
#                 else:
#                     logging.error("오늘 주문 건에 대한 체결 조회 이상")
#             self.db.con.commit()
#         except Exception as e:
#             logging.error(f"Error: insert_bond_status {e}")
#
#
# if __name__ == "__main__":
#     bond_balance = BondBalance()
#     nest_asyncio.apply()
#     asyncio.run(bond_balance.collect_bond_balance_status())

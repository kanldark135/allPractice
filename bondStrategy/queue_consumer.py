from unhandle.global_exception import GlobalException
from database import Database
from dto import TelegramData, TelegramDatas
from datetime import datetime
import message_queue
import logging
import asyncio
import query
import os
from bond_orderbook import BondOrderBook
from dotenv import load_dotenv


class QueueConsumer:
    def __init__(self, queue: message_queue, application=None):
        load_dotenv()
        self.queue = queue
        self.db = Database()
        self.order_book = BondOrderBook()
        self.application = application
        self.account_list = None
        self.security_list = None
        self.first_data_processed = False
        self.last_processed_day = None

    async def run(self):
        while True:
            # try:
            #     if not self.queue.is_empty():
            #         self.security_list = await self.queue.get()
            #         self.order_book.telegram_datas = await self.create_telegram_datas()
            #         today_order = self.check_today_order_exists()   # 오늘자 주문이 이미 존재하는지 확인
            #         if not today_order:
            #             buy_order_book, sell_order_book = await self.order_book.process_bond_orderbook()
            #             if buy_order_book or sell_order_book:
            #                 await self.send_csv_to_telegram(os.getenv("CHAT_ID"), buy_order_book)
            #                 await self.send_csv_to_telegram(os.getenv("CHAT_ID"), sell_order_book)
            #                 logging.info("오더북 전송 완료")
            #         else:   # 오늘 주문이 나갔음에도 가격 한번 더 들어온 케이스
            #             await self.order_book.process_modify_order()
            #     else:
            #         await asyncio.sleep(60)
            # except Exception as e:
            #     logging.error(f"global error: {e}")

            # for practice(debugging)
            try:
                if not self.queue.is_empty():
                    self.security_list = await self.queue.get()
                    self.order_book.telegram_datas = await self.create_telegram_datas()
                    buy_order_book, sell_order_book = await self.order_book.process_bond_orderbook()
                    if buy_order_book or sell_order_book:
                        await self.send_csv_to_telegram(os.getenv("CHAT_ID"), buy_order_book)
                        await self.send_csv_to_telegram(os.getenv("CHAT_ID"), sell_order_book)
                        logging.info("오더북 전송 완료")
                else:
                    await asyncio.sleep(60)
            except Exception as e:
                print(f"global error: {e}")

    # 오늘 날짜 주문 있는지 확인
    def check_today_order_exists(self):
        try:
            self.db.connect_db()
            self.db.cur.execute(query.check_today_order_exists())
            count = self.db.cur.fetchone()

            if count[0] > 0:
                logging.info("today_order_exists")
                return True
            else:
                logging.info("today_order_not_exists")
                return False

        except Exception as e:
            logging.error(f"failed to check_today's_order: {e}")
        finally:
            self.db.disconnect_db()

    async def manage_chat_log(self, user_id, user_name, security_list):
        try:
            self.db.connect_db()
            for security in security_list:
                security_name = security.split('KR')[0] or security.split('kr')[0]
                security_code = security.split('KR')[1].split(' ')[0] or security.split('kr')[1].split(' ')[0]
                price = security.split('KR')[1].split(' ')[1]
                self.db.cur.execute(query.insert_datas_bond_history(),
                                    (user_id, user_name, security_name,
                                     'KR' + security_code, price,
                                     datetime.now().strftime('%Y%m%d%H%M%S')))
            self.db.con.commit()
            logging.info("chat log 저장 성공")
        except Exception as e:
            self.db.con.rollback()
            raise GlobalException(f"Error: manage_chat_log: {e}")
        finally:
            self.db.disconnect_db()

    def fetch_status_and_ratio(self, security_code):  # ratio 차등 비중
        try:
            self.db.connect_db()
            self.db.cur.execute(query.select_status_and_ratio_by_code(), security_code)
            row = self.db.cur.fetchone()
            status, ratio = row[0], row[1]
        except Exception as e:
            raise GlobalException(f"Error: fetch_status: {e}")
        finally:
            self.db.disconnect_db()

        return status, ratio

    async def create_telegram_datas(self):
        telegram_datas = TelegramDatas()

        for security in self.security_list:
            status, ratio = self.fetch_status_and_ratio(security["security_code"])
            telegram_data = TelegramData(security["user_id"], security["user_name"], security["security_name"],
                                         security["security_code"], security["price"], status, ratio)
            telegram_datas.add_telegram_data(telegram_data)

        return telegram_datas

    async def send_csv_to_telegram(self, chat_id, file_name):
        if self.application is None:
            raise GlobalException("Error: Application not set for QueueConsumer")

        try:
            with open(file_name, 'rb') as file:
                await self.application.bot.send_document(chat_id=chat_id, document=file)
        except Exception as e:
            raise GlobalException(f"Error sending CSV file to Telegram: {e}")

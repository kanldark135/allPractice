import asyncio
import nest_asyncio
import datetime
import logging
import os
import tracemalloc

import regex
from dotenv import load_dotenv
from telegram import ForceReply, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, ConversationHandler

from bond_orderbook import BondOrderBook
from etf_orderbook import EtfOrderBook
from message_queue import FilteredMessagesQueue
from queue_consumer import QueueConsumer
from report import Report
#from bond_balance import BondBalance

load_dotenv()
tracemalloc.start()
CONFIRMATION = 1

# Enable logging
logging.basicConfig(    
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

last_date = datetime.datetime.now().date()
filtered_message_queue = FilteredMessagesQueue()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"Hi {user.mention_html()}!",
        reply_markup=ForceReply(selective=True),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_text("Help!")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("주문 취소")


# async def reporting(update: Update, context: ContextTypes.DEFAULT_TYPE, report) -> None:
#     await report.process_generating_report()


async def start_order(update: Update, context: ContextTypes.DEFAULT_TYPE, bond_orderbook, etf_orderbook):
    await bond_orderbook.execute_orders(update)

    if not etf_orderbook.scheduler.running:
        etf_orderbook.scheduler.start()

    if not etf_orderbook.etf_task:
        # 스케쥴러와 상관없이 처음에는 우선 주문나가야 함.
        etf_orderbook.etf_task = asyncio.create_task(etf_orderbook.process_etf_order_book())
        etf_orderbook.condition.set()
        # async with etf_orderbook.condition:
        #    etf_orderbook.is_first_run = False
        #    etf_orderbook.condition.set()


async def print_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Chat ID: {chat_id}")


# Entry Point
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, queue_consumer: QueueConsumer) -> int:
    security_list = update.message.text.split('\n')
    await queue_consumer.manage_chat_log(update.effective_user.id, update.effective_user.username, security_list)

    security_lists = await get_filtered_message(update.message.text, update)
    if security_lists:
        context.user_data["security_lists"] = security_lists
        reply_keyboard = [['Y', 'N']]
        await update.message.reply_text("주문을 접수할까요? 입력값: Y/N",
                                        reply_markup=ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True))
        return CONFIRMATION
    else:
        await update.message.reply_text("지원하지 않는 메세지 형식입니다. 다시 입력해주세요. 예시)한국캐피탈451-3 KR6023763B35 9880")
        return ConversationHandler.END


async def ask_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    response = update.message.text
    security_lists = context.user_data["security_lists"]
    pattern = regex.compile(r'^[YNyn]$')
    if not pattern.match(response):
        await update.message.reply_text("잘못된 입력입니다. Y/N 중 하나를 눌러주세요.")
        return CONFIRMATION

    if response == "Y" or response == "y":
        await update.message.reply_text("주문을 접수하였습니다. 주문지 생성이 시작됩니다.", reply_markup=ReplyKeyboardRemove())
        await filtered_message_queue.put(security_lists)
    elif response == "N" or response == "n":
        await update.message.reply_text("취소하였습니다. 종목이름, 종목코드, 가격을 다시 입력해주세요", reply_markup=ReplyKeyboardRemove())

    return ConversationHandler.END


# TODO Filtering using Reg Expressions
async def get_filtered_message(message, update):
    security_datas = message.split('\n')
    security_list = []

    for security_data in security_datas:
        security_name = security_data.split('KR')[0] or security_data.split('kr')[0]
        security_code = security_data.split('KR')[1].split(' ')[0] or security_data.split('kr')[1].split(' ')[0]
        if security_code.startswith('kr'):
            security_code = security_code.replace('kr', 'KR')
        # if length of security_code is not 10, it is not valid
        if len(security_code) != 10:
            return None
        price = security_data.split('KR')[1].split(' ')[1]
        if price.endswith('원'):
            price = price.replace('원', '')
        security_list.append({
            "user_id": update.effective_user.id,
            "user_name": update.effective_user.username,
            "security_name": security_name,
            "security_code": 'KR' + security_code,
            "price": int(price),
        })

    return security_list if len(security_list) == len(security_datas) else None


def reset_bot(application):
    for handler in application.handlers:
        application.remove_handler(handler)

    # Re-add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("buy", start_order))
    queue_consumer = QueueConsumer(filtered_message_queue)
    conversation_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND,
                                     lambda update, context: message_handler(update, context, queue_consumer))],
        states={
            CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_confirmation)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    application.add_handler(conversation_handler)


def time_until_next_reset(target_hour, target_minute):
    now = datetime.datetime.now()
    next_reset = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if next_reset <= now:
        next_reset += datetime.timedelta(days=1)
    time_until = (next_reset - now).total_seconds()
    return time_until


async def check_and_reset_bot(application):
    while True:
        global last_date
        current_date = datetime.datetime.now().date()
        now = datetime.datetime.now()
        reset_time = now.replace(hour=8, minute=30, second=0, microsecond=0)

        if current_date > last_date and now >= reset_time:
            reset_bot(application)
            await filtered_message_queue.clear()
            last_date = current_date

        time_to_wait = time_until_next_reset(8, 30)
        await asyncio.sleep(time_to_wait)


async def main() -> None:
    """Start the bot."""
    global last_date
    application = Application.builder().token(os.environ.get("TELEGRAM_TOKEN")).build()

    queue_consumer = QueueConsumer(filtered_message_queue, application)

    bond_orderbook = BondOrderBook()
    etf_orderbook = EtfOrderBook()
    Report(application)
    #BondBalance()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("chat_id", print_chat_id))
    application.add_handler(
        CommandHandler("order", lambda update, context: start_order(update, context, bond_orderbook, etf_orderbook)))
    # application.add_handler(
    #     CommandHandler("report", lambda update, context: reporting(update, context, report)))

    conversation_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND,
                                     lambda update, context: message_handler(update, context, queue_consumer))],
        states={
            CONFIRMATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_confirmation)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],

    )
    application.add_handler(conversation_handler)

    asyncio.create_task(queue_consumer.run())
    asyncio.create_task(check_and_reset_bot(application))

    await application.run_polling()
    await application.shutdown()


if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())

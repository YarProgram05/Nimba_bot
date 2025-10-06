import os
import logging
import warnings
from telegram.warnings import PTBUserWarning
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    CallbackContext,
    ConversationHandler,
    filters
)
from dotenv import load_dotenv
import datetime
from datetime import time as tm
from zoneinfo import ZoneInfo  # Python 3.9+

# Подавляем warning о per_message=False
warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*per_message=False.*")

# Загружаем переменные окружения
load_dotenv()

# Импортируем состояния
from states import (
    SELECTING_ACTION,
    WB_REPORT_FILES,
    WB_REMAINS_FILES,
    OZON_REMAINS_CABINET_CHOICE,
    BARCODE_FILES,
    CSV_FILES,
    OZON_SALES_CABINET_CHOICE,
    OZON_SALES_DATE_START,
    OZON_SALES_DATE_END
)

# Импортируем обработчики
from handlers.wb_handler import (
    start_wb_report,
    handle_wb_files,
    generate_wb_report
)
from handlers.ozon_remains_handler import (
    start_ozon_remains,
    handle_cabinet_choice,
    send_ozon_remains_automatic
)
from handlers.wb_remains_handler import (
    start_wb_remains,
    handle_wb_remains_files,
    generate_wb_remains_report
)
from handlers.barcode_handler import (
    start_barcode_generation,
    handle_barcode_files,
    generate_barcode_report
)
from handlers.csv_converter_handler import (
    start_csv_conversion,
    handle_csv_files,
    generate_xlsx_files
)
from handlers.ozon_sales_handler import (
    start_ozon_sales,
    handle_sales_cabinet_choice,
    handle_sales_date_start,
    handle_sales_date_end
)

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_next_monday_10am(tz):
    """Возвращает datetime ближайшего понедельника в 10:00 по указанному часовому поясу"""
    now = datetime.datetime.now(tz)
    days_ahead = (0 - now.weekday()) % 7  # 0 = понедельник
    next_monday = now.replace(hour=21, minute=50, second=0, microsecond=0) + datetime.timedelta(days=days_ahead)

    # Если сегодня понедельник, но уже после 10:00 — берём следующий понедельник
    if days_ahead == 0 and now.time() > datetime.time(21, 50):
        next_monday += datetime.timedelta(weeks=1)

    return next_monday

def get_main_menu():
    """Возвращает главное меню с кнопками"""
    return ReplyKeyboardMarkup(
        [
            ["Продажи Ozon", "Продажи WB"],
            ["Остатки товаров Ozon", "Остатки товаров WB"],
            ["Генерация штрихкодов"],
            ["Конвертация CSV в XLSX"],
            ["Помощь"]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )


def cleanup_user_data(context: CallbackContext):
    """Полная очистка данных пользователя"""
    try:
        for key, value in list(context.user_data.items()):
            if key.endswith('_files') and isinstance(value, list):
                for file_path in value:
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except:
                        pass
        context.user_data.clear()
        return True
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}")
        return False


async def start(update: Update, context: CallbackContext) -> int:
    cleanup_user_data(context)
    # print("Ваш chat_id:", update.effective_chat.id)
    welcome_text = (
        "🔄 Бот сброшен. Добро пожаловать!\n\n"
        "Я помогу вам:\n"
        "📊 Анализировать продажи и остатки на Ozon и Wildberries\n"
        "🏷️ Генерировать штрихкоды\n"
        "🔄 Конвертировать CSV файлы в XLSX\n\n"
        "Выберите действие из меню ниже:"
    )
    await update.message.reply_text(welcome_text, reply_markup=get_main_menu())
    return SELECTING_ACTION


async def show_help(update: Update, context: CallbackContext) -> int:
    cleanup_user_data(context)
    help_text = (
        "📋 Список команд и функций:\n\n"
        "/start - Вернуться в главное меню (сброс всех операций)\n"
        "/help - Показать справку\n\n"
        "💡 Используйте кнопки для выбора функций!"
    )
    await update.message.reply_text(help_text)
    return SELECTING_ACTION


async def select_action(update: Update, context: CallbackContext) -> int:
    text = update.message.text
    if text == "Продажи Ozon":
        return await start_ozon_sales(update, context)
    elif text == "Продажи WB":
        return await start_wb_report(update, context)
    elif text == "Остатки товаров Ozon":
        return await start_ozon_remains(update, context)
    elif text == "Остатки товаров WB":
        return await start_wb_remains(update, context)
    elif text == "Генерация штрихкодов":
        return await start_barcode_generation(update, context)
    elif text == "Конвертация CSV в XLSX":
        return await start_csv_conversion(update, context)
    elif text == "Помощь":
        return await show_help(update, context)
    return SELECTING_ACTION


def main() -> None:
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("❌ BOT_TOKEN не задан в .env")

    application = Application.builder().token(bot_token).build()

    # === 🗓️ ЕЖЕНЕДЕЛЬНЫЙ ОТЧЁТ: КАЖДЫЙ ПОНЕДЕЛЬНИК В 10:00 ПО МОСКВЕ ===
    YOUR_CHAT_ID = 726413418  # ← ваш ID
    moscow_tz = ZoneInfo("Europe/Moscow")

    next_run = get_next_monday_10am(moscow_tz)
    first_run_seconds = (next_run - datetime.datetime.now(moscow_tz)).total_seconds()

    # Кабинет 1
    application.job_queue.run_repeating(
        callback=send_ozon_remains_automatic,
        interval=7 * 24 * 60 * 60,  # 7 дней в секундах
        first=first_run_seconds,
        data={'chat_id': YOUR_CHAT_ID, 'cabinet_id': 1}
    )

    # Кабинет 2 (на 1 минуту позже)
    application.job_queue.run_repeating(
        callback=send_ozon_remains_automatic,
        interval=7 * 24 * 60 * 60,
        first=first_run_seconds + 60,
        data={'chat_id': YOUR_CHAT_ID, 'cabinet_id': 2}
    )

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("help", show_help),
            MessageHandler(
                filters.Regex(
                    '^(Продажи Ozon|Продажи WB|Остатки товаров Ozon|Остатки товаров WB|Генерация штрихкодов|Конвертация CSV в XLSX|Помощь)$'
                ),
                select_action
            ),
        ],
        states={
            SELECTING_ACTION: [
                MessageHandler(
                    filters.Regex(
                        '^(Продажи Ozon|Продажи WB|Остатки товаров Ozon|Остатки товаров WB|Генерация штрихкодов|Конвертация CSV в XLSX|Помощь)$'
                    ),
                    select_action
                ),
            ],
            WB_REPORT_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_wb_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_wb_report),
            ],
            WB_REMAINS_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_wb_remains_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_wb_remains_report),
            ],
            OZON_REMAINS_CABINET_CHOICE: [
                CallbackQueryHandler(handle_cabinet_choice),
            ],
            BARCODE_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_barcode_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_barcode_report),
            ],
            CSV_FILES: [
                MessageHandler(filters.Document.FileExtension("csv"), handle_csv_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_xlsx_files),
            ],
            OZON_SALES_CABINET_CHOICE: [
                CallbackQueryHandler(handle_sales_cabinet_choice),
            ],
            OZON_SALES_DATE_START: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sales_date_start),
            ],
            OZON_SALES_DATE_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sales_date_end),
            ],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False,
        per_chat=True,
        per_user=True
    )

    application.add_handler(conv_handler)

    logger.info("🚀 Бот запущен!")
    application.run_polling()


if __name__ == '__main__':
    main()
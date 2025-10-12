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
    filters,
    PicklePersistence  # ← ДОБАВЛЕНО
)
from dotenv import load_dotenv

# Подавляем warning о per_message=False
warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*per_message=False.*")

# Загружаем переменные окружения
load_dotenv()

# Импортируем состояния
from states import (
    SELECTING_ACTION,
    WB_REPORT_FILES,
    WB_REMAINS_FILES,
    WB_REMAINS_CABINET_CHOICE,
    WB_REPORT_CABINET_CHOICE,
    OZON_REMAINS_CABINET_CHOICE,
    BARCODE_FILES,
    CSV_FILES,
    OZON_SALES_CABINET_CHOICE,
    OZON_SALES_DATE_START,
    OZON_SALES_DATE_END,
    ALL_MP_REMAINS,
    AUTO_REPORT_TOGGLE,
    AUTO_REPORT_FREQUENCY,
    AUTO_REPORT_WEEKLY_DAY,
    AUTO_REPORT_TIME,
    AUTO_REPORT_DAILY_TIME
)

# Импортируем обработчики
from handlers.wb_handler import (
    start_wb_report,
    handle_wb_files,
    generate_wb_report,
    handle_wb_sales_cabinet_choice
)
from handlers.ozon_remains_handler import (
    start_ozon_remains,
    handle_cabinet_choice
)
from handlers.wb_remains_handler import (
    start_wb_remains,
    handle_wb_cabinet_choice
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
from handlers.all_mp_remains_handler import (
    start_all_mp_remains,
    send_all_mp_remains_automatic
)
# Импортируем обработчики автоотчётов
from handlers.auto_report_handler import (
    start_auto_report,
    handle_toggle,
    handle_interval_type,
    handle_time_input,
    handle_weekly_day_choice,
    handle_daily_time_input
)

# Менеджер автоотчётов
from utils.auto_report_manager import schedule_all_jobs

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def get_main_menu():
    """Возвращает главное меню с кнопками"""
    return ReplyKeyboardMarkup(
        [
            ["Продажи Ozon", "Продажи WB"],
            ["Остатки товаров Ozon", "Остатки товаров WB"],
            ["Остатки на всех МП"],
            ["Автоотчёты"],
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
    welcome_text = (
        "🔄 Бот сброшен. Добро пожаловать!\n\n"
        "Я помогу вам:\n"
        "📊 Анализировать продажи и остатки на Ozon и Wildberries\n"
        "🏷️ Генерировать штрихкоды\n"
        "🔄 Конвертировать CSV файлы в XLSX\n"
        "🤖 Настраивать автоматические отчёты\n\n"
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
    elif text == "Остатки на всех МП":
        return await start_all_mp_remains(update, context)
    elif text == "Автоотчёты":
        return await start_auto_report(update, context)
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

    # 🔑 ВКЛЮЧАЕМ ПЕРСИСТЕНТНОСТЬ
    persistence = PicklePersistence(filepath="bot_conversation_data.pickle")

    application = Application.builder().token(bot_token).persistence(persistence).build()

    # Загружаем сохранённые автоматические отчёты
    schedule_all_jobs(application)

    # === ОСНОВНОЙ ДИАЛОГ ===
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("help", show_help),
        ],
        states={
            SELECTING_ACTION: [
                MessageHandler(filters.Regex(
                    '^(Продажи Ozon|Продажи WB|Остатки товаров Ozon|Остатки товаров WB|Остатки на всех МП|Автоотчёты|Генерация штрихкодов|Конвертация CSV в XLSX|Помощь)$'
                ), select_action),
            ],
            WB_REPORT_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_wb_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_wb_report),
            ],
            WB_REMAINS_CABINET_CHOICE: [
                CallbackQueryHandler(handle_wb_cabinet_choice),
            ],
            WB_REPORT_CABINET_CHOICE: [
                CallbackQueryHandler(handle_wb_sales_cabinet_choice),
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
            ALL_MP_REMAINS: [],
            # Состояния автоотчётов
            AUTO_REPORT_TOGGLE: [
                MessageHandler(filters.Text(["✅ Включить", "❌ Выключить"]), handle_toggle)
            ],
            AUTO_REPORT_FREQUENCY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_interval_type)
            ],
            AUTO_REPORT_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_time_input)
            ],
            AUTO_REPORT_WEEKLY_DAY: [
                CallbackQueryHandler(handle_weekly_day_choice)
            ],
            AUTO_REPORT_DAILY_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_daily_time_input)
            ],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False,
        per_chat=True,
        per_user=True,
        # 🔑 КЛЮЧЕВЫЕ ПАРАМЕТРЫ ДЛЯ РАБОТЫ СОСТОЯНИЙ:
        name="main_conversation",
        persistent=True,
        allow_reentry=True
    )

    application.add_handler(conv_handler)

    logger.info("🚀 Бот запущен!")
    application.run_polling()


if __name__ == '__main__':
    main()
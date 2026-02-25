import os
import logging
import warnings
from telegram.warnings import PTBUserWarning
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    CallbackContext,
    ConversationHandler,
    filters,
    PicklePersistence
)
from dotenv import load_dotenv

# Подавляем warning о per_message=False
warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*per_message=False.*")

# Загружаем переменные окружения
load_dotenv()


# Импортируем состояния
from states import (
    SELECTING_ACTION,
    WB_REMAINS_CABINET_CHOICE,
    OZON_REMAINS_CABINET_CHOICE,
    BARCODE_FILES,
    CSV_FILES,
    OZON_SALES_CABINET_CHOICE,
    OZON_SALES_DATE_START,
    OZON_SALES_DATE_END,
    WB_SALES_CABINET_CHOICE,
    WB_SALES_DATE_START,
    WB_SALES_DATE_END,
    ALL_MP_REMAINS,
    AUTO_REPORT_TOGGLE,
    AUTO_REPORT_FREQUENCY,
    AUTO_REPORT_TIME,
    AUTO_REPORT_WEEKLY_DAY,
    AUTO_REPORT_DAILY_TIME,
    AUTO_REPORT_START_TIME,
    AUTO_REPORT_START_DAY,
    SELECTING_AUTO_REPORT_TYPE,
    SETTINGS_MENU,
    SETTINGS_STOCK_RED,
    SETTINGS_STOCK_YELLOW,
    TZ_CABINET_SELECT,
    TZ_WAIT_FILE
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
# ОБРАБОТЧИК: продажи WB через загрузку файлов (API не используется)
from handlers.wb_sales_handler import (
    start_wb_sales,
    handle_wb_sales_cabinet_choice as handle_wb_sales_cabinet_choice_api,
    handle_wb_sales_date_start,
    handle_wb_sales_date_end
)
from handlers.all_mp_remains_handler import (
    start_all_mp_remains,
    send_all_mp_remains_automatic
)
from handlers.auto_report_handler import (
    start_auto_report,
    handle_toggle_inline,
    handle_interval_type_inline,
    handle_time_inline,
    handle_weekly_day_choice,
    handle_daily_time_input,
    handle_start_time_input,
    handle_start_day_choice,
    handle_back_from_time_input,
    handle_back_from_start_time,
    handle_select_report_type
)
from handlers.settings_handler import (
    start_settings,
    handle_settings_choice,
    handle_stock_red_input,
    handle_stock_yellow_input
)
from handlers.tz_handler import (
    start_tz_generation,
    handle_tz_cabinet_select,
    handle_tz_file,
)

# Менеджер автоотчётов
from utils.auto_report_manager import schedule_all_jobs
from utils.menu import get_main_menu

# Настройка логгирования
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
logs_dir = os.path.join(project_root, 'other')
os.makedirs(logs_dir, exist_ok=True)

# Консольный обработчик
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_format))

# Файловый обработчик для всех логов
file_handler = logging.FileHandler(os.path.join(logs_dir, 'bot.log'), encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(log_format))

# Файловый обработчик для ошибок
error_handler = logging.FileHandler(os.path.join(logs_dir, 'bot.err'), encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter(log_format))

# Настройка корневого логгера
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[console_handler, file_handler, error_handler],
    force=True
)

logger = logging.getLogger(__name__)

# Уменьшаем уровень логирования для httpx (чтобы не спамило HTTP запросами)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO)

# Инициализация базы данных артикулов
try:
    from utils.database import get_database
    db = get_database()
    if db.needs_sync():
        logger.info("🔄 Выполняется синхронизация базы данных с Excel...")
        db.sync_from_excel()
        logger.info("✅ База данных синхронизирована")
    else:
        logger.info("✅ База данных актуальна")
except Exception as e:
    logger.warning(f"⚠️ Ошибка инициализации базы данных: {e}")
    logger.warning("Будет использоваться чтение напрямую из Excel")


def cleanup_user_data(context: CallbackContext):
    """Полная очистка данных пользователя"""
    try:
        for key, value in list(context.user_data.items()):
            if key.endswith('_files') and isinstance(value, list):
                for file_path in value:
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файл {file_path}: {e}")
        context.user_data.clear()
        return True
    except Exception as e:
        logger.error(f"Ошибка при очистке данных: {e}")
        return False


async def start(update: Update, context: CallbackContext) -> int:
    cleanup_user_data(context)
    welcome_text = (
        "Добро пожаловать!\n\n"
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
        return await start_wb_sales(update, context)  # ← Используем НОВЫЙ обработчик
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
    elif text == "Настройки":
        return await start_settings(update, context)
    elif text == "Формирование ТЗ":
        return await start_tz_generation(update, context)
    elif text == "Помощь":
        return await show_help(update, context)
    return SELECTING_ACTION


# === ДЕБАГ: ЛОГИРОВАНИЕ ВСЕХ ОБНОВЛЕНИЙ ===
async def debug_all_updates(update: Update, context: CallbackContext):
    logger.info(f"📥 ПОЛНЫЙ UPDATE: {update}")
    if update.message:
        logger.info(f"   Текст сообщения: {repr(update.message.text)}")
        logger.info(f"   Chat ID: {update.effective_chat.id}")
    if update.callback_query:
        logger.info(f"   Callback data: {update.callback_query.data}")


async def error_handler(update: object, context: CallbackContext) -> None:
    """Обработчик ошибок бота"""
    error_message = str(context.error)

    # Игнорируем ошибку конфликта (запущено несколько экземпляров)
    # Это происходит когда старый процесс еще не завершился
    if "Conflict" in error_message and "getUpdates" in error_message:
        logger.warning(f"⚠️ Обнаружен конфликт подключений (возможно запущено несколько ботов). Игнорируем...")
        return

    # Игнорируем сетевые таймауты (это нормально)
    if "TimedOut" in error_message or "Timed out" in error_message:
        logger.debug(f"🔄 Таймаут сети (это нормально): {error_message}")
        return

    # Логируем остальные ошибки
    logger.error(f"❌ Произошла ошибка: {context.error}", exc_info=context.error)

    # Если есть update с сообщением от пользователя
    if update and isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Произошла ошибка. Попробуйте /start для перезапуска."
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке: {e}")


def main() -> None:
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("❌ BOT_TOKEN не задан в .env")

    # Создаем персистентность с обработкой ошибок
    persistence_file = "bot_conversation_data.pkl"
    try:
        persistence = PicklePersistence(filepath=persistence_file, update_interval=1)
    except (TypeError, EOFError) as e:
        logger.warning(f"⚠️ Файл персистентности поврежден, создаем новый: {e}")
        # Удаляем поврежденный файл
        if os.path.exists(persistence_file):
            os.remove(persistence_file)
            logger.info(f"Удален поврежденный файл: {persistence_file}")
        # Создаем новый
        persistence = PicklePersistence(filepath=persistence_file, update_interval=1)

    application = Application.builder().token(bot_token).persistence(persistence).build()

    # Загружаем сохранённые автоотчёты
    schedule_all_jobs(application)

    # === ДЕБАГ-ЛОГГЕР (закомментирован для продакшена) ===
    # Раскомментируйте следующую строку для подробного логирования всех обновлений
    # application.add_handler(MessageHandler(filters.ALL, debug_all_updates), group=-1)

    # Основной диалог
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("help", show_help),
        ],
        states={
            SELECTING_ACTION: [
                MessageHandler(filters.Regex(
                    '^(Продажи Ozon|Продажи WB|Остатки товаров Ozon|Остатки товаров WB|Остатки на всех МП|Автоотчёты|Генерация штрихкодов|Конвертация CSV в XLSX|Формирование ТЗ|Настройки|Помощь)$'
                ), select_action),
            ],
            WB_REMAINS_CABINET_CHOICE: [
                CallbackQueryHandler(handle_wb_cabinet_choice),
            ],
            # Состояния для WB продаж через загрузку файлов
            WB_SALES_CABINET_CHOICE: [
                CallbackQueryHandler(handle_wb_sales_cabinet_choice_api),
            ],
            WB_SALES_DATE_START: [
                MessageHandler(filters.Document.ALL, handle_wb_sales_date_start),
            ],
            WB_SALES_DATE_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_wb_sales_date_end),
            ],
            # Состояния для Ozon продаж
            OZON_REMAINS_CABINET_CHOICE: [
                CallbackQueryHandler(handle_cabinet_choice),
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
            # Прочие состояние
            BARCODE_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_barcode_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_barcode_report),
            ],
            CSV_FILES: [
                MessageHandler(filters.Document.FileExtension("csv"), handle_csv_files),
                MessageHandler(filters.Text("Все файлы отправлены"), generate_xlsx_files),
            ],
            ALL_MP_REMAINS: [],
            # Состояния автоотчётов
            AUTO_REPORT_TOGGLE: [
                CallbackQueryHandler(handle_toggle_inline)
            ],
            AUTO_REPORT_FREQUENCY: [
                CallbackQueryHandler(handle_interval_type_inline)
            ],
            AUTO_REPORT_TIME: [
                CallbackQueryHandler(handle_time_inline)
            ],
            AUTO_REPORT_WEEKLY_DAY: [
                CallbackQueryHandler(handle_weekly_day_choice)
            ],
            AUTO_REPORT_START_DAY: [
                CallbackQueryHandler(handle_start_day_choice),
                CallbackQueryHandler(handle_back_from_time_input)
            ],
            AUTO_REPORT_DAILY_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_daily_time_input),
                CallbackQueryHandler(handle_back_from_time_input)
            ],
            AUTO_REPORT_START_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_time_input),
                CallbackQueryHandler(handle_back_from_start_time)
            ],
            SELECTING_AUTO_REPORT_TYPE: [
                CallbackQueryHandler(handle_select_report_type)
            ],
            SETTINGS_MENU: [
                CallbackQueryHandler(handle_settings_choice)
            ],
            SETTINGS_STOCK_RED: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_stock_red_input)
            ],
            SETTINGS_STOCK_YELLOW: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_stock_yellow_input)
            ],
            TZ_CABINET_SELECT: [
                CallbackQueryHandler(handle_tz_cabinet_select),
            ],
            TZ_WAIT_FILE: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_tz_file),
            ],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False,
        per_chat=True,
        per_user=True,
        name="main_conversation",
        persistent=True,
        allow_reentry=True
    )

    application.add_handler(conv_handler)

    # Регистрируем обработчик ошибок
    application.add_error_handler(error_handler)

    logger.info("📡 Запуск в режиме polling")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()

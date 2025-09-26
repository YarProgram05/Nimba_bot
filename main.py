import os
import sys
import logging
import warnings
from telegram.warnings import PTBUserWarning
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
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

# Подавляем warning о per_message=False
warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*per_message=False.*")

# Загружаем переменные окружения
load_dotenv()

# Импортируем обработчики
# УДАЛЕНО: ozon_handler (не используется для API-выгрузки)
from handlers.wb_handler import (
    start_wb_report,
    handle_wb_files,
    generate_wb_report
)
from handlers.ozon_remains_handler import (
    start_ozon_remains,
    handle_report_type_choice,
    handle_cabinet_choice as handle_ozon_remains_cabinet,
    OZON_REMAINS_CABINET_CHOICE,
    OZON_REMAINS_REPORT_TYPE
)
from handlers.ozon_sales_handler import (
    start_ozon_sales,
    handle_cabinet_choice as handle_ozon_sales_cabinet,
    handle_date_input,
    OZON_SALES_CABINET_CHOICE,
    OZON_SALES_DATE_INPUT
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

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния разговоров
(
    SELECTING_ACTION,
    WB_REPORT_FILES,
    OZON_REMAINS_CABINET_CHOICE,
    OZON_REMAINS_REPORT_TYPE,
    OZON_SALES_CABINET_CHOICE,
    OZON_SALES_DATE_INPUT,
    WB_REMAINS_FILES,
    BARCODE_FILES,
    CSV_FILES
) = range(9)


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
    """Команда /start — сброс и возврат в главное меню"""
    cleanup_user_data(context)

    welcome_text = (
        "🔄 Бот сброшен. Добро пожаловать!\n\n"
        "Я помогу вам:\n"
        "📊 Анализировать продажи и остатки на Ozon и Wildberries\n"
        "🏷️ Генерировать штрихкоды\n"
        "🔄 Конвертировать CSV файлы в XLSX\n\n"
        "Выберите действие из меню ниже:"
    )

    await update.message.reply_text(
        welcome_text,
        reply_markup=get_main_menu()
    )
    return SELECTING_ACTION


async def show_help(update: Update, context: CallbackContext) -> int:
    """Показ помощи"""
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
    """Обработка выбора действия через кнопки"""
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


# ✅ ГЛОБАЛЬНЫЙ ОБРАБОТЧИК CALLBACK-КНОПОК
async def global_callback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    if query.data in ['raw', 'template']:
        await handle_report_type_choice(update, context)
    elif query.data in ['cabinet_1', 'cabinet_2']:
        # Определяем контекст по текущему состоянию
        current_state = context.user_data.get('conversation_state')

        # Для продаж Ozon
        if current_state == 'ozon_sales_cabinet':
            return await handle_ozon_sales_cabinet(update, context)
        # Для остатков Ozon
        else:
            return await handle_ozon_remains_cabinet(update, context)
    else:
        await query.message.reply_text("Неизвестная команда")


def main() -> None:
    # Получаем токен из .env
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("❌ BOT_TOKEN не задан в .env")

    application = Application.builder().token(bot_token).build()

    # ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("help", show_help),
        ],
        states={
            SELECTING_ACTION: [
                MessageHandler(filters.Regex(
                    '^(Продажи Ozon|Продажи WB|Остатки товаров Ozon|Остатки товаров WB|Генерация штрихкодов|Конвертация CSV в XLSX|Помощь)$'),
                    select_action),
            ],
            WB_REPORT_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_wb_files),
                MessageHandler(filters.Regex('^Все файлы отправлены$'), generate_wb_report),
            ],
            OZON_REMAINS_CABINET_CHOICE: [
                CallbackQueryHandler(global_callback_handler)
            ],
            OZON_REMAINS_REPORT_TYPE: [],
            OZON_SALES_CABINET_CHOICE: [
                CallbackQueryHandler(global_callback_handler)
            ],
            OZON_SALES_DATE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_date_input)
            ],
            WB_REMAINS_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_wb_remains_files),
                MessageHandler(filters.Regex('^Все файлы отправлены$'), generate_wb_remains_report),
            ],
            BARCODE_FILES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), handle_barcode_files),
                MessageHandler(filters.Regex('^Все файлы отправлены$'), generate_barcode_report),
            ],
            CSV_FILES: [
                MessageHandler(filters.Document.FileExtension("csv"), handle_csv_files),
                MessageHandler(filters.Regex('^Все файлы отправлены$'), generate_xlsx_files),
            ],
        },
        fallbacks=[CommandHandler('start', start)],
        per_message=False,
        per_chat=True,
        per_user=True
    )

    application.add_handler(conv_handler)

    # Отдельные команды вне разговора
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", show_help))

    # Запуск бота
    logger.info("🚀 Бот запущен!")
    application.run_polling()


if __name__ == '__main__':
    main()
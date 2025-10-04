import sys
import os

# Добавляем путь к utils
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.append(utils_path)

from template_loader import load_template
from excel_utils import create_report

import pandas as pd
import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler, filters
from states import WB_REPORT_FILES

logger = logging.getLogger(__name__)


async def start_wb_report(update: Update, context: CallbackContext) -> int:
    """Начало обработки отчета Wildberries (ПРОДАЖИ)"""
    context.user_data['wb_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=False  # Клавиатура остаётся, чтобы было удобно
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте файл продаж Wildberries:\n\n"
        "📎 Название файла должно содержать 'продажи' (например, 'ВБ_продажи.xlsx')\n\n"
        "После отправки всех файлов нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return WB_REPORT_FILES


async def handle_wb_files(update: Update, context: CallbackContext) -> int:
    """Обработка файлов Wildberries (ПРОДАЖИ)"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return WB_REPORT_FILES

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_{file_name}"
    await file.download_to_drive(file_path)

    # Сохранение файла
    user_data.setdefault('wb_files', []).append(file_path)

    # Уточняем: это файл ПРОДАЖ
    await update.message.reply_text(
        f"✅ Файл продаж Wildberries '{file_name}' получен.\n"
        "Нажмите «Все файлы отправлены», если готовы сформировать отчёт.",
        reply_markup=ReplyKeyboardMarkup([["Все файлы отправлены"]], resize_keyboard=True)
    )

    return WB_REPORT_FILES


async def generate_wb_report(update: Update, context: CallbackContext) -> int:
    """Генерация отчета Wildberries (ПРОДАЖИ)"""
    logger.info("Вызвана generate_wb_report для продаж WB")
    logger.info(f"Получено сообщение: '{update.message.text}'")

    user_data = context.user_data
    wb_files = user_data.get('wb_files', [])

    if not wb_files:
        await update.message.reply_text(
            "❌ Не получены файлы для формирования отчета!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Обрабатываю файлы продаж Wildberries...")

        # Загрузка шаблона
        art_to_id, id_to_name, main_ids_ordered = load_template("Шаблон_WB")

        # Обработка файлов
        all_purchases = {}
        all_cancels = {}
        all_income = {}

        for file_path in wb_files:
            purchases, cancels, income = process_wb_sales(file_path)

            for art in purchases:
                all_purchases[art] = all_purchases.get(art, 0) + purchases[art]
                all_income[art] = all_income.get(art, 0) + income.get(art, 0)

            for art in cancels:
                all_cancels[art] = all_cancels.get(art, 0) + cancels[art]

        # Группировка данных
        grouped, unmatched = group_wb_data(
            all_purchases,
            all_cancels,
            all_income,
            art_to_id,
            id_to_name
        )

        # Создание отчета
        report_path = "WB_Report.xlsx"
        create_report(
            grouped,
            unmatched,
            id_to_name,
            main_ids_ordered,
            report_path
        )

        # Отправка отчета
        await update.message.reply_document(
            document=open(report_path, 'rb'),
            caption="📊 Отчет по продажам Wildberries",
            reply_markup=ReplyKeyboardRemove()
        )

        # Очистка временных файлов
        for file_path in wb_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        if os.path.exists(report_path):
            os.remove(report_path)

    except Exception as e:
        logger.error(f"Ошибка обработки Wildberries: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при обработке файлов Wildberries: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END


def process_wb_sales(file_path):
    """Обработка файла продаж Wildberries"""
    df = None
    for i in range(10):
        try:
            df = pd.read_excel(file_path, header=i)
            required_columns = [
                'Артикул продавца',
                'шт.',
                'Выкупили, шт.',
                'К перечислению за товар, руб.'
            ]
            if all(col in df.columns for col in required_columns):
                break
        except Exception:
            continue

    if df is None:
        raise ValueError("Не удалось найти таблицу с нужными столбцами в файле")

    purchases = {}
    orders = {}
    income = {}
    cancels = {}

    for _, row in df.iterrows():
        art = str(row['Артикул продавца']).strip().lower()
        if not art or art == 'nan':
            continue

        ordered = row['шт.']
        purchased = row['Выкупили, шт.']
        amount = row['К перечислению за товар, руб.']

        if not isinstance(ordered, (int, float)) or not isinstance(purchased, (int, float)):
            continue

        orders[art] = orders.get(art, 0) + ordered
        purchases[art] = purchases.get(art, 0) + purchased
        income[art] = income.get(art, 0) + (amount if pd.notna(amount) else 0)

    for art in orders:
        cancels[art] = orders[art] - purchases.get(art, 0)

    return purchases, cancels, income


def group_wb_data(purchases, cancels, income, art_to_id, id_to_name):
    """Группировка данных Wildberries по шаблону"""
    all_arts = set(purchases.keys()) | set(cancels.keys()) | set(income.keys())

    grouped = {}
    unmatched = {}

    for art in all_arts:
        group_id = art_to_id.get(art, None)

        if group_id is not None:
            group_name = id_to_name.get(group_id, art)

            if group_id not in grouped:
                grouped[group_id] = {
                    'name': group_name,
                    'purchases': 0,
                    'cancels': 0,
                    'income': 0
                }

            grouped[group_id]['purchases'] += purchases.get(art, 0)
            grouped[group_id]['cancels'] += cancels.get(art, 0)
            grouped[group_id]['income'] += income.get(art, 0)
        else:
            unmatched[art] = {
                'name': f"НЕОПОЗНАННЫЙ: {art}",
                'purchases': purchases.get(art, 0),
                'cancels': cancels.get(art, 0),
                'income': income.get(art, 0)
            }

    return grouped, unmatched
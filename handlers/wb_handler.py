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

# Исправляем импорты
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)


logger = logging.getLogger(__name__)


async def start_wb_report(update: Update, context: CallbackContext) -> int:
    """Начало обработки отчета Wildberries"""
    context.user_data['wb_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте файлы для Wildberries:\n\n"
        "1. Файл продаж ('ВБ_продажи')\n\n"
        "После отправки файла нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return 2  # Состояние ожидания файлов


async def handle_wb_files(update: Update, context: CallbackContext) -> int:
    """Обработка файлов Wildberries"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return 2

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_{file_name}"
    await file.download_to_drive(file_path)

    # Сохранение файла
    user_data.setdefault('wb_files', []).append(file_path)
    await update.message.reply_text(f"✅ Файл Wildberries '{file_name}' получен")

    return 2


async def generate_wb_report(update: Update, context: CallbackContext) -> int:
    """Генерация отчета Wildberries"""
    user_data = context.user_data
    wb_files = user_data.get('wb_files', [])

    if not wb_files:
        await update.message.reply_text(
            "❌ Не получены файлы для формирования отчета!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Обрабатываю файлы Wildberries...")

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
            os.remove(file_path)
        os.remove(report_path)

    except Exception as e:
        logger.error(f"Ошибка обработки Wildberries: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при обработки файлов Wildberries: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END


def process_wb_sales(file_path):
    """Обработка файла продаж Wildberries"""
    # Поиск нужных столбцов
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
        except:
            continue

    # Сбор данных
    purchases = {}  # Выкупы
    orders = {}  # Заказы
    income = {}  # Начисления
    cancels = {}  # Отмены

    for _, row in df.iterrows():
        art = str(row['Артикул продавца']).strip().lower()
        if not art or art == 'nan':
            continue

        # Получаем значения
        ordered = row['шт.']
        purchased = row['Выкупили, шт.']
        amount = row['К перечислению за товар, руб.']

        # Если значения не числа, пропускаем
        if not isinstance(ordered, (int, float)) or not isinstance(purchased, (int, float)):
            continue

        # Суммируем данные
        orders[art] = orders.get(art, 0) + ordered
        purchases[art] = purchases.get(art, 0) + purchased
        income[art] = income.get(art, 0) + amount

    # Рассчитываем отмены
    for art in orders:
        cancels[art] = orders[art] - purchases[art]

    return purchases, cancels, income


def group_wb_data(purchases, cancels, income, art_to_id, id_to_name):
    """Группировка данных Wildberries по шаблону"""
    all_arts = set(purchases.keys()) | set(cancels.keys()) | set(income.keys())

    grouped = {}
    unmatched = {}

    for art in all_arts:
        # Поиск соответствия в шаблоне
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
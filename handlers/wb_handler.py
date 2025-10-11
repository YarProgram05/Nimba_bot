# handlers/wb_handler.py

import sys
import os
import pandas as pd
import logging
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler

# Настройка путей
utils_path = os.path.join(os.path.dirname(__file__), '..', 'utils')
if utils_path not in sys.path:
    sys.path.append(utils_path)

# Импорты из utils
from utils.template_loader import get_cabinet_articles_by_template_id
from utils.excel_utils import create_report

from states import WB_REPORT_CABINET_CHOICE, WB_REPORT_FILES

logger = logging.getLogger(__name__)


# === ШАГ 1: ВЫБОР КАБИНЕТА ===
async def start_wb_report(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета WB для отчёта по продажам"""
    keyboard = [
        [InlineKeyboardButton("🏪 WB Nimba (Кабинет 1)", callback_data='wb_sales_cabinet_1')],
        [InlineKeyboardButton("🏬 WB Galioni (Кабинет 2)", callback_data='wb_sales_cabinet_2')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🏢 Выберите кабинет Wildberries для отчёта по продажам:",
        reply_markup=reply_markup
    )
    return WB_REPORT_CABINET_CHOICE


# === ШАГ 2: ОБРАБОТКА ВЫБОРА ===
async def handle_wb_sales_cabinet_choice(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == 'wb_sales_cabinet_1':
        cabinet_name = "WB Nimba"
        sheet_name = "Отдельно ВБ Nimba"
    elif query.data == 'wb_sales_cabinet_2':
        cabinet_name = "WB Galioni"
        sheet_name = "Отдельно ВБ Galioni"
    else:
        await query.message.reply_text("❌ Неизвестный кабинет.")
        return ConversationHandler.END

    context.user_data['wb_sales_cabinet'] = cabinet_name
    context.user_data['wb_sales_sheet'] = sheet_name

    await query.message.edit_text(f"✅ Выбран кабинет: {cabinet_name}")

    # Клавиатура для отправки файла
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)

    await query.message.reply_text(
        "📤 Пожалуйста, отправьте файл продаж Wildberries:\n\n"
        "📎 Название файла должно содержать 'продажи' (например, 'ВБ_продажи.xlsx')\n\n"
        "После отправки всех файлов нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    context.user_data['wb_files'] = []
    return WB_REPORT_FILES


# === ШАГ 3: ПРИЁМ ФАЙЛОВ ===
async def handle_wb_files(update: Update, context: CallbackContext) -> int:
    document = update.message.document
    file_name = document.file_name

    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return WB_REPORT_FILES

    file = await context.bot.get_file(document)
    file_path = f"temp_{file_name}"
    await file.download_to_drive(file_path)

    context.user_data.setdefault('wb_files', []).append(file_path)

    await update.message.reply_text(
        f"✅ Файл продаж '{file_name}' получен для {context.user_data['wb_sales_cabinet']}.\n"
        "Нажмите «Все файлы отправлены», если готовы сформировать отчёт.",
        reply_markup=ReplyKeyboardMarkup([["Все файлы отправлены"]], resize_keyboard=True)
    )
    return WB_REPORT_FILES


# === ШАГ 4: ГЕНЕРАЦИЯ ОТЧЁТА ===
async def generate_wb_report(update: Update, context: CallbackContext) -> int:
    user_data = context.user_data
    wb_files = user_data.get('wb_files', [])
    sheet_name = user_data.get('wb_sales_sheet')

    if not wb_files or not sheet_name:
        await update.message.reply_text("❌ Данные повреждены. Начните сначала.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Обрабатываю файлы продаж Wildberries...")

        # Загрузка шаблона с использованием нового template_loader
        template_id_to_name, template_id_to_cabinet_arts = get_cabinet_articles_by_template_id(sheet_name)

        # Получаем main_ids_ordered — ID в порядке появления в Excel (без дубликатов)
        template_path = os.path.join(os.path.dirname(__file__), '..', "База данных артикулов для выкупов и начислений.xlsx")
        if not os.path.exists(template_path):
            template_path = "База данных артикулов для выкупов и начислений.xlsx"
        df_order = pd.read_excel(template_path, sheet_name=sheet_name)
        main_ids_ordered = []
        seen = set()
        for _, row in df_order.iterrows():
            if not pd.isna(row.get('ID')):
                tid = int(row['ID'])
                if tid not in seen:
                    main_ids_ordered.append(tid)
                    seen.add(tid)

        # Построение art_to_id из template_id_to_cabinet_arts
        art_to_id = {}
        for template_id, cabinet_arts in template_id_to_cabinet_arts.items():
            for art in cabinet_arts:
                clean_art = str(art).strip().lower()
                art_to_id[clean_art] = template_id

        id_to_name = template_id_to_name

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

        # Группировка данных (как в старом файле)
        grouped, unmatched = group_wb_data(
            all_purchases,
            all_cancels,
            all_income,
            art_to_id,
            id_to_name
        )

        # Создание отчета с использованием старого excel_utils.create_report
        report_path = f"WB_Report_{sheet_name.replace(' ', '_')}.xlsx"
        create_report(
            grouped,
            unmatched,
            id_to_name,
            main_ids_ordered,
            report_path
        )

        await update.message.reply_document(
            document=open(report_path, 'rb'),
            caption=f"📊 Отчет по продажам Wildberries\nКабинет: {user_data['wb_sales_cabinet']}",
            reply_markup=ReplyKeyboardRemove()
        )

        # Очистка
        for fp in wb_files + [report_path]:
            if os.path.exists(fp):
                os.remove(fp)

    except Exception as e:
        logger.error(f"Ошибка обработки WB продаж: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {str(e)}", reply_markup=ReplyKeyboardRemove())

    return ConversationHandler.END


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (как в старом файле) ===

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
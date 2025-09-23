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


async def start_ozon_report(update: Update, context: CallbackContext) -> int:
    """Начало обработки отчета Ozon"""
    context.user_data['ozon_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте файлы для Ozon в следующем порядке:\n\n"
        "1. Файлы выкупов (с названием вида 'Озон_Выкупы_*')\n"
        "2. Файл начислений ('Озон_Начисления')\n\n"
        "После отправки всех файлов нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return 1  # Состояние ожидания файлов


async def handle_ozon_files(update: Update, context: CallbackContext) -> int:
    """Обработка файлов Ozon"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return 1

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_{file_name}"
    await file.download_to_drive(file_path)

    # Определение типа файла
    if "Озон_Выкупы" in file_name:
        user_data.setdefault('ozon_files', []).append(file_path)
        await update.message.reply_text(f"✅ Файл выкупов '{file_name}' получен")
    elif "Озон_Начисления" in file_name:
        if user_data.get('ozon_income_file'):
            await update.message.reply_text("⚠️ Файл начислений уже был получен. Заменяю...")
        user_data['ozon_income_file'] = file_path
        await update.message.reply_text(f"✅ Файл начислений '{file_name}' получен")
    else:
        await update.message.reply_text("⚠️ Неопознанный файл. Отправьте файлы выкупов или начислений")
        os.remove(file_path)

    return 1


async def generate_ozon_report(update: Update, context: CallbackContext) -> int:
    """Генерация отчета Ozon"""
    user_data = context.user_data
    purchase_files = user_data.get('ozon_files', [])
    income_file = user_data.get('ozon_income_file')

    if not purchase_files or not income_file:
        await update.message.reply_text(
            "❌ Не хватает файлов для формирования отчета!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Обрабатываю файлы Ozon...")

        # Загрузка шаблона
        art_to_id, id_to_name, main_ids_ordered = load_template("Шаблон_Ozon")

        # Обработка выкупов
        all_purchases = {}
        all_cancels = {}

        for file_path in purchase_files:
            purchases, cancels = process_ozon_purchases(file_path)
            for art, count in purchases.items():
                all_purchases[art] = all_purchases.get(art, 0) + count
            for art, count in cancels.items():
                all_cancels[art] = all_cancels.get(art, 0) + count

        # Обработка начислений
        income = process_ozon_income(income_file)

        # Группировка данных
        grouped, unmatched = group_ozon_data(
            all_purchases,
            all_cancels,
            income,
            art_to_id,
            id_to_name
        )

        # Создание отчета
        report_path = "Ozon_Report.xlsx"
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
            caption="📊 Отчет по продажам Ozon",
            reply_markup=ReplyKeyboardRemove()
        )

        # Очистка временных файлов
        for file_path in purchase_files:
            os.remove(file_path)
        os.remove(income_file)
        os.remove(report_path)

    except Exception as e:
        logger.error(f"Ошибка обработки Ozon: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при обработке файлов Ozon: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END


def process_ozon_purchases(file_path):
    """Обработка файлов выкупов Ozon"""
    # Поиск заголовков в первых 10 строках
    for i in range(10):
        try:
            df = pd.read_excel(file_path, header=i)
            if 'Статус' in df.columns and 'Артикул' in df.columns:
                break
        except:
            continue

    purchases = {}
    cancels = {}

    for _, row in df.iterrows():
        status = str(row['Статус']).strip()
        art = str(row['Артикул']).strip().lower() if not pd.isna(row['Артикул']) else None

        if not art:
            continue

        if status == 'Доставлен':
            purchases[art] = purchases.get(art, 0) + 1
        elif status == 'Отменён':
            cancels[art] = cancels.get(art, 0) + 1

    return purchases, cancels


def process_ozon_income(file_path):
    """Обработка файла начислений Ozon с учетом всех требований"""
    # Поиск нужных столбцов в первых 10 строках
    for i in range(10):
        try:
            df = pd.read_excel(file_path, header=i)

            # Проверяем наличие необходимых столбцов
            if 'Сумма итого, руб.' in df.columns:
                # Определяем столбцы для артикула и типа начисления
                art_col = None
                type_col = None

                # Поиск столбца с артикулом
                for col in ['Артикул', 'артикул', 'Артикул продавца', 'артикул продавца']:
                    if col in df.columns:
                        art_col = col
                        break

                # Поиск столбца с типом начисления
                for col in ['Тип начисления', 'тип начисления', 'Группа услуг', 'группа услуг']:
                    if col in df.columns:
                        type_col = col
                        break

                if art_col or type_col:
                    # Сортировка по артикулу от А до Я
                    if art_col:
                        df = df.sort_values(by=art_col, ascending=True)
                    break
        except:
            continue

    income = {}

    for _, row in df.iterrows():
        # Получаем сумму начислений
        amount = row['Сумма итого, руб.']
        if pd.isna(amount):
            continue

        # Пробуем получить артикул
        art = None
        if art_col and not pd.isna(row[art_col]):
            art = str(row[art_col]).strip().lower()

        # Если артикул не найден, используем тип начисления
        if not art and type_col and not pd.isna(row[type_col]):
            type_val = str(row[type_col]).strip().lower()
            art = f"ТИП_НАЧИСЛЕНИЯ: {type_val}"

        # Если ни артикул, ни тип начисления не найдены, пропускаем строку
        if not art:
            continue

        # Учитываем все начисления (положительные и отрицательные)
        income[art] = income.get(art, 0) + amount

    return income


def group_ozon_data(purchases, cancels, income, art_to_id, id_to_name):
    all_arts = set(purchases.keys()) | set(cancels.keys()) | set(income.keys())

    grouped = {}
    unmatched = {}

    for art in all_arts:
        # Для типов начислений всегда считаем как несоответствующие
        if art.startswith("ТИП_НАЧИСЛЕНИЯ:"):
            unmatched[art] = {
                'name': art,
                'purchases': purchases.get(art, 0),
                'cancels': cancels.get(art, 0),
                'income': income.get(art, 0)
            }
            continue

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
                'name': f"НЕОПОЗНАННЫЙ_АРТИКУЛ: {art}",
                'purchases': purchases.get(art, 0),
                'cancels': cancels.get(art, 0),
                'income': income.get(art, 0)
            }

    return grouped, unmatched

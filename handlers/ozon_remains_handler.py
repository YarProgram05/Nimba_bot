import sys
import os
import pandas as pd
import logging
import json
import time
import requests
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext, ConversationHandler
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter

# Настройка путей
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')

if root_dir not in sys.path:
    sys.path.append(root_dir)
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

logger = logging.getLogger(__name__)

# Состояния
OZON_REMAINS_CABINET_CHOICE = 7
OZON_REMAINS_FILES = 3
OZON_REMAINS_REPORT_TYPE = 4


# ======================
# Ozon API Класс
# ======================
class OzonAPI:
    def __init__(self, cabinet_id=1):
        from dotenv import load_dotenv
        load_dotenv()

        if cabinet_id == 1:
            self.client_id = os.getenv('OZON_CLIENT_ID_1')
            self.api_key = os.getenv('OZON_API_KEY_1')
        elif cabinet_id == 2:
            self.client_id = os.getenv('OZON_CLIENT_ID_2')
            self.api_key = os.getenv('OZON_API_KEY_2')
        else:
            raise ValueError("Поддерживаются только cabinet_id 1 или 2")

        if not self.client_id or not self.api_key:
            raise ValueError(f"❌ OZON_CLIENT_ID или OZON_API_KEY не заданы в .env для кабинета {cabinet_id}")

        self.base_url = "https://api-seller.ozon.ru"
        self.headers = {
            'Client-Id': self.client_id,
            'Api-Key': self.api_key,
            'Content-Type': 'application/json'
        }

    def get_product_list(self, limit=1000, last_id=""):
        url = f"{self.base_url}/v3/product/list"
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": limit}
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Ошибка при получении списка товаров: {e}")
            return None

    def get_product_info_list(self, offer_ids=None, product_ids=None, skus=None):
        url = f"{self.base_url}/v3/product/info/list"
        payload = {
            "offer_id": offer_ids or [],
            "product_id": product_ids or [],
            "sku": skus or []
        }
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Ошибка при получении информации о товарах: {e}")
            return None

    def get_analytics_stocks(self, sku_list):
        url = f"{self.base_url}/v1/analytics/stocks"
        sku_list_clean = []
        for sku in sku_list:
            try:
                sku_list_clean.append(int(float(sku)))
            except (ValueError, TypeError):
                continue

        if not sku_list_clean:
            return []

        payload = {
            "skus": sku_list_clean,
            "turnover_grades": [
                "TURNOVER_GRADE_NONE", "DEFICIT", "POPULAR", "ACTUAL", "SURPLUS",
                "NO_SALES", "WAS_NO_SALES", "RESTRICTED_NO_SALES", "COLLECTING_DATA",
                "WAITING_FOR_SUPPLY", "WAS_DEFICIT", "WAS_POPULAR", "WAS_ACTUAL", "WAS_SURPLUS"
            ]
        }

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get('items', [])
        except Exception as e:
            logger.error(f"Ошибка при получении аналитики остатков: {e}")
            return []


def clean_offer_id(offer_id_raw):
    """Только очищает от невидимых символов, НЕ меняет регистр"""
    try:
        if not offer_id_raw:
            return None
        s = str(offer_id_raw)
        s = ''.join(c for c in s if c.isprintable())
        s = s.strip()
        return s if s else None
    except Exception:
        return None


def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# ======================
# Обработчики
# ======================

async def start_ozon_remains(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета Ozon для остатков"""
    keyboard = [
        [InlineKeyboardButton("🏪 Озон_1 Nimba", callback_data='remains_cabinet_1')],
        [InlineKeyboardButton("🏬 Озон_2 Galioni", callback_data='remains_cabinet_2')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🏢 Выберите кабинет Ozon для выгрузки остатков:",
        reply_markup=reply_markup
    )

    return OZON_REMAINS_CABINET_CHOICE

async def handle_cabinet_choice(update: Update, context: CallbackContext) -> int:
    """Обработка выбора кабинета Ozon"""
    query = update.callback_query
    await query.answer()

    cabinet_data = query.data  # cabinet_1 или cabinet_2
    cabinet_id = 1 if cabinet_data == 'cabinet_1' else 2

    # Сохраняем выбор в user_data
    context.user_data['ozon_cabinet_id'] = cabinet_id

    await query.message.edit_text(f"⏳ Получаю остатки с Ozon API (Озон {cabinet_id})...")

    try:
        # 1️⃣ Инициализируем API с выбранным кабинетом
        ozon = OzonAPI(cabinet_id=cabinet_id)

        # 2️⃣ Получаем список товаров
        product_list = ozon.get_product_list(limit=1000)
        if not product_list:
            raise Exception("Не удалось получить список товаров")

        items = product_list.get('result', {}).get('items', [])
        if not items:
            raise Exception("Товары не найдены")

        # 3️⃣ Собираем offer_id
        offer_ids = []
        for item in items:
            offer_id = clean_offer_id(item.get('offer_id'))
            if offer_id:
                offer_ids.append(offer_id)

        # 4️⃣ Получаем SKU
        all_skus = []
        offer_id_to_name = {}

        for chunk in chunk_list(offer_ids, 1000):
            product_info_response = ozon.get_product_info_list(offer_ids=chunk)
            if not product_info_response:
                continue

            items_in_response = []
            if 'result' in product_info_response and 'items' in product_info_response['result']:
                items_in_response = product_info_response['result']['items']
            elif 'items' in product_info_response:
                items_in_response = product_info_response['items']
            elif isinstance(product_info_response.get('result'), list):
                items_in_response = product_info_response['result']
            else:
                continue

            for item_info in items_in_response:
                offer_id = clean_offer_id(item_info.get('offer_id'))
                sku = item_info.get('sku')
                name = item_info.get('name', '—')
                if offer_id and sku:
                    all_skus.append(sku)
                    offer_id_to_name[offer_id] = name

            time.sleep(0.5)

        if not all_skus:
            raise Exception("Не удалось получить SKU")

        # 5️⃣ Получаем аналитику остатков
        stock_dict = {}

        for sku_chunk in chunk_list(all_skus, 100):
            items = ozon.get_analytics_stocks(sku_chunk)
            for item in items:
                offer_id = clean_offer_id(item.get('offer_id'))
                if not offer_id:
                    continue

                if offer_id in stock_dict:
                    stock_dict[offer_id]['available_stock_count'] += item.get('available_stock_count', 0)
                    stock_dict[offer_id]['return_from_customer_stock_count'] += item.get('return_from_customer_stock_count', 0)
                    stock_dict[offer_id]['other_stock_count'] += item.get('other_stock_count', 0)
                else:
                    stock_dict[offer_id] = {
                        'name': item.get('name', offer_id_to_name.get(offer_id, '—')),
                        'available_stock_count': item.get('available_stock_count', 0),
                        'return_from_customer_stock_count': item.get('return_from_customer_stock_count', 0),
                        'other_stock_count': item.get('other_stock_count', 0)
                    }
            time.sleep(0.5)

        # 6️⃣ Fallback для отсутствующих
        missing_offer_ids = list(set(offer_ids) - set(stock_dict.keys()))
        if missing_offer_ids:
            for chunk in chunk_list(missing_offer_ids, 100):
                info_response = ozon.get_product_info_list(offer_ids=chunk)
                if not info_response:
                    continue

                items_in_response = []
                if 'result' in info_response and 'items' in info_response['result']:
                    items_in_response = info_response['result']['items']
                elif 'items' in info_response:
                    items_in_response = info_response['items']
                elif isinstance(info_response.get('result'), list):
                    items_in_response = info_response['result']
                else:
                    continue

                for item in items_in_response:
                    offer_id = clean_offer_id(item.get('offer_id'))
                    if not offer_id:
                        continue

                    stocks = item.get('stocks', {})
                    name = item.get('name', '—')
                    stock_dict[offer_id] = {
                        'name': name,
                        'available_stock_count': stocks.get('present', 0),
                        'return_from_customer_stock_count': 0,
                        'other_stock_count': stocks.get('reserved', 0)
                    }

                time.sleep(0.5)

        # ✅ Сохраняем данные в context
        context.user_data['ozon_stock_dict'] = stock_dict
        context.user_data['offer_id_to_name'] = offer_id_to_name

        # ➡️ Отправляем кнопки выбора отчёта
        keyboard = [
            [InlineKeyboardButton("📊 Исходные артикулы (как в Ozon)", callback_data='raw')],
            [InlineKeyboardButton("🧩 Группировка по шаблону Nimba", callback_data='template')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.message.reply_text(
            "✅ Данные получены! Выберите формат отчёта:",
            reply_markup=reply_markup
        )

        return OZON_REMAINS_REPORT_TYPE

    except Exception as e:
        logger.error(f"Ошибка при получении данных: {str(e)}", exc_info=True)
        await query.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

async def handle_report_type_choice(update: Update, context: CallbackContext) -> int:
    """Обработка выбора типа отчёта"""
    query = update.callback_query
    await query.answer()

    report_type = query.data
    stock_dict = context.user_data.get('ozon_stock_dict', {})
    offer_id_to_name = context.user_data.get('offer_id_to_name', {})

    try:
        if report_type == 'raw':
            # 📄 Отчёт по исходным артикулам
            report_data = []
            for offer_id, data in stock_dict.items():
                name = data['name']
                available = data['available_stock_count']
                returning = data['return_from_customer_stock_count']
                prepare = data['other_stock_count']
                total = available + returning + prepare

                report_data.append({
                    'Наименование': name,
                    'Артикул': offer_id,
                    'Доступно на складах': available,
                    'Возвращаются от покупателей': returning,
                    'Подготовка к продаже': prepare,
                    'Итого на МП': total
                })

            # ✅ Сортировка по "Наименование" от А до Я
            df = pd.DataFrame(report_data).sort_values(by='Наименование', key=lambda x: x.str.lower()).reset_index(drop=True)
            headers = ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже", "Итого на МП"]

        elif report_type == 'template':
            # 📄 Отчёт по шаблону — БЕЗ столбца "Наименование"
            template_path = os.path.join(root_dir, "База данных артикулов для выкупов и начислений.xlsx")
            if not os.path.exists(template_path):
                template_path = "База данных артикулов для выкупов и начислений.xlsx"

            if not os.path.exists(template_path):
                raise Exception("Файл шаблона не найден!")

            import importlib.util
            spec = importlib.util.spec_from_file_location("template_loader",
                                                          os.path.join(utils_dir, "template_loader.py"))
            template_loader = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(template_loader)

            art_to_id, id_to_name, main_ids_ordered = template_loader.load_template("Шаблон_Ozon")

            stock_data = {}
            for offer_id, data in stock_dict.items():
                stock_data[offer_id] = {
                    "available": data['available_stock_count'],
                    "returning": data['return_from_customer_stock_count'],
                    "prepare": data['other_stock_count']
                }

            grouped, unmatched = group_ozon_remains_data(stock_data, art_to_id, id_to_name)

            report_data = []

            for id_val in main_ids_ordered:
                if id_val in grouped:
                    data = grouped[id_val]
                    total = data['available'] + data['returning'] + data['prepare']
                    report_data.append({
                        'Артикул': data['name'],
                        'Доступно на складах': data['available'],
                        'Возвращаются от покупателей': data['returning'],
                        'Подготовка к продаже': data['prepare'],
                        'Итого на МП': total
                    })
                else:
                    name = id_to_name.get(id_val, f"ID {id_val}")
                    report_data.append({
                        'Артикул': name,
                        'Доступно на складах': 0,
                        'Возвращаются от покупателей': 0,
                        'Подготовка к продаже': 0,
                        'Итого на МП': 0
                    })

            for art, data in unmatched.items():
                total = data['available'] + data['returning'] + data['prepare']
                report_data.append({
                    'Артикул': f"НЕОПОЗНАННЫЙ: {art}",
                    'Доступно на складах': data['available'],
                    'Возвращаются от покупателей': data['returning'],
                    'Подготовка к продаже': data['prepare'],
                    'Итого на МП': total
                })

            df = pd.DataFrame(report_data)
            headers = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже", "Итого на МП"]

        else:
            raise ValueError("Неизвестный тип отчёта")

        # ✅ Создаём Excel с форматированием
        report_path = "Ozon_Remains_Report.xlsx"
        create_formatted_excel(df, headers, report_path)

        # 📤 Отправляем файл
        await query.message.reply_document(
            document=open(report_path, 'rb'),
            caption=f"📊 Отчёт по остаткам Ozon ({'исходные артикулы' if report_type == 'raw' else 'шаблон Nimba'})",
            reply_markup=ReplyKeyboardRemove()
        )

        # 🧹 Очистка
        if os.path.exists(report_path):
            os.remove(report_path)

    except Exception as e:
        logger.error(f"Ошибка при генерации отчёта: {str(e)}", exc_info=True)
        await query.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END


def group_ozon_remains_data(stock_data, art_to_id, id_to_name):
    """Группировка данных остатков Ozon по шаблону"""
    all_arts = set(stock_data.keys())

    grouped = {}
    unmatched = {}

    for art in all_arts:
        art_clean = str(art).strip().lower()
        group_id = art_to_id.get(art_clean, None)

        if group_id is not None:
            group_name = id_to_name.get(group_id, art)

            if group_id not in grouped:
                grouped[group_id] = {
                    'name': group_name,
                    'available': 0,
                    'returning': 0,
                    'prepare': 0
                }

            grouped[group_id]['available'] += stock_data[art]["available"]
            grouped[group_id]['returning'] += stock_data[art]["returning"]
            grouped[group_id]['prepare'] += stock_data[art]["prepare"]
        else:
            unmatched[art] = {
                'name': f"НЕОПОЗНАННЫЙ: {art}",
                'available': stock_data[art]["available"],
                'returning': stock_data[art]["returning"],
                'prepare': stock_data[art]["prepare"]
            }

    return grouped, unmatched


def create_formatted_excel(df, headers, filename):
    """Создаёт Excel с форматированием: жирные заголовки, автоподбор ширины, суммы, границы, выравнивание"""
    wb = Workbook()
    ws = wb.active
    ws.title = "Остатки"

    # Стили
    bold_font = Font(bold=True)
    center_alignment = Alignment(horizontal='center', vertical='center')
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    # Записываем заголовки
    ws.append(headers)

    # Жирный + выравнивание по центру для заголовков
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = bold_font
        cell.alignment = center_alignment
        cell.border = thin_border

    # Объединяем ячейки для столбца "Артикул" (A1:A2) — ВО ВСЕХ ОТЧЁТАХ
    ws.merge_cells('A1:A2')

    # Если есть "Наименование" — объединяем и B1:B2
    if "Наименование" in headers:
        ws.merge_cells('B1:B2')

    # Определяем, с какой строки начинаются данные
    data_start_row = 3
    sum_row = 2

    # Записываем данные
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False), data_start_row):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.alignment = center_alignment
            cell.border = thin_border

    # ✅ Добавляем суммы — ТОЛЬКО для числовых столбцов
    num_rows = len(df)
    if num_rows > 0:
        # Определяем, с какого столбца начинать суммы:
        # - Если есть "Наименование" → суммы с 3-го столбца (C)
        # - Если нет → суммы с 2-го столбца (B)
        start_col_index = 3 if "Наименование" in headers else 2

        for col in range(start_col_index, len(headers) + 1):
            col_letter = get_column_letter(col)
            formula = f"=SUM({col_letter}{data_start_row}:{col_letter}{data_start_row + num_rows - 1})"
            cell = ws.cell(row=sum_row, column=col, value=formula)
            cell.font = bold_font
            cell.alignment = center_alignment
            cell.border = thin_border

    # Автоподбор ширины столбцов
    for col in range(1, len(headers) + 1):
        max_length = 0
        column = get_column_letter(col)
        for cell in ws[column]:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column].width = adjusted_width

    wb.save(filename)
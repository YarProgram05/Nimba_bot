import sys
import os
import pandas as pd
import logging
import time
import requests
from telegram import Update, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
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

from states import OZON_REMAINS_CABINET_CHOICE

# Импорт новой функции из template_loader
from utils.template_loader import get_cabinet_articles_by_template_id


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
# Нормализация и группировка
# ======================

def normalize_art(art_str):
    """Нормализует строку: приводит к нижнему регистру, удаляет лишние пробелы, очищает от невидимых символов"""
    if not art_str:
        return ""
    s = str(art_str)
    s = ''.join(c for c in s if c.isprintable())
    s = s.strip().lower()
    return s


def group_ozon_remains_data(stock_data, template_id_to_cabinet_arts, template_id_to_name):
    """
    Группирует данные остатков по шаблонным артикулам.

    :param stock_data: dict {offer_id: {"available": ..., "returning": ..., "prepare": ...}}
    :param template_id_to_cabinet_arts: dict {template_id: [cabinet_art1, cabinet_art2, ...]}
    :param template_id_to_name: dict {template_id: "Шаблонное название"}
    :return: grouped (по template_id), unmatched (артикулы без привязки)
    """
    stock_data_clean = {}
    for art, data in stock_data.items():
        clean_art = normalize_art(art)
        if clean_art:
            stock_data_clean[clean_art] = data

    cabinet_art_to_template_id = {}
    for template_id, arts in template_id_to_cabinet_arts.items():
        for art in arts:
            clean_art = normalize_art(art)
            if clean_art:
                cabinet_art_to_template_id[clean_art] = template_id

    grouped = {}
    unmatched = {}

    for clean_art, data in stock_data_clean.items():
        template_id = cabinet_art_to_template_id.get(clean_art)

        if template_id is not None:
            if template_id not in grouped:
                grouped[template_id] = {
                    'name': template_id_to_name.get(template_id, f"ID {template_id}"),
                    'available': 0,
                    'returning': 0,
                    'prepare': 0
                }
            grouped[template_id]['available'] += data['available']
            grouped[template_id]['returning'] += data['returning']
            grouped[template_id]['prepare'] += data['prepare']
        else:
            unmatched[clean_art] = {
                'name': f"НЕОПОЗНАННЫЙ: {clean_art}",
                'available': data['available'],
                'returning': data['returning'],
                'prepare': data['prepare']
            }

    return grouped, unmatched


# ======================
# Обработчики
# ======================

async def start_ozon_remains(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета Ozon"""
    context.user_data['current_flow'] = 'remains'

    keyboard = [
        [InlineKeyboardButton("🏪 Озон_1 Nimba", callback_data='cabinet_1')],
        [InlineKeyboardButton("🏬 Озон_2 Galioni", callback_data='cabinet_2')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🏢 Выберите кабинет Ozon для выгрузки остатков:",
        reply_markup=reply_markup
    )

    return OZON_REMAINS_CABINET_CHOICE


async def handle_cabinet_choice(update: Update, context: CallbackContext) -> int:
    """Обработка выбора кабинета Ozon — сразу генерируем оба отчёта"""
    query = update.callback_query
    await query.answer()

    cabinet_data = query.data
    cabinet_id = 1 if cabinet_data == 'cabinet_1' else 2
    context.user_data['ozon_cabinet_id'] = cabinet_id

    loading_message = await query.message.edit_text(f"⏳ Получаю остатки с Ozon API (Озон {cabinet_id})...")
    context.user_data['ozon_remains_loading_message_id'] = loading_message.message_id

    try:
        ozon = OzonAPI(cabinet_id=cabinet_id)

        # --- Получение данных ---
        product_list = ozon.get_product_list(limit=1000)
        if not product_list:
            raise Exception("Не удалось получить список товаров")

        items = product_list.get('result', {}).get('items', [])
        if not items:
            raise Exception("Товары не найдены")

        offer_ids = []
        for item in items:
            offer_id = clean_offer_id(item.get('offer_id'))
            if offer_id:
                offer_ids.append(offer_id)

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

        stock_dict = {}

        for sku_chunk in chunk_list(all_skus, 100):
            items = ozon.get_analytics_stocks(sku_chunk)
            for item in items:
                offer_id = clean_offer_id(item.get('offer_id'))
                if not offer_id:
                    continue

                if offer_id in stock_dict:
                    stock_dict[offer_id]['available_stock_count'] += item.get('available_stock_count', 0)
                    stock_dict[offer_id]['return_from_customer_stock_count'] += item.get(
                        'return_from_customer_stock_count', 0)
                    stock_dict[offer_id]['valid_stock_count'] += item.get('valid_stock_count', 0)
                else:
                    stock_dict[offer_id] = {
                        'name': item.get('name', offer_id_to_name.get(offer_id, '—')),
                        'available_stock_count': item.get('available_stock_count', 0),
                        'return_from_customer_stock_count': item.get('return_from_customer_stock_count', 0),
                        'valid_stock_count': item.get('valid_stock_count', 0)
                    }
            time.sleep(0.5)

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
                        'valid_stock_count': stocks.get('reserved', 0)
                    }

                time.sleep(0.5)

        # === 1. Отчёт по исходным артикулам ===
        raw_data = []
        for offer_id, data in stock_dict.items():
            name = data['name']
            available = data['available_stock_count']
            returning = data['return_from_customer_stock_count']
            prepare = data['valid_stock_count']
            total = available + returning + prepare
            raw_data.append({
                'Наименование': name,
                'Артикул': offer_id,
                'Доступно на складах': available,
                'Возвращаются от покупателей': returning,
                'Подготовка к продаже': prepare,
                'Итого на МП': total
            })

        df_raw = pd.DataFrame(raw_data).sort_values(by='Наименование', key=lambda x: x.str.lower()).reset_index(
            drop=True)
        headers_raw = ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей",
                       "Подготовка к продаже", "Итого на МП"]

        # === 2. Отчёт по шаблону Nimba/Galioni ===
        sheet_name = "Отдельно Озон Nimba" if cabinet_id == 1 else "Отдельно Озон Galioni"

        template_id_to_name, template_id_to_cabinet_arts = get_cabinet_articles_by_template_id(sheet_name)

        # Получаем main_ids_ordered — ID в порядке появления в Excel (без дубликатов)
        template_path = os.path.join(root_dir, "База данных артикулов для выкупов и начислений.xlsx")
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

        # Подготовка stock_data
        stock_data = {}
        for offer_id, data in stock_dict.items():
            stock_data[offer_id] = {
                "available": data['available_stock_count'],
                "returning": data['return_from_customer_stock_count'],
                "prepare": data['valid_stock_count']
            }

        # Группировка по шаблонам
        grouped, unmatched = group_ozon_remains_data(
            stock_data,
            template_id_to_cabinet_arts,
            template_id_to_name
        )

        template_data = []
        for id_val in main_ids_ordered:
            if id_val in grouped:
                d = grouped[id_val]
                total = d['available'] + d['returning'] + d['prepare']
                template_data.append({
                    'Артикул': d['name'],
                    'Доступно на складах': d['available'],
                    'Возвращаются от покупателей': d['returning'],
                    'Подготовка к продаже': d['prepare'],
                    'Итого на МП': total
                })
            else:
                name = template_id_to_name.get(id_val, f"ID {id_val}")
                template_data.append({
                    'Артикул': name,
                    'Доступно на складах': 0,
                    'Возвращаются от покупателей': 0,
                    'Подготовка к продаже': 0,
                    'Итого на МП': 0
                })

        for art, d in unmatched.items():
            total = d['available'] + d['returning'] + d['prepare']
            template_data.append({
                'Артикул': f"НЕОПОЗНАННЫЙ: {art}",
                'Доступно на складах': d['available'],
                'Возвращаются от покупателей': d['returning'],
                'Подготовка к продаже': d['prepare'],
                'Итого на МП': total
            })

        df_template = pd.DataFrame(template_data)
        headers_template = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже",
                            "Итого на МП"]

        # === Сводка по всем остаткам ===
        total_available = sum(data['available_stock_count'] for data in stock_dict.values())
        total_returning = sum(data['return_from_customer_stock_count'] for data in stock_dict.values())
        total_prepare = sum(data['valid_stock_count'] for data in stock_dict.values())
        total_mp = total_available + total_returning + total_prepare

        def fmt_num(x):
            return f"{x:,}".replace(",", " ")

        summary_text = (
            f"📊 <b>Сводка по остаткам Ozon</b>\n"
            f"Кабинет: <b>Озон {cabinet_id}</b>\n\n"
            f"📦 <b>Доступно на складах:</b> {fmt_num(total_available)} шт\n"
            f"↩️ <b>Возвращаются от покупателей:</b> {fmt_num(total_returning)} шт\n"
            f"🔄 <b>Подготовка к продаже:</b> {fmt_num(total_prepare)} шт\n"
            f"✅ <b>Итого на МП:</b> {fmt_num(total_mp)} шт"
        )

        # ✅ Создаём Excel с двумя листами
        report_path = "Ozon_Remains_Report.xlsx"
        create_excel_with_two_sheets(df_raw, headers_raw, df_template, headers_template, report_path)

        # 📤 Отправляем файл
        await query.message.reply_document(
            document=open(report_path, 'rb'),
            caption="📊 Отчёт по остаткам Ozon: два листа — исходные артикулы и шаблон Nimba/Galioni",
            reply_markup=ReplyKeyboardRemove()
        )

        # 💬 Отправляем сводку
        await query.message.reply_text(summary_text, parse_mode="HTML")

        # 🧹 Очистка
        if os.path.exists(report_path):
            os.remove(report_path)

        # Удаляем сообщение о загрузке
        chat_id = query.message.chat_id
        try:
            loading_msg_id = context.user_data.get('ozon_remains_loading_message_id')
            if loading_msg_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=loading_msg_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение о загрузке остатков: {e}")

    except Exception as e:
        logger.error(f"Ошибка при получении данных: {str(e)}", exc_info=True)
        await query.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )
        # Удаляем сообщение о загрузке даже при ошибке
        chat_id = query.message.chat_id
        try:
            loading_msg_id = context.user_data.get('ozon_remains_loading_message_id')
            if loading_msg_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=loading_msg_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение о загрузке остатков при ошибке: {e}")

    return ConversationHandler.END


def create_excel_with_two_sheets(df_raw, headers_raw, df_template, headers_template, filename):
    """Создаёт Excel с двумя листами: сначала 'Остатки шаблон Nimba', затем 'Остатки исходные артикулы'"""
    wb = Workbook()
    wb.remove(wb.active)  # удаляем дефолтный лист

    # Сначала — шаблон Nimba/Galioni
    ws1 = wb.create_sheet(title="Остатки шаблон Nimba")
    _write_sheet(ws1, df_template, headers_template, has_name=False)

    # Затем — исходные артикулы
    ws2 = wb.create_sheet(title="Остатки исходные артикулы")
    _write_sheet(ws2, df_raw, headers_raw, has_name=True)

    wb.save(filename)


def _write_sheet(ws, df, headers, has_name):
    """Вспомогательная функция для записи одного листа с форматированием"""
    bold_font = Font(bold=True)
    center_alignment = Alignment(horizontal='center', vertical='center')
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    # Заголовки
    ws.append(headers)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = bold_font
        cell.alignment = center_alignment
        cell.border = thin_border

    # Объединение ячеек в заголовке
    ws.merge_cells('A1:A2')
    if has_name:
        ws.merge_cells('B1:B2')

    data_start_row = 3
    sum_row = 2

    # Данные
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False), data_start_row):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.alignment = center_alignment
            cell.border = thin_border

    # Суммы
    num_rows = len(df)
    if num_rows > 0:
        start_col_index = 3 if has_name else 2
        for col in range(start_col_index, len(headers) + 1):
            col_letter = get_column_letter(col)
            formula = f"=SUM({col_letter}{data_start_row}:{col_letter}{data_start_row + num_rows - 1})"
            cell = ws.cell(row=sum_row, column=col, value=formula)
            cell.font = bold_font
            cell.alignment = center_alignment
            cell.border = thin_border

    # Автоподбор ширины
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


# ======================
# Автоматическая отправка отчёта (для job_queue)
# ======================

async def send_ozon_remains_automatic(context: CallbackContext):
    """Автоматическая отправка отчёта по остаткам Ozon для одного кабинета"""
    chat_id = context.job.data.get('chat_id')
    cabinet_id = context.job.data.get('cabinet_id', 1)

    try:
        ozon = OzonAPI(cabinet_id=cabinet_id)

        # --- Получение данных ---
        product_list = ozon.get_product_list(limit=1000)
        if not product_list:
            raise Exception("Не удалось получить список товаров")

        items = product_list.get('result', {}).get('items', [])
        if not items:
            raise Exception("Товары не найдены")

        offer_ids = []
        for item in items:
            offer_id = clean_offer_id(item.get('offer_id'))
            if offer_id:
                offer_ids.append(offer_id)

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

        stock_dict = {}

        for sku_chunk in chunk_list(all_skus, 100):
            items = ozon.get_analytics_stocks(sku_chunk)
            for item in items:
                offer_id = clean_offer_id(item.get('offer_id'))
                if not offer_id:
                    continue

                if offer_id in stock_dict:
                    stock_dict[offer_id]['available_stock_count'] += item.get('available_stock_count', 0)
                    stock_dict[offer_id]['return_from_customer_stock_count'] += item.get(
                        'return_from_customer_stock_count', 0)
                    stock_dict[offer_id]['valid_stock_count'] += item.get('valid_stock_count', 0)
                else:
                    stock_dict[offer_id] = {
                        'name': item.get('name', offer_id_to_name.get(offer_id, '—')),
                        'available_stock_count': item.get('available_stock_count', 0),
                        'return_from_customer_stock_count': item.get('return_from_customer_stock_count', 0),
                        'valid_stock_count': item.get('valid_stock_count', 0)
                    }
            time.sleep(0.5)

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
                        'valid_stock_count': stocks.get('reserved', 0)
                    }

                time.sleep(0.5)

        # === Подготовка данных ===
        raw_data = []
        for offer_id, data in stock_dict.items():
            name = data['name']
            available = data['available_stock_count']
            returning = data['return_from_customer_stock_count']
            prepare = data['valid_stock_count']
            total = available + returning + prepare
            raw_data.append({
                'Наименование': name,
                'Артикул': offer_id,
                'Доступно на складах': available,
                'Возвращаются от покупателей': returning,
                'Подготовка к продаже': prepare,
                'Итого на МП': total
            })

        df_raw = pd.DataFrame(raw_data).sort_values(by='Наименование', key=lambda x: x.str.lower()).reset_index(
            drop=True)
        headers_raw = ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей",
                       "Подготовка к продаже", "Итого на МП"]

        # === Шаблон Nimba/Galioni ===
        sheet_name = "Отдельно Озон Nimba" if cabinet_id == 1 else "Отдельно Озон Galioni"

        template_id_to_name, template_id_to_cabinet_arts = get_cabinet_articles_by_template_id(sheet_name)

        # Получаем main_ids_ordered
        template_path = os.path.join(root_dir, "База данных артикулов для выкупов и начислений.xlsx")
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

        stock_data = {}
        for offer_id, data in stock_dict.items():
            stock_data[offer_id] = {
                "available": data['available_stock_count'],
                "returning": data['return_from_customer_stock_count'],
                "prepare": data['valid_stock_count']
            }

        grouped, unmatched = group_ozon_remains_data(
            stock_data,
            template_id_to_cabinet_arts,
            template_id_to_name
        )

        template_data = []
        for id_val in main_ids_ordered:
            if id_val in grouped:
                d = grouped[id_val]
                total = d['available'] + d['returning'] + d['prepare']
                template_data.append({
                    'Артикул': d['name'],
                    'Доступно на складах': d['available'],
                    'Возвращаются от покупателей': d['returning'],
                    'Подготовка к продаже': d['prepare'],
                    'Итого на МП': total
                })
            else:
                name = template_id_to_name.get(id_val, f"ID {id_val}")
                template_data.append({
                    'Артикул': name,
                    'Доступно на складах': 0,
                    'Возвращаются от покупателей': 0,
                    'Подготовка к продаже': 0,
                    'Итого на МП': 0
                })

        for art, d in unmatched.items():
            total = d['available'] + d['returning'] + d['prepare']
            template_data.append({
                'Артикул': f"НЕОПОЗНАННЫЙ: {art}",
                'Доступно на складах': d['available'],
                'Возвращаются от покупателей': d['returning'],
                'Подготовка к продаже': d['prepare'],
                'Итого на МП': total
            })

        df_template = pd.DataFrame(template_data)
        headers_template = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже",
                            "Итого на МП"]

        # === Сводка ===
        total_available = sum(data['available_stock_count'] for data in stock_dict.values())
        total_returning = sum(data['return_from_customer_stock_count'] for data in stock_dict.values())
        total_prepare = sum(data['valid_stock_count'] for data in stock_dict.values())
        total_mp = total_available + total_returning + total_prepare

        def fmt_num(x):
            return f"{x:,}".replace(",", " ")

        summary_text = (
            f"📊 <b>Еженедельный отчёт по остаткам Ozon</b>\n"
            f"Кабинет: <b>Озон {cabinet_id}</b>\n"
            f"Дата: {time.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"📦 <b>Доступно на складах:</b> {fmt_num(total_available)} шт\n"
            f"↩️ <b>Возвращаются от покупателей:</b> {fmt_num(total_returning)} шт\n"
            f"🔄 <b>Подготовка к продаже:</b> {fmt_num(total_prepare)} шт\n"
            f"✅ <b>Итого на МП:</b> {fmt_num(total_mp)} шт"
        )

        # === Создание Excel ===
        report_path = f"Ozon_Remains_Weekly_Cabinet{cabinet_id}_{int(time.time())}.xlsx"
        create_excel_with_two_sheets(df_raw, headers_raw, df_template, headers_template, report_path)

        # === Отправка ===
        await context.bot.send_document(
            chat_id=chat_id,
            document=open(report_path, 'rb'),
            caption=f"📊 Еженедельный отчёт: Ozon Кабинет {cabinet_id}",
        )
        await context.bot.send_message(chat_id=chat_id, text=summary_text, parse_mode="HTML")

        # === Очистка ===
        if os.path.exists(report_path):
            os.remove(report_path)

    except Exception as e:
        logger.error(f"Ошибка в автоматическом отчёте для кабинета {cabinet_id}: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при генерации отчёта для Ozon Кабинет {cabinet_id}: {str(e)}"
        )
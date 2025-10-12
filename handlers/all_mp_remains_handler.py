# handlers/all_mp_remains_handler.py

import os
import sys
import shutil
import logging
import pandas as pd
import time
from telegram import Update, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')

if root_dir not in sys.path:
    sys.path.append(root_dir)
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

logger = logging.getLogger(__name__)

from states import ALL_MP_REMAINS
from handlers.ozon_remains_handler import OzonAPI
from handlers.wb_remains_handler import WildberriesAPI
from handlers.ozon_remains_handler import clean_offer_id
from handlers.wb_remains_handler import clean_article


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ: СЫРЫЕ ДАННЫЕ ===

async def fetch_ozon_remains_raw(cabinet_id):
    """Полностью копируем логику из handle_cabinet_choice для надежности"""
    ozon = OzonAPI(cabinet_id=cabinet_id)

    # --- Получение данных (точно как в рабочей функции) ---
    product_list = ozon.get_product_list(limit=1000)
    if not product_list:
        logger.warning(f"Ozon кабинет {cabinet_id}: не удалось получить список товаров")
        return {}, []

    items = product_list.get('result', {}).get('items', [])
    if not items:
        logger.warning(f"Ozon кабинет {cabinet_id}: товары не найдены")
        return {}, []

    offer_ids = []
    for item in items:
        offer_id = clean_offer_id(item.get('offer_id'))
        if offer_id:
            offer_ids.append(offer_id)

    all_skus = []
    offer_id_to_name = {}

    from handlers.ozon_remains_handler import chunk_list
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

    if not all_skus:
        logger.warning(f"Ozon кабинет {cabinet_id}: не удалось получить SKU")
        return {}, []

    # === АГРЕГАЦИЯ СЫРЫХ ДАННЫХ ПО АРТИКУЛАМ ===
    raw_stock_dict = {}  # Для агрегации сырых данных

    for sku_chunk in chunk_list(all_skus, 100):
        items = ozon.get_analytics_stocks(sku_chunk)
        for item in items:
            offer_id = clean_offer_id(item.get('offer_id'))
            if not offer_id:
                continue

            name = item.get('name', offer_id_to_name.get(offer_id, '—'))
            available = item.get('available_stock_count', 0)
            returning = item.get('return_from_customer_stock_count', 0)
            prepare = item.get('valid_stock_count', 0)

            if offer_id not in raw_stock_dict:
                raw_stock_dict[offer_id] = {
                    'name': name,
                    'available': 0,
                    'returning': 0,
                    'prepare': 0
                }

            raw_stock_dict[offer_id]['available'] += available
            raw_stock_dict[offer_id]['returning'] += returning
            raw_stock_dict[offer_id]['prepare'] += prepare

    missing_offer_ids = list(set(offer_ids) - set(raw_stock_dict.keys()))
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
                available = stocks.get('present', 0)
                returning = 0
                prepare = stocks.get('reserved', 0)

                if offer_id not in raw_stock_dict:
                    raw_stock_dict[offer_id] = {
                        'name': name,
                        'available': 0,
                        'returning': 0,
                        'prepare': 0
                    }

                raw_stock_dict[offer_id]['available'] += available
                raw_stock_dict[offer_id]['returning'] += returning
                raw_stock_dict[offer_id]['prepare'] += prepare

    # === СОЗДАНИЕ АГРЕГИРОВАННЫХ СЫРЫХ ДАННЫХ ===
    raw_data = []
    for offer_id, data in raw_stock_dict.items():
        total = data['available'] + data['returning'] + data['prepare']
        raw_data.append({
            'Наименование': data['name'],
            'Артикул': offer_id,
            'Доступно на складах': data['available'],
            'Возвращаются от покупателей': data['returning'],
            'Подготовка к продаже': data['prepare'],
            'Итого на МП': total
        })

    # Преобразуем в формат, который ожидает основная функция (для сводного отчёта)
    result_dict = {}
    for offer_id, data in raw_stock_dict.items():
        result_dict[offer_id] = {
            'avail': data['available'],
            'return': data['returning'],
            'prep': data['prepare']
        }

    return result_dict, raw_data


async def fetch_wb_remains_raw(cabinet_id):
    wb = WildberriesAPI(cabinet_id=cabinet_id)
    stocks = wb.get_fbo_stocks_v1()

    # === АГРЕГАЦИЯ СЫРЫХ ДАННЫХ ПО АРТИКУЛАМ ===
    raw_stock_dict = {}

    for item in stocks:
        art = clean_article(item.get("supplierArticle"))
        if not art:
            continue

        quantity = item.get('quantity', 0)
        in_way_to_client = item.get('inWayToClient', 0)
        in_way_from_client = item.get('inWayFromClient', 0)

        if art not in raw_stock_dict:
            raw_stock_dict[art] = {
                'quantity': 0,
                'in_way_to_client': 0,
                'in_way_from_client': 0
            }

        raw_stock_dict[art]['quantity'] += quantity
        raw_stock_dict[art]['in_way_to_client'] += in_way_to_client
        raw_stock_dict[art]['in_way_from_client'] += in_way_from_client

    # === СОЗДАНИЕ АГРЕГИРОВАННЫХ СЫРЫХ ДАННЫХ ===
    raw_data = []
    stock_dict = {}

    for art, data in raw_stock_dict.items():
        total = data['quantity'] + data['in_way_to_client'] + data['in_way_from_client']
        raw_data.append({
            'Артикул': art,
            'Доступно на складах': data['quantity'],
            'Возвращаются от покупателей': data['in_way_from_client'],
            'В пути до покупателей': data['in_way_to_client'],
            'Итого на МП': total
        })

        # Также заполняем stock_dict для сводного отчёта
        stock_dict[art] = {
            'avail': data['quantity'],
            'return': data['in_way_from_client'],
            'inway': data['in_way_to_client']
        }

    return stock_dict, raw_data


# === ФУНКЦИЯ НОРМАЛИЗАЦИИ ===

def normalize_art(art_str):
    """Нормализует строку: приводит к нижнему регистру, удаляет лишние пробелы, очищает от невидимых символов"""
    if not art_str:
        return ""
    s = str(art_str)
    s = ''.join(c for c in s if c.isprintable())
    s = s.strip().lower()
    return s


# === ФУНКЦИИ ДЛЯ СОЗДАНИЯ EXCEL ЛИСТОВ ===

def _write_sheet(ws, df, headers, has_name=False):
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


# === ОСНОВНОЙ ОБРАБОТЧИК ===

async def start_all_mp_remains(update: Update, context: CallbackContext) -> int:
    # Сохраняем message_id для последующего удаления
    context.user_data['all_mp_status_messages'] = []

    status_msg = await update.message.reply_text("⏳ Начинаю выгрузку остатков со всех маркетплейсов...",
                                                 reply_markup=ReplyKeyboardRemove())
    context.user_data['all_mp_status_messages'].append(status_msg.message_id)

    await generate_all_mp_report(update, context)
    return ConversationHandler.END


async def generate_all_mp_report(update: Update, context: CallbackContext):
    try:
        # Список для хранения ID сообщений о статусе
        status_message_ids = context.user_data.get('all_mp_status_messages', [])

        # === 1. Получаем сырые данные ===
        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Ozon Кабинет 1 (Nimba)...")
        status_message_ids.append(status_msg.message_id)
        ozon1_raw_dict, ozon1_raw_data = await fetch_ozon_remains_raw(1)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Ozon Кабинет 2 (Galioni)...")
        status_message_ids.append(status_msg.message_id)
        ozon2_raw_dict, ozon2_raw_data = await fetch_ozon_remains_raw(2)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Wildberries Кабинет 1 (Nimba)...")
        status_message_ids.append(status_msg.message_id)
        wb1_raw_dict, wb1_raw_data = await fetch_wb_remains_raw(1)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Wildberries Кабинет 2 (Galioni)...")
        status_message_ids.append(status_msg.message_id)
        wb2_raw_dict, wb2_raw_data = await fetch_wb_remains_raw(2)

        # === 2. Загружаем маппинги ===
        from utils.template_loader import get_cabinet_articles_by_template_id

        ozon1_id_to_name, ozon1_id_to_arts = get_cabinet_articles_by_template_id("Отдельно Озон Nimba")
        ozon2_id_to_name, ozon2_id_to_arts = get_cabinet_articles_by_template_id("Отдельно Озон Galioni")
        wb1_id_to_name, wb1_id_to_arts = get_cabinet_articles_by_template_id("Отдельно ВБ Nimba")
        wb2_id_to_name, wb2_id_to_arts = get_cabinet_articles_by_template_id("Отдельно ВБ Galioni")

        # === 3. Построим обратные маппинги ===
        def build_reverse(id_to_arts):
            rev = {}
            for tid, arts in id_to_arts.items():
                for art in arts:
                    clean_art = normalize_art(art)
                    rev[clean_art] = tid
            return rev

        ozon1_rev = build_reverse(ozon1_id_to_arts)
        ozon2_rev = build_reverse(ozon2_id_to_arts)
        wb1_rev = build_reverse(wb1_id_to_arts)
        wb2_rev = build_reverse(wb2_id_to_arts)

        # === 4. Агрегация данных ===
        ozon1_agg = {}
        for art, data in ozon1_raw_dict.items():
            clean_art = normalize_art(art)
            tid = ozon1_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon1_agg:
                    ozon1_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon1_agg[tid]['avail'] += data['avail']
                ozon1_agg[tid]['return'] += data['return']
                ozon1_agg[tid]['prep'] += data['prep']

        ozon2_agg = {}
        for art, data in ozon2_raw_dict.items():
            clean_art = normalize_art(art)
            tid = ozon2_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon2_agg:
                    ozon2_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon2_agg[tid]['avail'] += data['avail']
                ozon2_agg[tid]['return'] += data['return']
                ozon2_agg[tid]['prep'] += data['prep']

        wb1_agg = {}
        for art, data in wb1_raw_dict.items():
            clean_art = normalize_art(art)
            tid = wb1_rev.get(clean_art)
            if tid is not None:
                if tid not in wb1_agg:
                    wb1_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb1_agg[tid]['avail'] += data['avail']
                wb1_agg[tid]['return'] += data['return']
                wb1_agg[tid]['inway'] += data['inway']

        wb2_agg = {}
        for art, data in wb2_raw_dict.items():
            clean_art = normalize_art(art)
            tid = wb2_rev.get(clean_art)
            if tid is not None:
                if tid not in wb2_agg:
                    wb2_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb2_agg[tid]['avail'] += data['avail']
                wb2_agg[tid]['return'] += data['return']
                wb2_agg[tid]['inway'] += data['inway']

        # === 5. РАБОТА С ШАБЛОНОМ - ПОЛНОЕ КОПИРОВАНИЕ ===
        template_report_path = os.path.join(root_dir, "Шаблон выгрузки остатков всех МП.xlsx")
        if not os.path.exists(template_report_path):
            raise FileNotFoundError("Файл 'Шаблон выгрузки остатков всех МП.xlsx' не найден!")

        report_copy = os.path.join(root_dir, "Остатки_все_МП_отчёт.xlsx")

        # ПОЛНОСТЬЮ КОПИРУЕМ ФАЙЛ ШАБЛОНА
        shutil.copy(template_report_path, report_copy)

        # Загружаем скопированный файл
        wb = load_workbook(report_copy)
        ws = wb.active  # Это уже готовый лист "Остатки на МП" с правильным оформлением

        # Заполняем данными (только значения, оформление остаётся как в шаблоне)
        row = 7
        while True:
            cell_value = ws[f"A{row}"].value
            if not cell_value or str(cell_value).strip().upper() == "ИТОГО":
                break

            art_name = str(cell_value).strip()

            # Ищем template_id по имени во ВСЕХ кабинетах
            template_id = None
            all_id_to_name = [ozon1_id_to_name, ozon2_id_to_name, wb1_id_to_name, wb2_id_to_name]

            for id_to_name in all_id_to_name:
                for tid, name in id_to_name.items():
                    if str(name).strip().lower() == art_name.lower():
                        template_id = tid
                        break
                if template_id is not None:
                    break

            if template_id is not None:
                # --- Ozon 1 ---
                o1 = ozon1_agg.get(template_id, {'avail': 0, 'return': 0, 'prep': 0})
                ws[f"B{row}"] = o1['avail']
                ws[f"C{row}"] = o1['return']
                ws[f"D{row}"] = o1['prep']
                ws[f"E{row}"] = o1['avail'] + o1['return'] + o1['prep']

                # --- Ozon 2 ---
                o2 = ozon2_agg.get(template_id, {'avail': 0, 'return': 0, 'prep': 0})
                ws[f"G{row}"] = o2['avail']
                ws[f"H{row}"] = o2['return']
                ws[f"I{row}"] = o2['prep']
                ws[f"J{row}"] = o2['avail'] + o2['return'] + o2['prep']

                # --- WB 1 ---
                w1 = wb1_agg.get(template_id, {'avail': 0, 'return': 0, 'inway': 0})
                ws[f"L{row}"] = w1['avail']
                ws[f"M{row}"] = w1['return']
                ws[f"N{row}"] = w1['inway']
                ws[f"O{row}"] = w1['avail'] + w1['return'] + w1['inway']

                # --- WB 2 ---
                w2 = wb2_agg.get(template_id, {'avail': 0, 'return': 0, 'inway': 0})
                ws[f"Q{row}"] = w2['avail']
                ws[f"R{row}"] = w2['return']
                ws[f"S{row}"] = w2['inway']
                ws[f"T{row}"] = w2['avail'] + w2['return'] + w2['inway']

            row += 1

        # === ДОБАВЛЯЕМ ДОПОЛНИТЕЛЬНЫЕ ЛИСТЫ ===

        # Ozon1 исходные артикулы
        if ozon1_raw_data:
            df_ozon1_raw = pd.DataFrame(ozon1_raw_data).sort_values(by='Наименование',
                                                                    key=lambda x: x.str.lower()).reset_index(drop=True)
            headers_ozon1 = ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей",
                             "Подготовка к продаже", "Итого на МП"]
            ws_ozon1 = wb.create_sheet(title="Ozon1 исходные артикулы")
            _write_sheet(ws_ozon1, df_ozon1_raw, headers_ozon1, has_name=True)
        else:
            ws_ozon1 = wb.create_sheet(title="Ozon1 исходные артикулы")
            ws_ozon1.append(["Нет данных"])

        # Ozon2 исходные артикулы
        if ozon2_raw_data:
            df_ozon2_raw = pd.DataFrame(ozon2_raw_data).sort_values(by='Наименование',
                                                                    key=lambda x: x.str.lower()).reset_index(drop=True)
            headers_ozon2 = ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей",
                             "Подготовка к продаже", "Итого на МП"]
            ws_ozon2 = wb.create_sheet(title="Ozon2 исходные артикулы")
            _write_sheet(ws_ozon2, df_ozon2_raw, headers_ozon2, has_name=True)
        else:
            ws_ozon2 = wb.create_sheet(title="Ozon2 исходные артикулы")
            ws_ozon2.append(["Нет данных"])

        # WB1 исходные артикулы
        if wb1_raw_data:
            df_wb1_raw = pd.DataFrame(wb1_raw_data).sort_values(by='Артикул').reset_index(drop=True)
            headers_wb1 = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей",
                           "Итого на МП"]
            ws_wb1 = wb.create_sheet(title="WB1 исходные артикулы")
            _write_sheet(ws_wb1, df_wb1_raw, headers_wb1, has_name=False)
        else:
            ws_wb1 = wb.create_sheet(title="WB1 исходные артикулы")
            ws_wb1.append(["Нет данных"])

        # WB2 исходные артикулы
        if wb2_raw_data:
            df_wb2_raw = pd.DataFrame(wb2_raw_data).sort_values(by='Артикул').reset_index(drop=True)
            headers_wb2 = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей",
                           "Итого на МП"]
            ws_wb2 = wb.create_sheet(title="WB2 исходные артикулы")
            _write_sheet(ws_wb2, df_wb2_raw, headers_wb2, has_name=False)
        else:
            ws_wb2 = wb.create_sheet(title="WB2 исходные артикулы")
            ws_wb2.append(["Нет данных"])

        wb.save(report_copy)

        # === РАСЧЁТ СВОДНЫХ ДАННЫХ ПО ВСЕМ КАБИНЕТАМ ===

        # Ozon 1
        ozon1_total_avail = sum(data['avail'] for data in ozon1_raw_dict.values())
        ozon1_total_return = sum(data['return'] for data in ozon1_raw_dict.values())
        ozon1_total_prep = sum(data['prep'] for data in ozon1_raw_dict.values())
        ozon1_total_mp = ozon1_total_avail + ozon1_total_return + ozon1_total_prep

        # Ozon 2
        ozon2_total_avail = sum(data['avail'] for data in ozon2_raw_dict.values())
        ozon2_total_return = sum(data['return'] for data in ozon2_raw_dict.values())
        ozon2_total_prep = sum(data['prep'] for data in ozon2_raw_dict.values())
        ozon2_total_mp = ozon2_total_avail + ozon2_total_return + ozon2_total_prep

        # WB 1
        wb1_total_avail = sum(data['avail'] for data in wb1_raw_dict.values())
        wb1_total_return = sum(data['return'] for data in wb1_raw_dict.values())
        wb1_total_inway = sum(data['inway'] for data in wb1_raw_dict.values())
        wb1_total_mp = wb1_total_avail + wb1_total_return + wb1_total_inway

        # WB 2
        wb2_total_avail = sum(data['avail'] for data in wb2_raw_dict.values())
        wb2_total_return = sum(data['return'] for data in wb2_raw_dict.values())
        wb2_total_inway = sum(data['inway'] for data in wb2_raw_dict.values())
        wb2_total_mp = wb2_total_avail + wb2_total_return + wb2_total_inway

        # Общая сумма по всем маркетплейсам
        total_all_mp = ozon1_total_mp + ozon2_total_mp + wb1_total_mp + wb2_total_mp

        def fmt(x):
            return f"{x:,}".replace(",", " ")

        # === ФОРМИРОВАНИЕ КРАСИВОГО СООБЩЕНИЯ ===
        summary_text = (
            "📊 <b>Сводка по остаткам на всех маркетплейсах</b>\n\n"

            "🏪 <b>Ozon Кабинет 1 (Nimba)</b>\n"
            f"   📦 Доступно на складах: {fmt(ozon1_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(ozon1_total_return)} шт\n"
            f"   🔄 Подготовка к продаже: {fmt(ozon1_total_prep)} шт\n"
            f"   ✅ Итого на МП: {fmt(ozon1_total_mp)} шт\n\n"

            "🏬 <b>Ozon Кабинет 2 (Galioni)</b>\n"
            f"   📦 Доступно на складах: {fmt(ozon2_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(ozon2_total_return)} шт\n"
            f"   🔄 Подготовка к продаже: {fmt(ozon2_total_prep)} шт\n"
            f"   ✅ Итого на МП: {fmt(ozon2_total_mp)} шт\n\n"

            "🏪 <b>Wildberries Кабинет 1 (Nimba)</b>\n"
            f"   📦 Доступно на складах: {fmt(wb1_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(wb1_total_return)} шт\n"
            f"   🚚 В пути до покупателей: {fmt(wb1_total_inway)} шт\n"
            f"   ✅ Итого на МП: {fmt(wb1_total_mp)} шт\n\n"

            "🏬 <b>Wildberries Кабинет 2 (Galioni)</b>\n"
            f"   📦 Доступно на складах: {fmt(wb2_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(wb2_total_return)} шт\n"
            f"   🚚 В пути до покупателей: {fmt(wb2_total_inway)} шт\n"
            f"   ✅ Итого на МП: {fmt(wb2_total_mp)} шт\n\n"

            f"🔹 <b>ВСЕГО на всех маркетплейсах:</b> {fmt(total_all_mp)} шт"
        )

        # === Удаляем все промежуточные сообщения ===
        for msg_id in status_message_ids:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_id)
            except Exception:
                pass

        # === Отправляем финальный отчёт ===
        await update.message.reply_document(
            document=open(report_copy, 'rb'),
            caption="📊 Объединённый отчёт по остаткам на всех маркетплейсах\n\n"
                    "📄 Листы:\n"
                    "• Остатки на МП — сводный отчёт\n"
                    "• Ozon1/Ozon2 исходные артикулы — сырые данные Ozon\n"
                    "• WB1/WB2 исходные артикулы — сырые данные Wildberries",
            reply_markup=ReplyKeyboardRemove()
        )

        # === Отправляем сводку текстом ===
        await update.message.reply_text(summary_text, parse_mode="HTML")

        if os.path.exists(report_copy):
            os.remove(report_copy)

    except Exception as e:
        # Удаляем промежуточные сообщения даже при ошибке
        status_message_ids = context.user_data.get('all_mp_status_messages', [])
        for msg_id in status_message_ids:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_id)
            except Exception:
                pass

        logger.error(f"Ошибка в объединённом отчёте: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {str(e)}", reply_markup=ReplyKeyboardRemove())

# ======================
# Автоматическая отправка отчёта по всем маркетплейсам (для job_queue)
# ======================

async def send_all_mp_remains_automatic(context: CallbackContext):
    """Автоматическая отправка объединённого отчёта по остаткам на всех маркетплейсах"""
    chat_id = context.job.data.get('chat_id')
    if not chat_id:
        logger.error("Автоматический отчёт: chat_id не указан в job.data")
        return

    try:
        # === 1. Получаем сырые данные ===
        ozon1_raw_dict, ozon1_raw_data = await fetch_ozon_remains_raw(1)
        ozon2_raw_dict, ozon2_raw_data = await fetch_ozon_remains_raw(2)
        wb1_raw_dict, wb1_raw_data = await fetch_wb_remains_raw(1)
        wb2_raw_dict, wb2_raw_data = await fetch_wb_remains_raw(2)

        # === 2. Загружаем маппинги ===
        from utils.template_loader import get_cabinet_articles_by_template_id

        ozon1_id_to_name, ozon1_id_to_arts = get_cabinet_articles_by_template_id("Отдельно Озон Nimba")
        ozon2_id_to_name, ozon2_id_to_arts = get_cabinet_articles_by_template_id("Отдельно Озон Galioni")
        wb1_id_to_name, wb1_id_to_arts = get_cabinet_articles_by_template_id("Отдельно ВБ Nimba")
        wb2_id_to_name, wb2_id_to_arts = get_cabinet_articles_by_template_id("Отдельно ВБ Galioni")

        # === 3. Построим обратные маппинги ===
        def build_reverse(id_to_arts):
            rev = {}
            for tid, arts in id_to_arts.items():
                for art in arts:
                    clean_art = normalize_art(art)
                    rev[clean_art] = tid
            return rev

        ozon1_rev = build_reverse(ozon1_id_to_arts)
        ozon2_rev = build_reverse(ozon2_id_to_arts)
        wb1_rev = build_reverse(wb1_id_to_arts)
        wb2_rev = build_reverse(wb2_id_to_arts)

        # === 4. Агрегация данных ===
        ozon1_agg = {}
        for art, data in ozon1_raw_dict.items():
            clean_art = normalize_art(art)
            tid = ozon1_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon1_agg:
                    ozon1_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon1_agg[tid]['avail'] += data['avail']
                ozon1_agg[tid]['return'] += data['return']
                ozon1_agg[tid]['prep'] += data['prep']

        ozon2_agg = {}
        for art, data in ozon2_raw_dict.items():
            clean_art = normalize_art(art)
            tid = ozon2_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon2_agg:
                    ozon2_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon2_agg[tid]['avail'] += data['avail']
                ozon2_agg[tid]['return'] += data['return']
                ozon2_agg[tid]['prep'] += data['prep']

        wb1_agg = {}
        for art, data in wb1_raw_dict.items():
            clean_art = normalize_art(art)
            tid = wb1_rev.get(clean_art)
            if tid is not None:
                if tid not in wb1_agg:
                    wb1_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb1_agg[tid]['avail'] += data['avail']
                wb1_agg[tid]['return'] += data['return']
                wb1_agg[tid]['inway'] += data['inway']

        wb2_agg = {}
        for art, data in wb2_raw_dict.items():
            clean_art = normalize_art(art)
            tid = wb2_rev.get(clean_art)
            if tid is not None:
                if tid not in wb2_agg:
                    wb2_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb2_agg[tid]['avail'] += data['avail']
                wb2_agg[tid]['return'] += data['return']
                wb2_agg[tid]['inway'] += data['inway']

        # === 5. РАБОТА С ШАБЛОНОМ ===
        template_report_path = os.path.join(root_dir, "Шаблон выгрузки остатков всех МП.xlsx")
        if not os.path.exists(template_report_path):
            raise FileNotFoundError("Файл 'Шаблон выгрузки остатков всех МП.xlsx' не найден!")

        report_copy = os.path.join(root_dir, f"Остатки_все_МП_авто_{int(time.time())}.xlsx")
        shutil.copy(template_report_path, report_copy)

        wb = load_workbook(report_copy)
        ws = wb.active

        row = 7
        while True:
            cell_value = ws[f"A{row}"].value
            if not cell_value or str(cell_value).strip().upper() == "ИТОГО":
                break

            art_name = str(cell_value).strip()
            template_id = None
            all_id_to_name = [ozon1_id_to_name, ozon2_id_to_name, wb1_id_to_name, wb2_id_to_name]

            for id_to_name in all_id_to_name:
                for tid, name in id_to_name.items():
                    if str(name).strip().lower() == art_name.lower():
                        template_id = tid
                        break
                if template_id is not None:
                    break

            if template_id is not None:
                # Ozon 1
                o1 = ozon1_agg.get(template_id, {'avail': 0, 'return': 0, 'prep': 0})
                ws[f"B{row}"] = o1['avail']
                ws[f"C{row}"] = o1['return']
                ws[f"D{row}"] = o1['prep']
                ws[f"E{row}"] = o1['avail'] + o1['return'] + o1['prep']

                # Ozon 2
                o2 = ozon2_agg.get(template_id, {'avail': 0, 'return': 0, 'prep': 0})
                ws[f"G{row}"] = o2['avail']
                ws[f"H{row}"] = o2['return']
                ws[f"I{row}"] = o2['prep']
                ws[f"J{row}"] = o2['avail'] + o2['return'] + o2['prep']

                # WB 1
                w1 = wb1_agg.get(template_id, {'avail': 0, 'return': 0, 'inway': 0})
                ws[f"L{row}"] = w1['avail']
                ws[f"M{row}"] = w1['return']
                ws[f"N{row}"] = w1['inway']
                ws[f"O{row}"] = w1['avail'] + w1['return'] + w1['inway']

                # WB 2
                w2 = wb2_agg.get(template_id, {'avail': 0, 'return': 0, 'inway': 0})
                ws[f"Q{row}"] = w2['avail']
                ws[f"R{row}"] = w2['return']
                ws[f"S{row}"] = w2['inway']
                ws[f"T{row}"] = w2['avail'] + w2['return'] + w2['inway']

            row += 1

        # === ДОПОЛНИТЕЛЬНЫЕ ЛИСТЫ ===
        # Ozon1
        if ozon1_raw_data:
            df_ozon1_raw = pd.DataFrame(ozon1_raw_data).sort_values(by='Наименование', key=lambda x: x.str.lower()).reset_index(drop=True)
            ws_ozon1 = wb.create_sheet(title="Ozon1 исходные артикулы")
            _write_sheet(ws_ozon1, df_ozon1_raw, ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже", "Итого на МП"], has_name=True)
        else:
            ws_ozon1 = wb.create_sheet(title="Ozon1 исходные артикулы")
            ws_ozon1.append(["Нет данных"])

        # Ozon2
        if ozon2_raw_data:
            df_ozon2_raw = pd.DataFrame(ozon2_raw_data).sort_values(by='Наименование', key=lambda x: x.str.lower()).reset_index(drop=True)
            ws_ozon2 = wb.create_sheet(title="Ozon2 исходные артикулы")
            _write_sheet(ws_ozon2, df_ozon2_raw, ["Наименование", "Артикул", "Доступно на складах", "Возвращаются от покупателей", "Подготовка к продаже", "Итого на МП"], has_name=True)
        else:
            ws_ozon2 = wb.create_sheet(title="Ozon2 исходные артикулы")
            ws_ozon2.append(["Нет данных"])

        # WB1
        if wb1_raw_data:
            df_wb1_raw = pd.DataFrame(wb1_raw_data).sort_values(by='Артикул').reset_index(drop=True)
            ws_wb1 = wb.create_sheet(title="WB1 исходные артикулы")
            _write_sheet(ws_wb1, df_wb1_raw, ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей", "Итого на МП"], has_name=False)
        else:
            ws_wb1 = wb.create_sheet(title="WB1 исходные артикулы")
            ws_wb1.append(["Нет данных"])

        # WB2
        if wb2_raw_data:
            df_wb2_raw = pd.DataFrame(wb2_raw_data).sort_values(by='Артикул').reset_index(drop=True)
            ws_wb2 = wb.create_sheet(title="WB2 исходные артикулы")
            _write_sheet(ws_wb2, df_wb2_raw, ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей", "Итого на МП"], has_name=False)
        else:
            ws_wb2 = wb.create_sheet(title="WB2 исходные артикулы")
            ws_wb2.append(["Нет данных"])

        wb.save(report_copy)

        # === РАСЧЁТ ПОДРОБНЫХ СВОДОК (как в ручном отчёте) ===

        # Ozon 1
        ozon1_total_avail = sum(data['avail'] for data in ozon1_raw_dict.values())
        ozon1_total_return = sum(data['return'] for data in ozon1_raw_dict.values())
        ozon1_total_prep = sum(data['prep'] for data in ozon1_raw_dict.values())
        ozon1_total_mp = ozon1_total_avail + ozon1_total_return + ozon1_total_prep

        # Ozon 2
        ozon2_total_avail = sum(data['avail'] for data in ozon2_raw_dict.values())
        ozon2_total_return = sum(data['return'] for data in ozon2_raw_dict.values())
        ozon2_total_prep = sum(data['prep'] for data in ozon2_raw_dict.values())
        ozon2_total_mp = ozon2_total_avail + ozon2_total_return + ozon2_total_prep

        # WB 1
        wb1_total_avail = sum(data['avail'] for data in wb1_raw_dict.values())
        wb1_total_return = sum(data['return'] for data in wb1_raw_dict.values())
        wb1_total_inway = sum(data['inway'] for data in wb1_raw_dict.values())
        wb1_total_mp = wb1_total_avail + wb1_total_return + wb1_total_inway

        # WB 2
        wb2_total_avail = sum(data['avail'] for data in wb2_raw_dict.values())
        wb2_total_return = sum(data['return'] for data in wb2_raw_dict.values())
        wb2_total_inway = sum(data['inway'] for data in wb2_raw_dict.values())
        wb2_total_mp = wb2_total_avail + wb2_total_return + wb2_total_inway

        # Общая сумма по всем маркетплейсам
        total_all_mp = ozon1_total_mp + ozon2_total_mp + wb1_total_mp + wb2_total_mp

        def fmt(x):
            return f"{x:,}".replace(",", " ")

        # === ПОДРОБНАЯ СВОДКА
        summary_text = (
            f"📊 <b>Еженедельный отчёт по остаткам на всех маркетплейсах</b>\n"
            f"📅 Дата: {time.strftime('%Y-%m-%d %H:%M')}\n\n"

            f"🏪 <b>Ozon Кабинет 1 (Nimba)</b>\n"
            f"   📦 Доступно на складах: {fmt(ozon1_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(ozon1_total_return)} шт\n"
            f"   🔄 Подготовка к продаже: {fmt(ozon1_total_prep)} шт\n"
            f"   ✅ Итого на МП: {fmt(ozon1_total_mp)} шт\n\n"

            f"🏬 <b>Ozon Кабинет 2 (Galioni)</b>\n"
            f"   📦 Доступно на складах: {fmt(ozon2_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(ozon2_total_return)} шт\n"
            f"   🔄 Подготовка к продаже: {fmt(ozon2_total_prep)} шт\n"
            f"   ✅ Итого на МП: {fmt(ozon2_total_mp)} шт\n\n"

            f"🏪 <b>Wildberries Кабинет 1 (Nimba)</b>\n"
            f"   📦 Доступно на складах: {fmt(wb1_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(wb1_total_return)} шт\n"
            f"   🚚 В пути до покупателей: {fmt(wb1_total_inway)} шт\n"
            f"   ✅ Итого на МП: {fmt(wb1_total_mp)} шт\n\n"

            f"🏬 <b>Wildberries Кабинет 2 (Galioni)</b>\n"
            f"   📦 Доступно на складах: {fmt(wb2_total_avail)} шт\n"
            f"   ↩️ Возвращаются от покупателей: {fmt(wb2_total_return)} шт\n"
            f"   🚚 В пути до покупателей: {fmt(wb2_total_inway)} шт\n"
            f"   ✅ Итого на МП: {fmt(wb2_total_mp)} шт\n\n"

            f"🔹 <b>ВСЕГО на всех маркетплейсах:</b> {fmt(total_all_mp)} шт"
        )

        # === ОТПРАВКА ===
        await context.bot.send_document(
            chat_id=chat_id,
            document=open(report_copy, 'rb'),
            caption="📊 Еженедельный отчёт: остатки на всех маркетплейсах"
        )
        await context.bot.send_message(chat_id=chat_id, text=summary_text, parse_mode="HTML")

        # === ОЧИСТКА ===
        if os.path.exists(report_copy):
            os.remove(report_copy)

    except Exception as e:
        logger.error(f"Ошибка в автоматическом отчёте по всем МП: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при генерации еженедельного отчёта по всем маркетплейсам: {str(e)}"
        )
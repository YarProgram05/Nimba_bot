# handlers/all_mp_remains_handler.py

import os
import sys
import shutil
import logging
import pandas as pd
from telegram import Update, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler
from openpyxl import load_workbook

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
        return {}

    items = product_list.get('result', {}).get('items', [])
    if not items:
        logger.warning(f"Ozon кабинет {cabinet_id}: товары не найдены")
        return {}

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
        return {}

    stock_dict = {}

    for sku_chunk in chunk_list(all_skus, 100):
        items = ozon.get_analytics_stocks(sku_chunk)
        for item in items:
            offer_id = clean_offer_id(item.get('offer_id'))
            if not offer_id:
                continue

            if offer_id in stock_dict:
                stock_dict[offer_id]['available_stock_count'] += item.get('available_stock_count', 0)
                stock_dict[offer_id]['return_from_customer_stock_count'] += item.get('return_from_customer_stock_count',
                                                                                     0)
                stock_dict[offer_id]['other_stock_count'] += item.get('other_stock_count', 0)
            else:
                stock_dict[offer_id] = {
                    'name': item.get('name', offer_id_to_name.get(offer_id, '—')),
                    'available_stock_count': item.get('available_stock_count', 0),
                    'return_from_customer_stock_count': item.get('return_from_customer_stock_count', 0),
                    'other_stock_count': item.get('other_stock_count', 0)
                }

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

    # Преобразуем в формат, который ожидает основная функция
    result_dict = {}
    for offer_id, data in stock_dict.items():
        result_dict[offer_id] = {
            'avail': data['available_stock_count'],
            'return': data['return_from_customer_stock_count'],
            'prep': data['other_stock_count']
        }

    return result_dict


async def fetch_wb_remains_raw(cabinet_id):
    wb = WildberriesAPI(cabinet_id=cabinet_id)
    stocks = wb.get_fbo_stocks_v1()
    stock_dict = {}
    for item in stocks:
        art = clean_article(item.get("supplierArticle"))
        if not art:
            continue
        if art not in stock_dict:
            stock_dict[art] = {
                'avail': 0,
                'return': 0,
                'inway': 0
            }
        stock_dict[art]['avail'] += item.get('quantity', 0)
        stock_dict[art]['return'] += item.get('inWayFromClient', 0)
        stock_dict[art]['inway'] += item.get('inWayToClient', 0)

    return stock_dict


# === ФУНКЦИЯ НОРМАЛИЗАЦИИ ===

def normalize_art(art_str):
    """Нормализует строку: приводит к нижнему регистру, удаляет лишние пробелы, очищает от невидимых символов"""
    if not art_str:
        return ""
    s = str(art_str)
    s = ''.join(c for c in s if c.isprintable())
    s = s.strip().lower()
    return s


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
        ozon1_raw = await fetch_ozon_remains_raw(1)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Ozon Кабинет 2 (Galioni)...")
        status_message_ids.append(status_msg.message_id)
        ozon2_raw = await fetch_ozon_remains_raw(2)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Wildberries Кабинет 1 (Nimba)...")
        status_message_ids.append(status_msg.message_id)
        wb1_raw = await fetch_wb_remains_raw(1)

        status_msg = await update.message.reply_text("📊 Запрашиваю остатки Wildberries Кабинет 2 (Galioni)...")
        status_message_ids.append(status_msg.message_id)
        wb2_raw = await fetch_wb_remains_raw(2)

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
        for art, data in ozon1_raw.items():
            clean_art = normalize_art(art)
            tid = ozon1_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon1_agg:
                    ozon1_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon1_agg[tid]['avail'] += data['avail']
                ozon1_agg[tid]['return'] += data['return']
                ozon1_agg[tid]['prep'] += data['prep']

        ozon2_agg = {}
        for art, data in ozon2_raw.items():
            clean_art = normalize_art(art)
            tid = ozon2_rev.get(clean_art)
            if tid is not None:
                if tid not in ozon2_agg:
                    ozon2_agg[tid] = {'avail': 0, 'return': 0, 'prep': 0}
                ozon2_agg[tid]['avail'] += data['avail']
                ozon2_agg[tid]['return'] += data['return']
                ozon2_agg[tid]['prep'] += data['prep']

        wb1_agg = {}
        for art, data in wb1_raw.items():
            clean_art = normalize_art(art)
            tid = wb1_rev.get(clean_art)
            if tid is not None:
                if tid not in wb1_agg:
                    wb1_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb1_agg[tid]['avail'] += data['avail']
                wb1_agg[tid]['return'] += data['return']
                wb1_agg[tid]['inway'] += data['inway']

        wb2_agg = {}
        for art, data in wb2_raw.items():
            clean_art = normalize_art(art)
            tid = wb2_rev.get(clean_art)
            if tid is not None:
                if tid not in wb2_agg:
                    wb2_agg[tid] = {'avail': 0, 'return': 0, 'inway': 0}
                wb2_agg[tid]['avail'] += data['avail']
                wb2_agg[tid]['return'] += data['return']
                wb2_agg[tid]['inway'] += data['inway']

        # === 5. Заполняем шаблон ===
        template_report_path = os.path.join(root_dir, "Шаблон выгрузки остатков всех МП.xlsx")
        if not os.path.exists(template_report_path):
            raise FileNotFoundError("Файл 'Шаблон выгрузки остатков всех МП.xlsx' не найден!")

        report_copy = os.path.join(root_dir, "Остатки_все_МП_отчёт.xlsx")
        shutil.copy(template_report_path, report_copy)

        wb = load_workbook(report_copy)
        ws = wb.active

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

        wb.save(report_copy)

        # === Удаляем все промежуточные сообщения ===
        for msg_id in status_message_ids:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_id)
            except Exception:
                pass  # Игнорируем ошибки удаления

        # === Отправляем финальный отчёт ===
        await update.message.reply_document(
            document=open(report_copy, 'rb'),
            caption="📊 Объединённый отчёт по остаткам на всех маркетплейсах",
            reply_markup=ReplyKeyboardRemove()
        )

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
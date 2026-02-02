# handlers/wb_remains_handler.py

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

from states import WB_REMAINS_CABINET_CHOICE  # ← ДОЛЖЕН БЫТЬ В states.py

# Импорт новой функции из template_loader
from utils.template_loader import get_cabinet_articles_by_template_id


def clean_article(article):
    """Очистка артикула от лишних символов"""
    try:
        if not article:
            return None
        s = str(article)
        s = ''.join(c for c in s if c.isprintable())
        s = s.strip()
        return s if s else None
    except Exception:
        return None


class WildberriesAPI:
    def __init__(self, cabinet_id=1):
        from dotenv import load_dotenv
        load_dotenv()

        if cabinet_id == 1:
            self.api_token = os.getenv('WB_API_TOKEN_1')
        elif cabinet_id == 2:
            self.api_token = os.getenv('WB_API_TOKEN_2')
        elif cabinet_id == 3:
            self.api_token = os.getenv('WB_API_TOKEN_3')
        else:
            raise ValueError("Поддерживаются только cabinet_id 1, 2 или 3")

        if not self.api_token:
            raise ValueError(f"❌ WB_API_TOKEN не задан в .env для кабинета {cabinet_id}")

        self.headers = {
            'Authorization': self.api_token,
            'Content-Type': 'application/json'
        }

    def get_fbo_stocks_v1(self):
        """Получает ВСЕ FBO-остатки через statistics-api"""
        all_stocks = []
        last_change_date = "2010-01-01T00:00:00"

        while True:
            # 🔥 ИСПРАВЛЕНО: удалены лишние пробелы в конце URL!
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
            params = {"dateFrom": last_change_date}

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
                response.raise_for_status()
                logger.info(f"Запрос FBO остатков v1, статус={response.status_code}, dateFrom={last_change_date}")

                if response.status_code == 200:
                    data = response.json()
                    if not isinstance(data, list):
                        logger.error(f"Некорректный ответ (не список): {data}")
                        break

                    if not data:
                        logger.info("Получен пустой ответ — выгрузка завершена")
                        break

                    all_stocks.extend(data)
                    logger.info(f"Получено {len(data)} строк, всего: {len(all_stocks)}")

                    last_change_date = data[-1].get("lastChangeDate")
                    if not last_change_date:
                        break

                    time.sleep(1)
                else:
                    logger.error(f"Ошибка v1 stocks: {response.status_code} - {response.text}")
                    break

            except requests.exceptions.Timeout:
                logger.error(f"Таймаут при запросе FBO остатков (dateFrom={last_change_date})")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка сети при запросе FBO остатков: {e}")
                break
            except Exception as e:
                logger.error(f"Неожиданная ошибка в v1 stocks: {e}", exc_info=True)
                break

        return all_stocks


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


def group_wb_remains_data(stock_data, template_id_to_cabinet_arts, template_id_to_name):
    """
    Группирует данные остатков WB по шаблонным артикулам.

    :param stock_data: dict {article: {"in_stock": ..., "in_way_from_client": ..., "in_way_to_client": ...}}
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
                    'in_stock': 0,
                    'in_way_from_client': 0,
                    'in_way_to_client': 0
                }
            grouped[template_id]['in_stock'] += data['in_stock']
            grouped[template_id]['in_way_from_client'] += data['in_way_from_client']
            grouped[template_id]['in_way_to_client'] += data['in_way_to_client']
        else:
            unmatched[clean_art] = {
                'name': f"НЕОПОЗНАННЫЙ: {clean_art}",
                'in_stock': data['in_stock'],
                'in_way_from_client': data['in_way_from_client'],
                'in_way_to_client': data['in_way_to_client']
            }

    return grouped, unmatched


# ======================
# Обработчики
# ======================

async def start_wb_remains(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета Wildberries"""
    context.user_data['current_flow'] = 'wb_remains'

    keyboard = [
        [InlineKeyboardButton("🏪 WB_1 Nimba", callback_data='wb_cabinet_1')],
        [InlineKeyboardButton("🏬 WB_2 Galioni", callback_data='wb_cabinet_2')],
        [InlineKeyboardButton("🏢 WB_3 AGNIA", callback_data='wb_cabinet_3')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🏢 Выберите кабинет Wildberries для выгрузки остатков:",
        reply_markup=reply_markup
    )

    return WB_REMAINS_CABINET_CHOICE


async def handle_wb_cabinet_choice(update: Update, context: CallbackContext) -> int:
    """Обработка выбора кабинета WB — генерация отчёта"""
    query = update.callback_query
    await query.answer()

    cabinet_data = query.data
    if cabinet_data == 'wb_cabinet_1':
        cabinet_id = 1
        cabinet_name = "WB_1 Nimba"
        sheet_name = "Отдельно ВБ Nimba"
    elif cabinet_data == 'wb_cabinet_2':
        cabinet_id = 2
        cabinet_name = "WB_2 Galioni"
        sheet_name = "Отдельно ВБ Galioni"
    elif cabinet_data == 'wb_cabinet_3':
        cabinet_id = 3
        cabinet_name = "WB_3 AGNIA"
        sheet_name = "Отдельно ВБ AGNIA"
    else:
        await query.message.reply_text("❌ Неизвестный кабинет.")
        return ConversationHandler.END

    context.user_data['wb_cabinet_id'] = cabinet_id

    loading_msg1 = await query.message.edit_text(f"⏳ Получаю остатки с Wildberries API ({cabinet_name})...")
    context.user_data['wb_remains_loading_msg1_id'] = loading_msg1.message_id

    try:
        wb_api = WildberriesAPI(cabinet_id=cabinet_id)

        loading_msg2 = await query.message.reply_text("📊 Запрашиваю остатки по товарам...")
        context.user_data['wb_remains_loading_msg2_id'] = loading_msg2.message_id
        stocks = wb_api.get_fbo_stocks_v1()

        if not stocks:
            await query.message.reply_text(
                "ℹ️ Остатки не найдены. Возможные причины:\n"
                "• У вас нет товаров на складах Wildberries (FBO)\n"
                "• Токен не имеет доступа к остаткам",
                reply_markup=ReplyKeyboardRemove()
            )
            return ConversationHandler.END

        # === 1. Сырые данные ===
        raw_data = []
        stock_dict = {}

        for item in stocks:
            vendor_code = item.get("supplierArticle")
            if not vendor_code:
                continue

            article = clean_article(vendor_code)
            if not article:
                continue

            if article not in stock_dict:
                stock_dict[article] = {
                    'in_stock': 0,
                    'in_way_to_client': 0,
                    'in_way_from_client': 0
                }

            stock_dict[article]['in_stock'] += item.get('quantity', 0)
            stock_dict[article]['in_way_to_client'] += item.get('inWayToClient', 0)
            stock_dict[article]['in_way_from_client'] += item.get('inWayFromClient', 0)

        for article, counts in stock_dict.items():
            total = (
                    counts['in_stock'] +
                    counts['in_way_to_client'] +
                    counts['in_way_from_client']
            )
            raw_data.append({
                'Артикул': article,
                'Доступно на складах': counts['in_stock'],
                'Возвращаются от покупателей': counts['in_way_from_client'],
                'В пути до покупателей': counts['in_way_to_client'],
                'Итого на МП': total
            })

        df_raw = pd.DataFrame(raw_data).sort_values(by='Артикул').reset_index(drop=True)
        headers_raw = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей", "Итого на МП"]

        # === 2. Группировка по шаблону Nimba/Galioni ===
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

        wb_stock_data = {}
        for art, counts in stock_dict.items():
            wb_stock_data[art] = {
                "in_stock": counts['in_stock'],
                "in_way_from_client": counts['in_way_from_client'],
                "in_way_to_client": counts['in_way_to_client']
            }

        grouped, unmatched = group_wb_remains_data(wb_stock_data, template_id_to_cabinet_arts, template_id_to_name)

        template_data = []
        for id_val in main_ids_ordered:
            if id_val in grouped:
                d = grouped[id_val]
                total = d['in_stock'] + d['in_way_from_client'] + d['in_way_to_client']
                template_data.append({
                    'Артикул': d['name'],
                    'Доступно на складах': d['in_stock'],
                    'Возвращаются от покупателей': d['in_way_from_client'],
                    'В пути до покупателей': d['in_way_to_client'],
                    'Итого на МП': total
                })
            else:
                name = template_id_to_name.get(id_val, f"ID {id_val}")
                template_data.append({
                    'Артикул': name,
                    'Доступно на складах': 0,
                    'Возвращаются от покупателей': 0,
                    'В пути до покупателей': 0,
                    'Итого на МП': 0
                })

        for art, d in unmatched.items():
            total = d['in_stock'] + d['in_way_from_client'] + d['in_way_to_client']
            template_data.append({
                'Артикул': f"НЕОПОЗНАННЫЙ: {art}",
                'Доступно на складах': d['in_stock'],
                'Возвращаются от покупателей': d['in_way_from_client'],
                'В пути до покупателей': d['in_way_to_client'],
                'Итого на МП': total
            })

        df_template = pd.DataFrame(template_data)
        headers_template = ["Артикул", "Доступно на складах", "Возвращаются от покупателей", "В пути до покупателей", "Итого на МП"]

        # === Сводка ===
        total_in_stock = sum(d['in_stock'] for d in stock_dict.values())
        total_in_way_from = sum(d['in_way_from_client'] for d in stock_dict.values())
        total_in_way_to = sum(d['in_way_to_client'] for d in stock_dict.values())
        total_mp = total_in_stock + total_in_way_from + total_in_way_to

        def fmt_num(x):
            return f"{x:,}".replace(",", " ")

        summary_text = (
            f"📊 <b>Сводка по остаткам Wildberries (FBO)</b>\n"
            f"Кабинет: <b>{cabinet_name}</b>\n\n"
            f"📦 <b>Доступно на складах:</b> {fmt_num(total_in_stock)} шт\n"
            f"↩️ <b>Возвращаются от покупателей:</b> {fmt_num(total_in_way_from)} шт\n"
            f"🚚 <b>В пути до покупателей:</b> {fmt_num(total_in_way_to)} шт\n"
            f"✅ <b>Итого на МП:</b> {fmt_num(total_mp)} шт"
        )

        # === Создаём Excel с двумя листами ===
        report_path = f"WB_Remains_Report_Cabinet{cabinet_id}.xlsx"
        create_excel_with_two_sheets(df_raw, headers_raw, df_template, headers_template, report_path)

        # === Отправляем ===
        await query.message.reply_document(
            document=open(report_path, 'rb'),
            caption=f"📊 Отчёт по остаткам Wildberries: {cabinet_name}",
            reply_markup=ReplyKeyboardRemove()
        )
        await query.message.reply_text(summary_text, parse_mode="HTML")

        # === Очистка ===
        if os.path.exists(report_path):
            os.remove(report_path)

        # Удаляем служебные сообщения
        chat_id = query.message.chat_id
        try:
            msg1_id = context.user_data.get('wb_remains_loading_msg1_id')
            if msg1_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg1_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить первое сообщение о загрузке WB: {e}")

        try:
            msg2_id = context.user_data.get('wb_remains_loading_msg2_id')
            if msg2_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg2_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить второе сообщение о загрузке WB: {e}")

    except Exception as e:
        logger.error(f"Ошибка при получении остатков WB (кабинет {cabinet_id}): {str(e)}", exc_info=True)
        await query.message.reply_text(f"❌ Ошибка: {str(e)}", reply_markup=ReplyKeyboardRemove())
        # Удаляем служебные сообщения даже при ошибке
        chat_id = query.message.chat_id
        try:
            msg1_id = context.user_data.get('wb_remains_loading_msg1_id')
            if msg1_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg1_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить первое сообщение о загрузке WB при ошибке: {e}")

        try:
            msg2_id = context.user_data.get('wb_remains_loading_msg2_id')
            if msg2_id:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg2_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить второе сообщение о загрузке WB при ошибке: {e}")

    return ConversationHandler.END


def create_excel_with_two_sheets(df_raw, headers_raw, df_template, headers_template, filename):
    """Создаёт Excel с двумя листами"""
    wb = Workbook()
    wb.remove(wb.active)

    ws1 = wb.create_sheet(title="Остатки шаблон Nimba")
    _write_sheet(ws1, df_template, headers_template, has_name=False)

    ws2 = wb.create_sheet(title="Остатки исходные артикулы")
    _write_sheet(ws2, df_raw, headers_raw, has_name=False)

    wb.save(filename)


def _write_sheet(ws, df, headers, has_name):
    """Вспомогательная функция для записи одного листа"""
    bold_font = Font(bold=True)
    center_alignment = Alignment(horizontal='center', vertical='center')
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    ws.append(headers)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col)
        cell.font = bold_font
        cell.alignment = center_alignment
        cell.border = thin_border

    ws.merge_cells('A1:A2')

    data_start_row = 3
    sum_row = 2

    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False), data_start_row):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            cell.alignment = center_alignment
            cell.border = thin_border

    num_rows = len(df)
    if num_rows > 0:
        start_col_index = 2
        for col in range(start_col_index, len(headers) + 1):
            col_letter = get_column_letter(col)
            formula = f"=SUM({col_letter}{data_start_row}:{col_letter}{data_start_row + num_rows - 1})"
            cell = ws.cell(row=sum_row, column=col, value=formula)
            cell.font = bold_font
            cell.alignment = center_alignment
            cell.border = thin_border

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
# Заглушки для совместимости
# ======================

async def handle_wb_remains_files(update: Update, context: CallbackContext):
    await update.message.reply_text("Файлы не требуются.")
    return ConversationHandler.END

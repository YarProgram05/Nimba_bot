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
        else:
            raise ValueError("Поддерживаются только cabinet_id 1 или 2")

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
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
            params = {"dateFrom": last_change_date}

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
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
            except Exception as e:
                logger.error(f"Исключение в v1 stocks: {e}", exc_info=True)
                break

        return all_stocks


# ======================
# Обработчики
# ======================

async def start_wb_remains(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета Wildberries"""
    context.user_data['current_flow'] = 'wb_remains'

    keyboard = [
        [InlineKeyboardButton("🏪 WB_1 Nimba", callback_data='wb_cabinet_1')],
        [InlineKeyboardButton("🏬 WB_2 Galioni", callback_data='wb_cabinet_2')]
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
    elif cabinet_data == 'wb_cabinet_2':
        cabinet_id = 2
        cabinet_name = "WB_2 Galioni"
    else:
        await query.message.reply_text("❌ Неизвестный кабинет.")
        return ConversationHandler.END

    context.user_data['wb_cabinet_id'] = cabinet_id

    await query.message.edit_text(f"⏳ Получаю остатки с Wildberries API ({cabinet_name})...")

    try:
        wb_api = WildberriesAPI(cabinet_id=cabinet_id)

        await query.message.reply_text("📊 Запрашиваю остатки по товарам...")
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

        # === 2. Группировка по шаблону Nimba ===
        template_path = os.path.join(root_dir, "База данных артикулов для выкупов и начислений.xlsx")
        if not os.path.exists(template_path):
            template_path = "База данных артикулов для выкупов и начислений.xlsx"
        if not os.path.exists(template_path):
            raise Exception("Файл шаблона не найден!")

        import importlib.util
        spec = importlib.util.spec_from_file_location("template_loader", os.path.join(utils_dir, "template_loader.py"))
        template_loader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(template_loader)

        art_to_id, id_to_name, main_ids_ordered = template_loader.load_template("Шаблон_WB")

        wb_stock_data = {}
        for art, counts in stock_dict.items():
            wb_stock_data[art] = {
                "in_stock": counts['in_stock'],
                "in_way_from_client": counts['in_way_from_client'],
                "in_way_to_client": counts['in_way_to_client']
            }

        grouped, unmatched = group_wb_remains_data(wb_stock_data, art_to_id, id_to_name)

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
                name = id_to_name.get(id_val, f"ID {id_val}")
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

    except Exception as e:
        logger.error(f"Ошибка при получении остатков WB (кабинет {cabinet_id}): {str(e)}", exc_info=True)
        await query.message.reply_text(f"❌ Ошибка: {str(e)}", reply_markup=ReplyKeyboardRemove())

    return ConversationHandler.END


def group_wb_remains_data(stock_data, art_to_id, id_to_name):
    """Группировка данных остатков WB по шаблону"""
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
                    'in_stock': 0,
                    'in_way_from_client': 0,
                    'in_way_to_client': 0
                }

            grouped[group_id]['in_stock'] += stock_data[art]["in_stock"]
            grouped[group_id]['in_way_from_client'] += stock_data[art]["in_way_from_client"]
            grouped[group_id]['in_way_to_client'] += stock_data[art]["in_way_to_client"]
        else:
            unmatched[art] = {
                'name': f"НЕОПОЗНАННЫЙ: {art}",
                'in_stock': stock_data[art]["in_stock"],
                'in_way_from_client': stock_data[art]["in_way_from_client"],
                'in_way_to_client': stock_data[art]["in_way_to_client"]
            }

    return grouped, unmatched


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

async def generate_wb_remains_report(update: Update, context: CallbackContext):
    # Этот обработчик больше не используется — выбор кабинета через кнопки
    await update.message.reply_text("Пожалуйста, используйте команду /wb_remains для выбора кабинета.")
    return ConversationHandler.END
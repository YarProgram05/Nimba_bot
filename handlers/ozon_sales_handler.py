import sys
import os
import pandas as pd
import logging
import requests
from datetime import datetime, timezone
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext, ConversationHandler
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
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
OZON_SALES_CABINET_CHOICE = 8
OZON_SALES_DATE_INPUT = 9


# ======================
# Ozon API Класс (тот же, что и в remains)
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

    def get_fbo_postings(self, since: str, to: str):
        all_postings = []
        offset = 0
        limit = 1000
        while True:
            payload = {
                "dir": "ASC",
                "filter": {"since": since, "to": to},
                "limit": limit,
                "offset": offset,
                "with": {"analytics_data": False, "financial_data": False}
            }
            response = requests.post(
                f"{self.base_url}/v2/posting/fbo/list",
                headers=self.headers,
                json=payload
            )
            if response.status_code != 200:
                raise Exception(f"FBO error {response.status_code}: {response.text}")
            data = response.json()
            postings = data.get("result", [])
            if not postings:
                break
            all_postings.extend(postings)
            if len(postings) < limit:
                break
            offset += limit
        return all_postings

    def get_financial_operations(self, date_from: str, date_to: str):
        all_ops = []
        page = 1
        while True:
            payload = {
                "filter": {"date": {"from": date_from, "to": date_to}},
                "page": page,
                "page_size": 1000
            }
            response = requests.post(
                f"{self.base_url}/v3/finance/transaction/list",
                headers=self.headers,
                json=payload
            )
            if response.status_code != 200:
                raise Exception(f"Finance error {response.status_code}: {response.text}")
            data = response.json()
            ops = data.get("result", {}).get("operations", [])
            if not ops:
                break
            all_ops.extend(ops)
            if page > 100:
                break
            page += 1
        return all_ops

    def get_offer_ids_by_skus(self, skus):
        if not skus:
            return {}

        valid_skus = []
        for s in skus:
            try:
                if isinstance(s, float) and s.is_integer():
                    valid_skus.append(str(int(s)))
                else:
                    valid_skus.append(str(s))
            except (ValueError, TypeError, OverflowError):
                continue

        if not valid_skus:
            return {}

        sku_to_offer = {}
        chunks = [valid_skus[i:i + 1000] for i in range(0, len(valid_skus), 1000)]
        for chunk in chunks:
            payload = {"sku": chunk}
            response = requests.post(
                f"{self.base_url}/v3/product/info/list",
                headers=self.headers,
                json=payload
            )
            if response.status_code == 200:
                items = response.json().get("items", [])
                for item in items:
                    sku = item.get("sku")
                    offer_id = item.get("offer_id")
                    if sku is not None and offer_id:
                        sku_to_offer[str(sku)] = str(offer_id).strip().lower()
        return sku_to_offer


def parse_date_input(date_str: str) -> datetime:
    return datetime.strptime(date_str.strip(), "%d.%m.%Y").replace(tzinfo=timezone.utc)


def create_excel_report(grouped, unmatched, id_to_name, main_ids_ordered, output_path, total_purchases, total_cancels,
                        total_income):
    """Создаёт Excel-отчёт с двумя листами: Сводный и Подробный"""
    wb = Workbook()

    # ===== ЛИСТ 1: Сводный =====
    ws1 = wb.active
    ws1.title = "Сводный"

    # Заголовки (жирные)
    headers1 = ["Показатель", "Значение"]
    ws1.append(headers1)
    for cell in ws1[1]:
        cell.font = Font(bold=True)

    # Данные
    ws1.append(["Выкупы, шт", total_purchases])
    ws1.append(["Отмены, шт", total_cancels])
    ws1.append(["Валовая маржа, руб", total_income])

    # Прибыль на 1 ед
    avg_profit_per_unit = total_income / total_purchases if total_purchases > 0 else 0
    ws1.append(["Прибыль на 1 ед, руб", avg_profit_per_unit])

    # Процент выкупов
    total_shipments = total_purchases + total_cancels
    purchase_percent = (total_purchases / total_shipments * 100) if total_shipments > 0 else 0
    ws1.append(["Процент выкупов", f"{purchase_percent:.2f}%"])

    # ===== ЛИСТ 2: Подробный =====
    ws2 = wb.create_sheet(title="Подробный")

    # Заголовки (жирные)
    headers2 = ["Наименование", "Выкупы, шт", "Валовая маржа, руб", "Прибыль на 1 ед, руб", "Отмены, шт"]
    ws2.append(headers2)
    for cell in ws2[1]:
        cell.font = Font(bold=True)

    # Сначала выводим все артикулы из шаблона (даже если 0)
    for group_id in main_ids_ordered:
        name = id_to_name.get(group_id, f"Группа {group_id}")
        purchases = grouped.get(group_id, {}).get('purchases', 0)
        cancels = grouped.get(group_id, {}).get('cancels', 0)
        income_val = grouped.get(group_id, {}).get('income', 0)
        profit_per_unit = income_val / purchases if purchases > 0 else 0
        ws2.append([name, purchases, income_val, profit_per_unit, cancels])

    # Затем неопознанные артикулы и типы начислений
    unknown_articles = []
    service_types = []

    for art, data in unmatched.items():
        name = data['name']
        if name.startswith("НЕОПОЗНАННЫЙ_АРТИКУЛ:"):
            unknown_articles.append((name, data))
        elif name.lower().startswith("тип_начисления:"):
            clean_name = name.split(":", 1)[-1].strip()
            new_name = f"ТИП_НАЧИСЛЕНИЯ: {clean_name}"
            service_types.append((new_name, data))
        else:
            unknown_articles.append((name, data))

    # Сортируем по алфавиту
    unknown_articles.sort(key=lambda x: x[0])
    service_types.sort(key=lambda x: x[0])

    # Добавляем неопознанные артикулы
    for name, data in unknown_articles:
        purchases = data['purchases']
        cancels = data['cancels']
        income_val = data['income']
        profit_per_unit = income_val / purchases if purchases > 0 else 0
        ws2.append([name, purchases, income_val, profit_per_unit, cancels])

    # Добавляем типы начислений (у них 0 выкупов и отмен)
    for name, data in service_types:
        income_val = data['income']
        ws2.append([name, 0, income_val, 0, 0])

    # ===== ФОРМАТИРОВАНИЕ =====
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    for ws in [ws1, ws2]:
        # Применяем стиль ко всем ячейкам
        for row in ws.iter_rows():
            for cell in row:
                if cell.value is not None:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    cell.border = thin_border

        # Автоподбор ширины
        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

    wb.save(output_path)


async def start_ozon_sales(update: Update, context: CallbackContext) -> int:
    """Начало — выбор кабинета Ozon для продаж"""
    # Устанавливаем состояние для корректной обработки callback
    context.user_data['conversation_state'] = 'ozon_sales_cabinet'
    keyboard = [
        [InlineKeyboardButton("🏪 Озон_1 Nimba", callback_data='cabinet_1')],
        [InlineKeyboardButton("🏬 Озон_2 Galioni", callback_data='cabinet_2')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🏢 Выберите кабинет Ozon для выгрузки продаж:",
        reply_markup=reply_markup
    )

    return OZON_SALES_CABINET_CHOICE


async def handle_cabinet_choice(update: Update, context: CallbackContext) -> int:
    """Обработка выбора кабинета Ozon"""
    query = update.callback_query
    await query.answer()

    cabinet_data = query.data
    cabinet_id = 1 if cabinet_data == 'cabinet_1' else 2

    context.user_data['ozon_cabinet_id'] = cabinet_id

    await query.message.edit_text(
        "📅 Введите период выгрузки продаж в формате ДД.ММ.ГГГГ (например, 01.08.2025):"
    )

    return OZON_SALES_DATE_INPUT


async def handle_date_input(update: Update, context: CallbackContext) -> int:
    """Обработка ввода даты и генерация отчёта"""
    try:
        date_input = update.message.text.strip()
        cabinet_id = context.user_data.get('ozon_cabinet_id', 1)

        # Разделяем даты
        if " - " in date_input:
            start_str, end_str = date_input.split(" - ", 1)
        else:
            # Если введена одна дата — считаем период 1 день
            start_str = end_str = date_input

        start_dt = parse_date_input(start_str)
        end_dt = parse_date_input(end_str)

        if end_dt < start_dt:
            await update.message.reply_text("❌ Дата окончания не может быть раньше начала.")
            return OZON_SALES_DATE_INPUT

        if (end_dt - start_dt).days > 31:
            await update.message.reply_text("❌ Максимальный период — 31 день.")
            return OZON_SALES_DATE_INPUT

        await update.message.reply_text("⏳ Получаю данные продаж с Ozon API...")

        # Инициализируем API
        ozon = OzonAPI(cabinet_id=cabinet_id)

        # Форматы дат
        start_posting = start_dt.strftime("%Y-%m-%dT00:00:00Z")
        end_posting = end_dt.strftime("%Y-%m-%dT23:59:59Z")
        start_finance = start_dt.strftime("%Y-%m-%dT00:00:00.000Z")
        end_finance = end_dt.strftime("%Y-%m-%dT23:59:59.999Z")

        # Получаем FBO-отправления
        postings = ozon.get_fbo_postings(start_posting, end_posting)

        purchases = {}
        cancels = {}
        for p in postings:
            status = p.get("status")
            for prod in p.get("products", []):
                offer_id = str(prod.get("offer_id", "")).strip().lower()
                if not offer_id:
                    continue
                qty = prod.get("quantity", 0)
                if status == "delivered":
                    purchases[offer_id] = purchases.get(offer_id, 0) + qty
                elif status == "cancelled":
                    cancels[offer_id] = cancels.get(offer_id, 0) + qty

        total_purchases = sum(purchases.values())
        total_cancels = sum(cancels.values())

        # Получаем финансовые операции
        operations = ozon.get_financial_operations(start_finance, end_finance)

        # Собираем SKU
        skus = set()
        for op in operations:
            for item in op.get("items", []):
                sku = item.get("sku")
                if sku is not None:
                    skus.add(sku)

        # Получаем маппинг SKU → offer_id
        sku_to_offer = {}
        if skus:
            sku_to_offer = ozon.get_offer_ids_by_skus(list(skus))

        # Собираем начисления
        income = {}
        for op in operations:
            amount = op.get("amount", 0)
            if amount == 0:
                continue

            items = op.get("items", [])
            operation_type_name = op.get("operation_type_name", "").strip()

            if items:
                offer_ids_found = []
                for item in items:
                    sku = item.get("sku")
                    if sku is not None:
                        offer_id = sku_to_offer.get(str(sku))
                        if offer_id:
                            offer_ids_found.append(offer_id)
                if offer_ids_found:
                    split_amount = amount / len(offer_ids_found)
                    for offer_id in offer_ids_found:
                        income[offer_id] = income.get(offer_id, 0) + split_amount
                else:
                    if operation_type_name:
                        art = f"тип_начисления: {operation_type_name}"
                    else:
                        art = f"тип_начисления: {op.get('type', 'other')}"
                    income[art] = income.get(art, 0) + amount
            else:
                if operation_type_name:
                    art = f"тип_начисления: {operation_type_name}"
                else:
                    art = f"тип_начисления: {op.get('type', 'other')}"
                income[art] = income.get(art, 0) + amount

        total_income = sum(income.values())

        # Загружаем шаблон
        import importlib.util
        spec = importlib.util.spec_from_file_location("template_loader", os.path.join(utils_dir, "template_loader.py"))
        template_loader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(template_loader)

        art_to_id, id_to_name, main_ids_ordered = template_loader.load_template("Шаблон_Ozon")

        # Группируем данные
        grouped = {}
        unmatched = {}

        # Инициализируем grouped для всех group_id из шаблона
        for group_id in main_ids_ordered:
            grouped[group_id] = {
                'name': id_to_name.get(group_id, f"Группа {group_id}"),
                'purchases': 0,
                'cancels': 0,
                'income': 0
            }

        all_arts = set(purchases.keys()) | set(cancels.keys()) | set(income.keys())

        for art in all_arts:
            if art.lower().startswith("тип_начисления:"):
                unmatched[art] = {
                    'name': art,
                    'purchases': purchases.get(art, 0),
                    'cancels': cancels.get(art, 0),
                    'income': income.get(art, 0)
                }
                continue

            group_id = art_to_id.get(art)
            if group_id is not None:
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

        # Создаём отчёт
        report_path = f"Ozon_Sales_Report_{start_dt.strftime('%d%m%Y')}_{end_dt.strftime('%d%m%Y')}.xlsx"
        create_excel_report(grouped, unmatched, id_to_name, main_ids_ordered, report_path, total_purchases,
                            total_cancels, total_income)

        # Отправляем файл
        await update.message.reply_document(
            document=open(report_path, 'rb'),
            caption=f"📊 Отчёт по продажам Ozon (Озон {cabinet_id})\nПериод: {start_dt.strftime('%d.%m.%Y')} – {end_dt.strftime('%d.%m.%Y')}",
            reply_markup=ReplyKeyboardRemove()
        )

        # Очистка
        if os.path.exists(report_path):
            os.remove(report_path)

        return ConversationHandler.END

    except ValueError as e:
        await update.message.reply_text(f"❌ Ошибка формата даты: {e}. Попробуйте снова.")
        return OZON_SALES_DATE_INPUT
    except Exception as e:
        logger.error(f"Ошибка при генерации отчёта: {str(e)}", exc_info=True)
        await update.message.reply_text(f"❌ Ошибка: {str(e)}", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
import os
import sys
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
import time
from telegram import Update, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext, ConversationHandler
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
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
from states import WB_SALES_CABINET_CHOICE, WB_SALES_DATE_START, WB_SALES_DATE_END

# Импорт функции из template_loader
from utils.template_loader import get_cabinet_articles_by_template_id


def parse_date_input(date_str: str) -> datetime:
    return datetime.strptime(date_str.strip(), "%d.%m.%Y").replace(tzinfo=timezone.utc)


def validate_date_format(text: str) -> bool:
    import re
    return bool(re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', text.strip()))


async def start_wb_sales(update: Update, context: CallbackContext) -> int:
    context.user_data['current_flow'] = 'wb_sales'

    keyboard = [
        [InlineKeyboardButton("🏪 WB_1 Nimba", callback_data='cabinet_1')],
        [InlineKeyboardButton("🏬 WB_2 Galioni", callback_data='cabinet_2')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    sent_message = await update.message.reply_text(
        "🏢 Выберите кабинет Wildberries для выгрузки продаж:",
        reply_markup=reply_markup
    )
    context.user_data['wb_sales_initial_message_id'] = sent_message.message_id

    return WB_SALES_CABINET_CHOICE


async def handle_wb_sales_cabinet_choice(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    cabinet_data = query.data
    if cabinet_data not in ('cabinet_1', 'cabinet_2'):
        await query.message.reply_text("❌ Неизвестный кабинет.")
        return ConversationHandler.END

    cabinet_id = 1 if cabinet_data == 'cabinet_1' else 2
    context.user_data['wb_sales_cabinet_id'] = cabinet_id

    await query.message.edit_reply_markup(reply_markup=None)
    await query.message.reply_text(
        f"✅ Выбран кабинет: WB {cabinet_id}\n\n"
        "📅 Введите дату начала периода в формате ДД.ММ.ГГГГ:"
    )
    return WB_SALES_DATE_START

async def handle_wb_sales_date_start(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip()
    if not validate_date_format(text):
        await update.message.reply_text("❌ Неверный формат даты. Введите в формате ДД.ММ.ГГГГ:")
        return WB_SALES_DATE_START

    try:
        start_dt = parse_date_input(text)
        today = datetime.now(timezone.utc).date()
        if start_dt.date() > today:
            await update.message.reply_text("❌ Дата начала не может быть в будущем.")
            return WB_SALES_DATE_START
    except Exception as e:
        logger.error(f"Ошибка парсинга даты: {e}")
        await update.message.reply_text("❌ Некорректная дата. Введите в формате ДД.ММ.ГГГГ:")
        return WB_SALES_DATE_START

    context.user_data['wb_sales_start_date'] = text
    await update.message.reply_text("📅 Введите дату окончания периода в формате ДД.ММ.ГГГГ:")
    return WB_SALES_DATE_END


async def handle_wb_sales_date_end(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip()
    if not validate_date_format(text):
        await update.message.reply_text("❌ Неверный формат даты. Введите в формате ДД.ММ.ГГГГ:")
        return WB_SALES_DATE_END

    try:
        start_str = context.user_data['wb_sales_start_date']
        end_str = text

        start_dt_input = datetime.strptime(start_str.strip(), "%d.%m.%Y")
        end_dt_input = datetime.strptime(end_str.strip(), "%d.%m.%Y")

        if end_dt_input < start_dt_input:
            await update.message.reply_text("❌ Дата окончания не может быть раньше начала.")
            return WB_SALES_DATE_END

        today = datetime.now().date()
        if end_dt_input.date() > today:
            await update.message.reply_text("❌ Дата окончания не может быть в будущем.")
            return WB_SALES_DATE_END

    except Exception as e:
        logger.error(f"Ошибка обработки дат: {e}")
        await update.message.reply_text("❌ Ошибка при обработке дат. Введите в формате ДД.ММ.ГГГГ:")
        return WB_SALES_DATE_END

    context.user_data['wb_sales_end_date'] = text
    loading_message = await update.message.reply_text(
        "⏳ Загружаю данные с Wildberries API... Это может занять несколько минут."
    )
    context.user_data['wb_sales_loading_message_id'] = loading_message.message_id

    start_time = time.time()
    try:
        cabinet_id = context.user_data['wb_sales_cabinet_id']
        start_str = context.user_data['wb_sales_start_date']
        end_str = context.user_data['wb_sales_end_date']

        from dotenv import load_dotenv
        load_dotenv()
        api_token = os.getenv(f'WB_API_TOKEN_{cabinet_id}')
        if not api_token:
            raise ValueError(f"❌ WB_API_TOKEN_{cabinet_id} не задан в .env")

        headers = {'Authorization': api_token}
        base_url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

        start_date = start_dt_input.strftime("%Y-%m-%d")
        end_date = end_dt_input.strftime("%Y-%m-%d")

        logger.info(f"Запрос финансового отчёта за период: {start_date} – {end_date}")

        # === ЗАПРОС ФИНАНСОВЫХ ДАННЫХ (для выкупов и прибыли) ===
        params = {"dateFrom": start_date, "dateTo": end_date}
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=60)
            if response.status_code == 429:
                logger.warning("429 Too Many Requests — ждём 5 сек...")
                time.sleep(5)
                response = requests.get(base_url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            all_records = response.json()
            if not isinstance(all_records, list):
                all_records = []
            logger.info(f"Получено записей из reportDetailByPeriod: {len(all_records)}")
        except Exception as e:
            logger.error(f"Ошибка при запросе к reportDetailByPeriod: {e}", exc_info=True)
            all_records = []

        if not all_records:
            await update.message.reply_text("ℹ️ За указанный период данные отсутствуют.")
            return ConversationHandler.END

        # === ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: выгрузка данных из /orders ===
        def fetch_orders_with_articles(api_token: str, start_date: str, end_date: str):
            from datetime import timezone, timedelta
            MSK = timezone(timedelta(hours=3))

            headers_local = {"Authorization": api_token}
            url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
            orders_with_articles = []
            current_date_from = start_date

            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=MSK)
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=MSK)

            while True:
                params_local = {"dateFrom": current_date_from, "flag": 0}
                try:
                    resp = requests.get(url, headers=headers_local, params=params_local, timeout=60)
                    if resp.status_code == 429:
                        time.sleep(5)
                        resp = requests.get(url, headers=headers_local, params=params_local, timeout=60)
                    resp.raise_for_status()
                    orders = resp.json()
                    if not orders:
                        break

                    for order in orders:
                        lch_str = order["lastChangeDate"]
                        if lch_str.endswith('Z'):
                            lch_str = lch_str[:-1] + '+00:00'
                        try:
                            last_change = datetime.fromisoformat(lch_str)
                            if last_change.tzinfo is None:
                                last_change = last_change.replace(tzinfo=timezone.utc)
                            last_change_msk = last_change.astimezone(MSK)
                        except Exception:
                            continue

                        if not (start_dt <= last_change_msk <= end_dt):
                            continue

                        # Определяем, отменён ли заказ В ПЕРИОДЕ
                        cancel_in_period = False
                        if order.get("isCancel") and order.get("cancelDate"):
                            c_date = order["cancelDate"]
                            if "T" not in c_date:
                                c_date += "T00:00:00"
                            try:
                                cancel_dt = datetime.fromisoformat(c_date)
                                if cancel_dt.tzinfo is None:
                                    cancel_dt = cancel_dt.replace(tzinfo=MSK)
                                else:
                                    cancel_dt = cancel_dt.astimezone(MSK)
                                if start_dt <= cancel_dt <= end_dt:
                                    cancel_in_period = True
                            except Exception:
                                pass

                        orders_with_articles.append({
                            "srid": order.get("srid"),
                            "sa_name": order.get("supplierArticle"),  # ← артикул
                            "is_canceled_in_period": cancel_in_period
                        })

                    next_date = orders[-1]["lastChangeDate"].split("T")[0]
                    if next_date == current_date_from:
                        next_date = (datetime.fromisoformat(next_date) + timedelta(days=1)).strftime("%Y-%m-%d")
                    current_date_from = next_date
                except Exception as ex:
                    logger.error(f"Ошибка при выгрузке заказов с артикулами: {ex}")
                    break
            return orders_with_articles

        # === ВЫГРУЗКА СТАТИСТИКИ ИЗ /orders ===
        logger.info("Выгрузка статистики заказов через /api/v1/supplier/orders...")
        orders_with_articles = fetch_orders_with_articles(api_token, start_date, end_date)
        # === АГРЕГАЦИЯ ПО АРТИКУЛАМ ИЗ /orders ===
        article_stats_from_orders = {}
        for order in orders_with_articles:
            sa_name = order.get("sa_name")
            if not sa_name:
                continue
            art_key = str(sa_name).strip().lower()
            if art_key not in article_stats_from_orders:
                article_stats_from_orders[art_key] = {"total": 0, "canceled": 0}
            article_stats_from_orders[art_key]["total"] += 1
            if order["is_canceled_in_period"]:
                article_stats_from_orders[art_key]["canceled"] += 1

        external_total_orders_count = len(orders_with_articles)
        external_gross_cancels = sum(stats["canceled"] for stats in article_stats_from_orders.values())
        logger.info(
            f"Заказов из /orders: {external_total_orders_count}, возвратов с cancelDate в периоде: {external_gross_cancels}")

        # === КЛАССИФИКАЦИЯ ОПЕРАЦИЙ ===
        INCOME_OPERATIONS = {
            "Продажа",
            "Возмещение за выдачу и возврат товаров на ПВЗ",
            "Возмещение издержек по перевозке/по складским операциям с товаром",
            "Компенсация скидки по программе лояльности"
        }
        EXPENSE_OPERATIONS = {
            "Возврат",
            "Логистика",
            "Платная приемка",
            "Удержание",
            "Хранение",
            "Штраф"
        }

        # === АГРЕГАЦИЯ ===
        art_data = {}
        gross_purchases = 0
        gross_cancels_old = 0
        total_income = 0.0

        for rec in all_records:
            oper_name = rec.get("supplier_oper_name")
            if oper_name is None:
                continue

            oper_clean = str(oper_name).strip()
            srid = rec.get("srid")
            sa_name = rec.get("sa_name")
            ppvz = float(rec.get("ppvz_for_pay", 0))

            # ➕ Валовая прибыль: по вашей логике
            if oper_clean in INCOME_OPERATIONS:
                total_income += ppvz
            elif oper_clean in EXPENSE_OPERATIONS:
                total_income -= ppvz
            else:
                logger.debug(f"Операция не учитывается в прибыли: '{oper_clean}' (ppvz={ppvz})")

            # ➕ Количество: только "Продажа" и "Возврат"
            if oper_clean == "Продажа":
                gross_purchases += 1
            elif oper_clean == "Возврат":
                gross_cancels_old += 1
            else:
                # Остальные операции не создают заказов/возвратов
                pass

            # ➕ Агрегация по артикулам
            if sa_name is not None:
                art_key = str(sa_name).strip().lower()
                if art_key not in art_data:
                    art_data[art_key] = {
                        "orders": set(),
                        "purchases": 0,
                        "cancels": 0,
                        "income": 0.0
                    }

                # Прибыль по артикулу
                if oper_clean in INCOME_OPERATIONS:
                    art_data[art_key]["income"] += ppvz
                elif oper_clean in EXPENSE_OPERATIONS:
                    art_data[art_key]["income"] -= ppvz

                # Количество — только для продаж и возвратов
                if oper_clean == "Продажа":
                    art_data[art_key]["purchases"] += 1
                    art_data[art_key]["orders"].add(srid)
                elif oper_clean == "Возврат":
                    art_data[art_key]["cancels"] += 1
                    art_data[art_key]["orders"].add(srid)

        net_purchases = gross_purchases - gross_cancels_old
        net_income = total_income

        # === Остальной код (агрегация по артикулам, Excel, сводка) — без изменений ===
        raw_art_data = []
        for art, data in art_data.items():
            # Прибыль и заказы из финотчёта (для совместимости)
            profit = data["income"]
            orders_cnt = len(data["orders"])

            # 🔥 Берём статистику из /orders
            stats = article_stats_from_orders.get(art, {"total": 0, "canceled": 0})
            total_shipments = stats["total"]
            canceled_count = stats["canceled"]
            net_purchases_art = total_shipments - canceled_count

            if net_purchases_art <= 0:
                continue

            profit_per_unit = profit / net_purchases_art if net_purchases_art > 0 else 0
            purchase_percent = (net_purchases_art / total_shipments * 100) if total_shipments > 0 else 0

            raw_art_data.append({
                "art": art,
                "orders": total_shipments,        # всего отправлений
                "purchases": net_purchases_art,   # выкупов
                "cancels": canceled_count,        # возвратов
                "profit": profit,
                "purchase_percent": purchase_percent,
                "profit_per_unit": profit_per_unit
            })

        raw_art_data.sort(key=lambda x: x["purchases"], reverse=True)

        sheet_name = "Отдельно ВБ Nimba" if cabinet_id == 1 else "Отдельно ВБ Galioni"
        template_id_to_name, template_id_to_cabinet_arts = get_cabinet_articles_by_template_id(sheet_name)

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

        art_to_id = {}
        for template_id, cabinet_arts in template_id_to_cabinet_arts.items():
            for art in cabinet_arts:
                clean_art = str(art).strip().lower()
                art_to_id[clean_art] = template_id

        id_to_name = template_id_to_name

        grouped = {}
        for group_id in main_ids_ordered:
            grouped[group_id] = {
                'name': id_to_name.get(group_id, f"Группа {group_id}"),
                'orders': 0,
                'purchases': 0,
                'cancels': 0,
                'income': 0.0
            }

        unmatched = {}
        for art, data in art_data.items():
            group_id = art_to_id.get(art)
            net_art_purchases = data["purchases"] - data["cancels"]
            if net_art_purchases <= 0:
                continue

            if group_id is not None:
                grouped[group_id]['orders'] += len(data['orders'])
                grouped[group_id]['purchases'] += net_art_purchases
                grouped[group_id]['cancels'] += data['cancels']
                grouped[group_id]['income'] += data['income']
            else:
                unmatched[art] = {
                    'name': f"НЕОПОЗНАННЫЙ_АРТИКУЛ: {art}",
                    'orders': len(data['orders']),
                    'purchases': net_art_purchases,
                    'cancels': data['cancels'],
                    'income': data['income']
                }

        start_naive = datetime.strptime(start_str.strip(), "%d.%m.%Y")
        end_naive = datetime.strptime(end_str.strip(), "%d.%m.%Y")
        report_path = f"WB_Sales_{start_naive.strftime('%d%m%Y')}-{end_naive.strftime('%d%m%Y')}.xlsx"
        create_excel_report(
            grouped, unmatched, id_to_name, main_ids_ordered, report_path,
            external_total_orders_count,  # ← из /orders
            net_purchases,  # ← СТАРАЯ ЛОГИКА (gross_purchases - gross_cancels_old)
            external_gross_cancels,  # ← из /orders (cancelDate в периоде)
            net_income,
            raw_art_data=raw_art_data
        )

        def fmt_num(x):
            if isinstance(x, float):
                return f"{x:,.2f}".replace(",", " ")
            return f"{x:,}".replace(",", " ")

        avg_profit_per_unit = net_income / net_purchases if net_purchases > 0 else 0
        purchase_percent = (net_purchases / external_total_orders_count * 100) if external_total_orders_count > 0 else 0

        text_summary = (
            f"📊 <b>Сводка по продажам Wildberries</b>\n"
            f"Кабинет: <b>WB {cabinet_id}</b>\n"
            f"Период: <b>{start_str} – {end_str}</b>\n\n"
            f"📦 <b>Заказы:</b> {fmt_num(external_total_orders_count)} шт\n"
            f"✅ <b>Выкупы:</b> {fmt_num(net_purchases)} шт\n"
            f"❌ <b>Возвраты:</b> {fmt_num(external_gross_cancels)} шт\n"
            f"💰 <b>Валовая прибыль:</b> {fmt_num(net_income)} ₽\n"
            f"📈 <b>Прибыль на 1 выкуп:</b> {fmt_num(avg_profit_per_unit)} ₽\n"
            f"🔄 <b>Процент выкупов:</b> {purchase_percent:.2f}%"
            f"\n\n🏆 <b>Топ-5 артикулов по выкупам:</b>\n"
        )

        top_5 = raw_art_data[:5]
        if top_5:
            for i, item in enumerate(top_5, 1):
                art = item["art"]
                purchases = item["purchases"]
                profit = item["profit"]
                text_summary += (
                    f"🔹 {i}. <b>{art}</b>\n"
                    f"   ✅ Выкупы: {fmt_num(purchases)} шт\n"
                    f"   💰 Прибыль: {fmt_num(profit)} ₽\n\n"
                )
        else:
            text_summary += "   — Нет данных по выкупам\n"

        await update.message.reply_document(
            document=open(report_path, 'rb'),
            caption=f"📊 Подробный отчёт по выкупам WB (кабинет {cabinet_id})\nПериод: {start_str} – {end_str}"
        )

        await update.message.reply_text(
            text_summary,
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
        )

        if os.path.exists(report_path):
            os.remove(report_path)

        chat_id = update.effective_chat.id
        for msg_key in ['wb_sales_initial_message_id', 'wb_sales_loading_message_id']:
            msg_id = context.user_data.get(msg_key)
            if msg_id:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                except Exception as e:
                    logger.warning(f"Не удалось удалить сообщение {msg_key}: {e}")

    except Exception as e:
        logger.error(f"Ошибка при генерации WB отчёта: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )
        chat_id = update.effective_chat.id
        for msg_key in ['wb_sales_initial_message_id', 'wb_sales_loading_message_id']:
            msg_id = context.user_data.get(msg_key)
            if msg_id:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                except Exception as e:
                    logger.warning(f"Не удалось удалить сообщение {msg_key} при ошибке: {e}")

    return ConversationHandler.END

def create_excel_report(grouped, unmatched, id_to_name, main_ids_ordered, output_path,
                        total_orders, total_purchases, total_cancels, total_income,
                        raw_art_data=None):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Сводный"

    headers1 = ["Показатель", "Значение"]
    ws1.append(headers1)
    for cell in ws1[1]:
        cell.font = Font(bold=True)

    ws1.append(["Заказы, шт", total_orders])
    ws1.append(["Выкупы, шт", total_purchases])
    ws1.append(["Возвраты/Отмены, шт", total_cancels])
    ws1.append(["Валовая прибыль, руб", total_income])

    avg_profit_per_unit = total_income / total_purchases if total_purchases > 0 else 0
    ws1.append(["Прибыль на 1 ед, руб", avg_profit_per_unit])

    total_shipments = total_purchases + total_cancels
    purchase_percent = (total_purchases / total_shipments * 100) if total_shipments > 0 else 0
    ws1.append(["Процент выкупов", f"{purchase_percent:.2f}%"])

    ws1.append([])

    if raw_art_data and len(raw_art_data) > 0:
        top_5 = raw_art_data[:5]
        ws1.append(["🏆 ТОП-5 артикулов по выкупам"])
        header_cell = ws1.cell(row=ws1.max_row, column=1)
        header_cell.font = Font(bold=True, size=12)
        header_cell.alignment = Alignment(horizontal="center")
        ws1.append([])
        top_headers = ["Место", "Артикул", "Выкупы, шт", "Прибыль, ₽"]
        ws1.append(top_headers)
        for col in range(1, len(top_headers) + 1):
            cell = ws1.cell(row=ws1.max_row, column=col)
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center")

        for i, item in enumerate(top_5, 1):
            ws1.append([
                i,
                item["art"],
                item["purchases"],
                item["profit"]
            ])

    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    for row in ws1.iter_rows():
        for cell in row:
            if cell.value is not None:
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = thin_border

    for col in ws1.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws1.column_dimensions[column].width = adjusted_width

    ws2 = wb.create_sheet(title="Подробный")
    headers2 = [
        "Наименование",
        "Выкупы, шт",
        "Валовая прибыль, руб",
        "Процент выкупов",
        "Прибыль на 1 ед, руб",
        "Заказы, шт",
        "Возвраты/Отмены, шт"
    ]
    ws2.append(headers2)
    for cell in ws2[1]:
        cell.font = Font(bold=True)

    red_fill = PatternFill(start_color="FF9999", end_color="FF9999", fill_type="solid")
    orange_fill = PatternFill(start_color="FFCC99", end_color="FFCC99", fill_type="solid")

    row_index = 2
    for group_id in main_ids_ordered:
        data = grouped.get(group_id, {})
        name = data.get('name', f"Группа {group_id}")
        orders = data.get('orders', 0)
        purchases = data.get('purchases', 0)
        cancels = data.get('cancels', 0)
        income_val = data.get('income', 0)

        profit_per_unit = income_val / purchases if purchases > 0 else 0
        total_shipments = purchases + cancels
        purchase_percent_val = (purchases / total_shipments * 100) if total_shipments > 0 else 0

        ws2.append([
            name,
            purchases,
            income_val,
            f"{purchase_percent_val:.2f}%",
            profit_per_unit,
            orders,
            cancels
        ])

        percent_cell = ws2.cell(row=row_index, column=4)
        if purchase_percent_val <= 50:
            percent_cell.fill = red_fill
        elif 50 < purchase_percent_val <= 60:
            percent_cell.fill = orange_fill
        row_index += 1

    unknown_articles = []
    for art, data in unmatched.items():
        name = data['name']
        if name.startswith("НЕОПОЗНАННЫЙ_АРТИКУЛ:"):
            unknown_articles.append((name, data))
        else:
            unknown_articles.append((name, data))

    unknown_articles.sort(key=lambda x: x[0])
    for name, data in unknown_articles:
        orders = data.get('orders', 0)
        purchases = data.get('purchases', 0)
        cancels = data.get('cancels', 0)
        income_val = data.get('income', 0)
        profit_per_unit = income_val / purchases if purchases > 0 else 0
        total_shipments = purchases + cancels
        purchase_percent_val = (purchases / total_shipments * 100) if total_shipments > 0 else 0

        ws2.append([
            name,
            purchases,
            income_val,
            f"{purchase_percent_val:.2f}%",
            profit_per_unit,
            orders,
            cancels
        ])

        percent_cell = ws2.cell(row=row_index, column=4)
        if purchase_percent_val <= 50:
            percent_cell.fill = red_fill
        elif 50 < purchase_percent_val <= 60:
            percent_cell.fill = orange_fill
        row_index += 1

    if raw_art_data:
        ws3 = wb.create_sheet(title="Исходные артикулы")
        headers3 = [
            "Артикул (sa_name)",
            "Выкупы, шт",
            "Валовая прибыль, руб",
            "Процент выкупов",
            "Прибыль на 1 ед, руб",
            "Заказы, шт",
            "Возвраты/Отмены, шт"
        ]
        ws3.append(headers3)
        for cell in ws3[1]:
            cell.font = Font(bold=True)

        row_idx = 2
        for item in raw_art_data:
            art = item["art"]
            purchases = item["purchases"]
            profit = item["profit"]
            purchase_percent = item["purchase_percent"]
            profit_per_unit = item["profit_per_unit"]
            orders = item["orders"]
            cancels = item["cancels"]

            ws3.append([
                art,
                purchases,
                profit,
                f"{purchase_percent:.2f}%",
                profit_per_unit,
                orders,
                cancels
            ])

            percent_cell = ws3.cell(row=row_idx, column=4)
            if purchase_percent <= 50:
                percent_cell.fill = red_fill
            elif 50 < purchase_percent <= 60:
                percent_cell.fill = orange_fill
            row_idx += 1

    for ws in [ws2, ws3] if raw_art_data else [ws2]:
        for row in ws.iter_rows():
            for cell in row:
                if cell.value is not None:
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                    cell.border = thin_border

        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

    wb.save(output_path)
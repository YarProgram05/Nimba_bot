import sys
import os
import pandas as pd
import logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler
from states import WB_REMAINS_FILES


# Получаем пути
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')

# Добавляем пути в sys.path
if root_dir not in sys.path:
    sys.path.append(root_dir)
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

logger = logging.getLogger(__name__)


async def start_wb_remains(update: Update, context: CallbackContext) -> int:
    """Начало обработки остатков Wildberries"""
    context.user_data['wb_remains_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте файл остатков для Wildberries с названием 'ВБ_остатки.xlsx'\n\n"
        "После отправки файла нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return WB_REMAINS_FILES  # Состояние ожидания файлов остатков WB


async def handle_wb_remains_files(update: Update, context: CallbackContext) -> int:
    """Обработка файлов остатков Wildberries"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return WB_REMAINS_FILES

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_wb_remains_{file_name}"
    await file.download_to_drive(file_path)

    # Сохранение файла
    user_data.setdefault('wb_remains_files', []).append(file_path)
    await update.message.reply_text(f"✅ Файл остатков Wildberries '{file_name}' получен")

    return WB_REMAINS_FILES


async def generate_wb_remains_report(update: Update, context: CallbackContext) -> int:
    """Генерация отчета по остаткам Wildberries"""
    user_data = context.user_data
    remains_files = user_data.get('wb_remains_files', [])

    if not remains_files:
        await update.message.reply_text(
            "❌ Не получены файлы для формирования отчета по остаткам!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Обрабатываю файлы остатков Wildberries...")

        # Загрузка шаблона
        template_path = os.path.join(root_dir, "База данных артикулов для выкупов и начислений.xlsx")
        if not os.path.exists(template_path):
            template_path = "База данных артикулов для выкупов и начислений.xlsx"

        if os.path.exists(template_path):
            # Импортируем template_loader напрямую
            import importlib.util
            spec = importlib.util.spec_from_file_location("template_loader",
                                                          os.path.join(utils_dir, "template_loader.py"))
            template_loader = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(template_loader)

            art_to_id, id_to_name, main_ids_ordered = template_loader.load_template("Шаблон_WB")
        else:
            await update.message.reply_text("❌ Файл шаблона не найден!")
            return ConversationHandler.END

        # Обработка файла остатков
        file_path = remains_files[0]  # Берем первый файл

        # Создаем отчет
        report_path = "WB_Remains_Report.xlsx"

        # Вызываем функцию обработки остатков WB
        success, report_data = process_wb_remains(file_path, art_to_id, id_to_name, main_ids_ordered)

        if success:
            # Создаем DataFrame и сохраняем отчет
            report_df = pd.DataFrame(report_data, columns=[
                "Артикул",
                "Доступно на складах",
                "В пути до покупателей",
                "Возвращаются от покупателей",
                "Итого на МП"
            ])
            report_df.to_excel(report_path, index=False)

            # Отправка отчета
            await update.message.reply_document(
                document=open(report_path, 'rb'),
                caption="📊 Отчет по остаткам Wildberries",
                reply_markup=ReplyKeyboardRemove()
            )

            # Очистка временных файлов
            os.remove(file_path)
            os.remove(report_path)
        else:
            raise Exception("Ошибка при обработке файла остатков")

    except Exception as e:
        logger.error(f"Ошибка обработки остатков Wildberries: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при обработке файлов остатков Wildberries: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END


def process_wb_remains(input_file, art_to_id, id_to_name, main_ids_ordered):
    """Обработка файла остатков Wildberries с использованием шаблона"""
    try:
        # Чтение файла Excel
        df = pd.read_excel(input_file)

        # Поиск необходимых столбцов по шаблону
        columns = {}
        target_columns = [
            "Артикул продавца",
            "В пути до получателей",
            "В пути возвраты на склад WB",
            "Всего находится на складах"
        ]

        for col in df.columns:
            for target in target_columns:
                if target in col:
                    if "Артикул" in target:
                        columns['article'] = col
                    elif "получателей" in target:
                        columns['to_clients'] = col
                    elif "возвраты" in target:
                        columns['returns'] = col
                    elif "складах" in target:
                        columns['in_stock'] = col
                    break

        # Проверка наличия всех столбцов
        if len(columns) != 4:
            found_columns = ", ".join(columns.values()) if columns else "не найдены"
            raise ValueError(f"Не найдены все необходимые столбцы. Найдены: {found_columns}")

        # Выбор нужных столбцов
        df_selected = df[list(columns.values())]
        df_selected.columns = ['article', 'to_clients', 'returns', 'in_stock']

        # Очистка и подготовка артикулов
        df_selected['article'] = df_selected['article'].apply(lambda x: str(x).strip().lower() if pd.notna(x) else "")

        # Собираем данные остатков
        stock_data = {}
        for _, row in df_selected.iterrows():
            article = row['article']
            if article and article != "nan":
                if article not in stock_data:
                    stock_data[article] = {"available": 0, "returning": 0, "prepare": 0}
                stock_data[article]["available"] += int(row['in_stock']) if not pd.isna(row['in_stock']) else 0
                stock_data[article]["returning"] += int(row['returns']) if not pd.isna(row['returns']) else 0
                stock_data[article]["prepare"] += int(row['to_clients']) if not pd.isna(row['to_clients']) else 0

        # Группировка данных по шаблону
        all_arts = set(stock_data.keys())
        grouped = {}
        unmatched = {}

        for art in all_arts:
            group_id = art_to_id.get(art, None)

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

        # Создаем отчет в СТРОГОМ порядке main_ids_ordered
        report_data = []

        # Сначала добавляем все артикулы из шаблона в порядке main_ids_ordered
        for id_val in main_ids_ordered:
            if id_val in grouped:
                data = grouped[id_val]
                total = data['available'] + data['returning'] + data['prepare']
                report_data.append([
                    data['name'],
                    data['available'],
                    data['prepare'],  # В пути до покупателей
                    data['returning'],  # Возвращаются от покупателей
                    total
                ])
            else:
                # Если артикул не найден в данных, добавляем с нулями
                name = id_to_name.get(id_val, f"ID {id_val}")
                report_data.append([
                    name,
                    0,
                    0,
                    0,
                    0
                ])

        # Затем добавляем неопознанные артикулы
        for art, data in unmatched.items():
            total = data['available'] + data['returning'] + data['prepare']
            report_data.append([
                data['name'],
                data['available'],
                data['prepare'],  # В пути до покупателей
                data['returning'],  # Возвращаются от покупателей
                total
            ])

        return True, report_data
    except Exception as e:
        logger.error(f"Ошибка при обработке остатков WB: {str(e)}", exc_info=True)
        return False, []


def group_wb_remains_data(stock_data, art_to_id, id_to_name):
    """Группировка данных остатков WB по шаблону (как в продажах)"""
    all_arts = set(stock_data.keys())

    grouped = {}
    unmatched = {}

    for art in all_arts:
        group_id = art_to_id.get(art, None)

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
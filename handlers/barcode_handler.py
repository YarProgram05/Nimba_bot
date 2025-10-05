import sys
import os
import pandas as pd
import re
import tempfile
import shutil
import zipfile
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler
import logging

from states import BARCODE_FILES
# Попробуем импортировать code128
try:
    import code128

    CODE128_AVAILABLE = True
except ImportError as e:
    print(f"Ошибка импорта code128: {e}")
    CODE128_AVAILABLE = False

# Получаем пути
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')

# Добавляем пути в sys.path
if root_dir not in sys.path:
    sys.path.append(root_dir)
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

import logging

logger = logging.getLogger(__name__)


# Регистрация русского шрифта
def register_font():
    """Регистрация шрифта для PDF"""
    try:
        # Явно указываем путь к Arial в проекте
        font_path = os.path.join(root_dir, 'arial.ttf')

        if os.path.exists(font_path):
            pdfmetrics.registerFont(TTFont('CustomFont', font_path))
            logger.info(f"Шрифт зарегистрирован: {font_path}")
            return True
        else:
            logger.warning(f"Файл шрифта не найден: {font_path}")
            return False
    except Exception as e:
        logger.error(f"Ошибка регистрации шрифта: {e}")
        return False


# Регистрируем шрифт при импорте
font_available = register_font()


def safe_filename(s):
    """Создает безопасное имя файла"""
    if pd.isna(s):
        return "unknown"
    return re.sub(r'[\\/*?:"<>|]', '', str(s)).strip()


def wrap_text(c, text, width, font_name, font_size):
    """Разбивает текст на строки по указанной ширине"""
    if pd.isna(text):
        text = ""
    words = str(text).split()
    lines = []
    current_line = []

    for word in words:
        test_line = ' '.join(current_line + [word])
        test_width = c.stringWidth(test_line, font_name, font_size)
        if test_width <= width:
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]

    if current_line:
        lines.append(' '.join(current_line))

    return lines


def generate_label_pdf(row, output_dir):
    """Генерирует PDF-файл с этикетками и штрихкодами"""
    try:
        # Проверяем количество
        try:
            qty = int(float(str(row['Количество']).strip()))
        except (ValueError, TypeError):
            logger.warning(f"Ошибка: неверное количество '{row['Количество']}' для строки. Пропуск.")
            return False

        # Формирование имени файла
        art = safe_filename(row.get('Артикул продавца', ''))
        filename = f"{art}_{qty}_шт.pdf"
        filepath = os.path.join(output_dir, filename)

        # Создание PDF документа
        label_width = 58 * mm
        label_height = 40 * mm
        c = canvas.Canvas(filepath, pagesize=(label_width, label_height))

        # Генерация штрихкода
        barcode_value = str(row['Баркод']).strip() if not pd.isna(row['Баркод']) else ''
        temp_file_path = None

        if barcode_value and CODE128_AVAILABLE:
            try:
                # Создаем временную директорию для изображения штрихкода
                temp_dir = tempfile.mkdtemp()
                temp_file_path = os.path.join(temp_dir, "barcode.png")

                # Генерируем изображение штрихкода
                barcode_image = code128.image(barcode_value, height=100)

                # Сохраняем изображение
                barcode_image.save(temp_file_path, "PNG")

                # Рассчитываем размеры для вставки в PDF
                img_width, img_height = barcode_image.size
                aspect_ratio = img_width / img_height

                # Устанавливаем максимальную ширину и высоту для штрихкода
                max_barcode_width = 50 * mm
                max_barcode_height = 15 * mm

                # Сохраняем пропорции
                if aspect_ratio > 1:
                    barcode_width = min(max_barcode_width, max_barcode_height * aspect_ratio)
                    barcode_height = barcode_width / aspect_ratio
                else:
                    barcode_height = min(max_barcode_height, max_barcode_width / aspect_ratio)
                    barcode_width = barcode_height * aspect_ratio

                # Позиция штрихкода (по центру)
                barcode_x = (label_width - barcode_width) / 2
                barcode_y = label_height - barcode_height - 2 * mm

            except Exception as e:
                logger.error(f"Ошибка генерации штрихкода: {e}")
                temp_file_path = None
                barcode_x = 0
                barcode_y = label_height - 12 * mm
        else:
            barcode_x = 0
            barcode_y = label_height - 12 * mm

        # Определяем какой шрифт использовать
        font_name = "CustomFont" if font_available else "Helvetica"

        # Создание этикеток
        for _ in range(qty):
            # Штрихкод
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    c.drawImage(
                        temp_file_path,
                        barcode_x,
                        barcode_y,
                        width=barcode_width,
                        height=barcode_height,
                        preserveAspectRatio=True,
                        mask='auto'
                    )
                except Exception as img_error:
                    logger.error(f"Ошибка при добавлении изображения штрихкода: {img_error}")

            # Текстовые данные с улучшенным оформлением
            # Заголовок (более крупный шрифт)
            header_font_size = 9
            c.setFont(font_name, header_font_size)

            # Начинаем текст ниже штрихкода
            if temp_file_path and os.path.exists(temp_file_path):
                current_y = barcode_y - 3 * mm
            else:
                current_y = label_height - 12 * mm

            line_height = 3 * mm  # Оптимизированное расстояние между строками
            text_width_limit = label_width - 4 * mm  # Ширина для текста с отступами

            # Формирование данных
            fields = [
                ("", row.get('Баркод', '')),
                ("", row.get('Наименование', '')),
                ("Арт.:", row.get('Артикул продавца', '')),
            ]

            # Добавляем цвет и размер в одном формате
            color = row.get('Цвет на бирке', '')
            size = row.get('Размер на бирке', '')
            if not pd.isna(color) or not pd.isna(size):
                if not pd.isna(color) and not pd.isna(size):
                    color_size_text = f"Цв.: {color} / Раз.: {size}"
                elif not pd.isna(color):
                    color_size_text = f"Цв.: {color}"
                else:
                    color_size_text = f"Раз.: {size}"
                fields.append(("", color_size_text))

            # Добавляем остальные поля
            fields.extend([
                ("Сост.:", row.get('Состав на бирке', '')),
                ("", row.get('ИП', '')),
            ])

            # Вывод текста с автоматическим переносом и центрированием
            normal_font_size = 7
            c.setFont(font_name, normal_font_size)

            for prefix, value in fields:
                if pd.isna(value):
                    value = ''

                # Формируем текст
                if prefix:
                    text = f"{prefix} {value}".strip()
                else:
                    text = str(value).strip()

                # Пропускаем пустые строки
                if not text:
                    continue

                # Разбиваем текст на строки с автоматическим переносом
                lines = wrap_text(c, text, text_width_limit, font_name, normal_font_size)

                # Выводим каждую строку с центрированием
                for line in lines:
                    if current_y < 3 * mm:
                        break

                    # Центрирование текста
                    line_width = c.stringWidth(line, font_name, normal_font_size)
                    text_x = (label_width - line_width) / 2
                    c.drawString(text_x, current_y, line)
                    current_y -= line_height

            c.showPage()

        c.save()
        logger.info(f"Создан файл: {filename}")

        # Удаляем временные файлы
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                shutil.rmtree(os.path.dirname(temp_file_path))
            except Exception as e:
                logger.error(f"Ошибка удаления временных файлов: {e}")

        return True

    except Exception as e:
        logger.error(f"Ошибка при создании этикетки: {e}", exc_info=True)
        # Удаляем временные файлы при ошибке
        if 'temp_file_path' in locals() and temp_file_path:
            try:
                shutil.rmtree(os.path.dirname(temp_file_path))
            except:
                pass
        return False

async def start_barcode_generation(update: Update, context: CallbackContext) -> int:
    """Начало генерации штрихкодов"""
    context.user_data['barcode_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте Excel файл с данными для генерации штрихкодов.\n\n"
        "Файл должен содержать столбцы:\n"
        "• Баркод\n"
        "• Количество\n"
        "• Наименование\n"
        "• Артикул продавца\n"
        "• Цвет на бирке\n"
        "• Размер на бирке\n"
        "• Состав на бирке\n"
        "• ИП\n"
        "После отправки файла нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return BARCODE_FILES  # Состояние ожидания файлов штрихкодов


async def handle_barcode_files(update: Update, context: CallbackContext) -> int:
    """Обработка файлов для генерации штрихкодов"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("❌ Файл должен быть в формате Excel (.xlsx)")
        return BARCODE_FILES

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_barcode_{file_name}"
    await file.download_to_drive(file_path)

    # Сохранение файла
    user_data.setdefault('barcode_files', []).append(file_path)
    await update.message.reply_text(f"✅ Файл '{file_name}' получен")

    return BARCODE_FILES


async def generate_barcode_report(update: Update, context: CallbackContext) -> int:
    """Генерация штрихкодов и отправка ZIP архива"""
    user_data = context.user_data
    barcode_files = user_data.get('barcode_files', [])

    if not barcode_files:
        await update.message.reply_text(
            "❌ Не получены файлы для генерации штрихкодов!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Генерирую штрихкоды...")

        # Создаем временную директорию для PDF файлов
        temp_dir = tempfile.mkdtemp()
        output_dir = os.path.join(temp_dir, "barcodes")
        os.makedirs(output_dir, exist_ok=True)

        # Обработка файла
        file_path = barcode_files[0]  # Берем первый файл

        # Чтение Excel-файла
        try:
            df = pd.read_excel(
                file_path,
                dtype=str,
                header=0  # Первая строка как заголовок
            )
            # Заполнение пропущенных значений
            df = df.fillna('')

            logger.info(f"Загружены столбцы: {list(df.columns)}")
            logger.info(f"Количество строк: {len(df)}")
        except Exception as e:
            raise Exception(f"Ошибка чтения файла: {e}")

        # Проверяем наличие обязательных столбцов
        required_columns = ['Баркод', 'Количество']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise Exception(f"Отсутствуют обязательные столбцы: {', '.join(missing_columns)}")

        # Отладочная информация
        logger.info(f"Загружено {len(df)} строк из Excel файла")
        await update.message.reply_text(f"📥 Загружено {len(df)} строк из файла")

        # Обработка данных
        processed_count = 0
        success_count = 0
        error_messages = []

        for index, row in df.iterrows():
            try:
                # Проверка на пустые значения
                barcode = str(row.get('Баркод', '')).strip()
                quantity = str(row.get('Количество', '')).strip()

                # Пропускаем строки без баркода или количества
                if not barcode or barcode.lower() == 'nan':
                    error_messages.append(f"Строка {index + 1}: пропущен баркод")
                    continue

                if not quantity or quantity.lower() == 'nan':
                    error_messages.append(f"Строка {index + 1}: пропущено количество")
                    continue

                # Пытаемся преобразовать количество в число
                try:
                    qty = int(float(quantity))  # Обрабатываем случаи с дробными числами
                except (ValueError, TypeError):
                    error_messages.append(f"Строка {index + 1}: неверный формат количества '{quantity}'")
                    continue

                processed_count += 1
                logger.info(f"Обработка строки {index + 1}: баркод={barcode}, количество={quantity}")

                if generate_label_pdf(row, output_dir):
                    success_count += 1
                    logger.info(f"Успешно обработана строка {index + 1}")
                else:
                    error_messages.append(f"Строка {index + 1}: ошибка генерации PDF")
                    logger.warning(f"Не удалось обработать строку {index + 1}")

            except Exception as e:
                error_msg = f"Строка {index + 1}: {str(e)}"
                error_messages.append(error_msg)
                logger.error(error_msg, exc_info=True)
                continue

        # Логируем все ошибки
        for error in error_messages:
            logger.warning(error)

        logger.info(f"Обработано {processed_count} строк, успешно сгенерировано {success_count} файлов")
        await update.message.reply_text(f"📊 Обработано {processed_count} строк, сгенерировано {success_count} файлов")

        if processed_count == 0:
            error_text = "Не найдено данных для обработки. Возможные причины:\n"
            error_text += "\n".join(error_messages[:5])  # Показываем первые 5 ошибок
            if len(error_messages) > 5:
                error_text += f"\n... и еще {len(error_messages) - 5} ошибок"
            raise Exception(error_text)

        # Проверяем сгенерированные файлы
        generated_files = []
        if os.path.exists(output_dir):
            generated_files = [f for f in os.listdir(output_dir) if f.endswith('.pdf')]

        if not generated_files:
            raise Exception("Не удалось сгенерировать ни одной этикетки. Проверьте формат данных в файле.")

        # Создаем ZIP архив и отправляем
        zip_path = "barcodes.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in generated_files:
                file_path_full = os.path.join(output_dir, file)
                zipf.write(file_path_full, file)

        await update.message.reply_document(
            document=open(zip_path, 'rb'),
            caption=f"📊 Сгенерировано {len(generated_files)} PDF файлов со штрихкодами",
            reply_markup=ReplyKeyboardRemove()
        )

        # Очистка временных файлов
        try:
            os.remove(file_path)
            os.remove(zip_path)
            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.error(f"Ошибка при очистке временных файлов: {e}")

    except Exception as e:
        logger.error(f"Ошибка генерации штрихкодов: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при генерации штрихкодов: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END
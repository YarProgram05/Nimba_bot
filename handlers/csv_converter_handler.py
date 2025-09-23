import sys
import os
import pandas as pd
import tempfile
import shutil
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import CallbackContext, ConversationHandler
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')

if root_dir not in sys.path:
    sys.path.append(root_dir)
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

logger = logging.getLogger(__name__)


def detect_encoding_simple(file_path):
    """Простое определение кодировки без внешних библиотек"""
    encodings_to_try = ['utf-8', 'utf-8-sig', 'windows-1251', 'cp1251', 'latin-1']

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)  # Читаем немного чтобы проверить
            return encoding
        except UnicodeDecodeError:
            continue
        except Exception:
            continue

    return 'utf-8'  # По умолчанию


def clean_csv_file(input_file, output_file=None, encoding='utf-8'):
    """
    Очищает CSV файл от пустых строк и сохраняет временную версию
    """
    if output_file is None:
        output_file = input_file + '.cleaned'

    try:
        with open(input_file, 'r', encoding=encoding, errors='ignore') as infile:
            lines = infile.readlines()

        # Убираем пустые строки и строки только с разделителями
        cleaned_lines = []
        for line in lines:
            stripped_line = line.strip()
            # Пропускаем полностью пустые строки и строки только с ;
            if stripped_line and not all(char in [';', ' ', '\t', '\ufeff'] for char in stripped_line):
                cleaned_lines.append(line)

        # Сохраняем очищенный файл
        with open(output_file, 'w', encoding='utf-8') as outfile:
            outfile.writelines(cleaned_lines)

        return output_file, len(lines) - len(cleaned_lines)

    except Exception as e:
        logger.error(f"Ошибка при очистке файла: {e}")
        return input_file, 0


def csv_to_xlsx(input_file, output_file=None, encoding='utf-8'):
    """
    Конвертирует CSV файл в XLSX формат без потери данных
    """
    try:
        if not os.path.exists(input_file):
            logger.error(f"Ошибка: Файл '{input_file}' не найден")
            return False

        # Автоопределение кодировки
        if encoding == 'auto':
            detected_encoding = detect_encoding_simple(input_file)
            logger.info(f"Определена кодировка: {detected_encoding}")
            encoding = detected_encoding

        # Определяем выходной файл
        if output_file is None:
            output_file = os.path.splitext(input_file)[0] + '.xlsx'

        logger.info(f"Конвертация: {input_file} -> {output_file}")
        logger.info(f"Кодировка: {encoding}")

        # Очищаем файл от пустых строк
        cleaned_file, removed_lines = clean_csv_file(input_file, encoding=encoding)
        if removed_lines > 0:
            logger.info(f"Удалено пустых строк: {removed_lines}")

        # Пробуем разные методы чтения
        success = False
        df = None

        # Метод 1: Стандартное чтение
        try:
            df = pd.read_csv(
                cleaned_file,
                delimiter=';',
                encoding=encoding,
                dtype=str,
                keep_default_na=False,
                quotechar='"',
                on_bad_lines='skip'  # Изменено для совместимости
            )
            success = True
            logger.info("Успешно прочитано стандартным методом")
        except Exception as e:
            logger.warning(f"Стандартный метод не сработал: {e}")

        # Метод 2: С engine='python'
        if not success:
            try:
                df = pd.read_csv(
                    cleaned_file,
                    delimiter=';',
                    encoding=encoding,
                    dtype=str,
                    engine='python',
                    error_bad_lines=False,
                    warn_bad_lines=True
                )
                success = True
                logger.info("Успешно прочитано с engine='python'")
            except Exception as e:
                logger.warning(f"Метод с engine='python' не сработал: {e}")

        # Метод 3: Ручное чтение как текста
        if not success:
            try:
                logger.info("Пробуем ручное чтение...")
                with open(cleaned_file, 'r', encoding=encoding) as f:
                    lines = f.readlines()

                data = []
                for line in lines:
                    if line.strip():
                        # Разделяем по точке с запятой и чистим значения
                        row = [cell.strip().strip('"') for cell in line.split(';')]
                        data.append(row)

                # Создаем DataFrame
                if data:
                    headers = data[0]
                    df_data = data[1:] if len(data) > 1 else []
                    df = pd.DataFrame(df_data, columns=headers)
                    success = True
                    logger.info("Успешно прочитано ручным методом")
            except Exception as e:
                logger.warning(f"Ручной метод не сработал: {e}")

        if not success or df is None:
            logger.error("❌ Не удалось прочитать файл")
            return False

        # Если создавали временный файл - удаляем его
        if cleaned_file != input_file and os.path.exists(cleaned_file):
            try:
                os.remove(cleaned_file)
            except:
                pass

        logger.info(f"Успешно прочитано: {len(df)} строк, {len(df.columns)} колонок")

        # Сохраняем в XLSX
        df.to_excel(output_file, index=False, engine='openpyxl')

        logger.info(f"✅ Успешно сохранено: {output_file}")
        return True

    except UnicodeDecodeError as e:
        logger.error(f"Ошибка кодировки: {e}")
        return False

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при конвертации: {str(e)}")
        return False


async def start_csv_conversion(update: Update, context: CallbackContext) -> int:
    """Начало конвертации CSV в XLSX"""
    context.user_data['csv_files'] = []

    # Создание клавиатуры
    buttons = [["Все файлы отправлены"]]
    reply_markup = ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await update.message.reply_text(
        "📤 Пожалуйста, отправьте CSV файлы для конвертации в формат XLSX.\n\n"
        "Файлы должны быть в кодировке UTF-8 или Windows-1251 и использовать точку с запятой (;) как разделитель.\n\n"
        "После отправки файлов нажмите кнопку ниже ⬇️",
        reply_markup=reply_markup
    )

    return 6  # Состояние ожидания CSV файлов


async def handle_csv_files(update: Update, context: CallbackContext) -> int:
    """Обработка CSV файлов"""
    user_data = context.user_data
    document = update.message.document
    file_name = document.file_name

    # Проверка типа файла
    if not file_name.lower().endswith('.csv'):
        await update.message.reply_text("❌ Файл должен быть в формате CSV (.csv)")
        return 6

    # Скачивание файла
    file = await context.bot.get_file(document)
    file_path = f"temp_csv_{file_name}"
    await file.download_to_drive(file_path)

    # Сохранение файла
    user_data.setdefault('csv_files', []).append(file_path)
    await update.message.reply_text(f"✅ Файл '{file_name}' получен")

    return 6


async def generate_xlsx_files(update: Update, context: CallbackContext) -> int:
    """Конвертация CSV файлов в XLSX и отправка файлов по отдельности"""
    user_data = context.user_data
    csv_files = user_data.get('csv_files', [])

    if not csv_files:
        await update.message.reply_text(
            "❌ Не получены файлы для конвертации!",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    try:
        await update.message.reply_text("⏳ Конвертирую CSV файлы в XLSX...")

        converted_files = []
        failed_files = []

        # Конвертируем каждый файл
        for file_path in csv_files:
            try:
                # Получаем имя файла без пути
                file_name = os.path.basename(file_path)
                xlsx_file_name = os.path.splitext(file_name)[0] + '.xlsx'

                # Создаем временный файл для XLSX
                temp_dir = tempfile.mkdtemp()
                xlsx_file_path = os.path.join(temp_dir, xlsx_file_name)

                # Конвертируем файл
                if csv_to_xlsx(file_path, xlsx_file_path, encoding='auto'):
                    converted_files.append({
                        'path': xlsx_file_path,
                        'name': xlsx_file_name,
                        'temp_dir': temp_dir
                    })
                    logger.info(f"Успешно сконвертирован: {file_name}")
                else:
                    failed_files.append(file_name)
                    # Очищаем временную директорию при ошибке
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    logger.error(f"Ошибка конвертации: {file_name}")

            except Exception as e:
                failed_files.append(os.path.basename(file_path))
                logger.error(f"Ошибка при обработке файла {file_path}: {e}")

        # Отправляем сконвертированные файлы по отдельности
        if converted_files:
            success_count = 0
            for file_info in converted_files:
                try:
                    # Отправляем файл
                    await update.message.reply_document(
                        document=open(file_info['path'], 'rb'),
                        caption=f"📊 Сконвертированный файл: {file_info['name']}"
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки файла {file_info['name']}: {e}")
                    failed_files.append(file_info['name'])
                finally:
                    # Закрываем файл и удаляем временные данные
                    try:
                        # Закрываем файл (он будет автоматически закрыт при выходе из контекста)
                        pass
                    except:
                        pass

            # Финальное сообщение
            final_message = f"✅ Успешно отправлено {success_count} файлов в формате XLSX"
            if failed_files:
                final_message += f"\n❌ Не удалось отправить {len(failed_files)} файлов"

            await update.message.reply_text(
                final_message,
                reply_markup=ReplyKeyboardRemove()
            )

            # Очистка временных файлов
            try:
                for file_path in csv_files:
                    if os.path.exists(file_path):
                        os.remove(file_path)

                # Удаляем временные директории
                for file_info in converted_files:
                    if os.path.exists(file_info['temp_dir']):
                        shutil.rmtree(file_info['temp_dir'])
            except Exception as e:
                logger.error(f"Ошибка при очистке временных файлов: {e}")

        else:
            await update.message.reply_text(
                "❌ Не удалось сконвертировать ни один файл",
                reply_markup=ReplyKeyboardRemove()
            )

    except Exception as e:
        logger.error(f"Ошибка конвертации CSV в XLSX: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при конвертации файлов: {str(e)}",
            reply_markup=ReplyKeyboardRemove()
        )

    return ConversationHandler.END
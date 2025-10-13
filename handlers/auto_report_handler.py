# handlers/auto_report_handler.py

import logging
from telegram import Update, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes, ConversationHandler

from utils.auto_report_manager import load_auto_reports, save_auto_reports, schedule_job

logger = logging.getLogger(__name__)

# Состояния
from states import (
    AUTO_REPORT_TOGGLE,
    AUTO_REPORT_FREQUENCY,
    AUTO_REPORT_TIME,
    AUTO_REPORT_WEEKLY_DAY,
    AUTO_REPORT_DAILY_TIME
)

# Единственная callback-функция
from handlers.all_mp_remains_handler import send_all_mp_remains_automatic

# Константы
HOUR_OPTIONS = ["1", "2", "3", "4", "5", "6", "12", "24"]
DAY_OPTIONS = ["1", "2", "3", "4", "5", "6", "7"]

DAYS_OF_WEEK = {
    0: "Понедельник",
    1: "Вторник",
    2: "Среда",
    3: "Четверг",
    4: "Пятница",
    5: "Суббота",
    6: "Воскресенье"
}


async def start_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало настройки автоотчётов — с inline-кнопками"""
    logger.info("✅ start_auto_report вызван!")

    reports = load_auto_reports()
    chat_id = str(update.effective_chat.id)
    user_config = reports.get(chat_id, {})
    enabled = user_config.get('enabled', False)

    status = "включены" if enabled else "выключены"

    # Inline-кнопки
    keyboard = [
        [
            InlineKeyboardButton("✅ Включить", callback_data="auto_toggle_on"),
            InlineKeyboardButton("❌ Выключить", callback_data="auto_toggle_off")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"Автоотчёты по всем маркетплейсам сейчас {status}.\n\nВыберите действие:",
        reply_markup=reply_markup
    )
    return AUTO_REPORT_TOGGLE


async def handle_toggle_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия inline-кнопок включения/выключения"""
    query = update.callback_query
    await query.answer()

    chat_id = update.effective_chat.id
    data = query.data
    logger.info(f"🔍 handle_toggle_inline: {data} от chat_id={chat_id}")

    # Удаляем inline-кнопки из сообщения
    await query.edit_message_reply_markup(reply_markup=None)

    if data == "auto_toggle_off":
        reports = load_auto_reports()
        chat_id_str = str(chat_id)
        if chat_id_str in reports:
            reports[chat_id_str]['enabled'] = False
            save_auto_reports(reports)
            current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat_id}")
            for job in current_jobs:
                job.schedule_removal()
        await query.message.reply_text("✅ Автоотчёты отключены.")
        return ConversationHandler.END

    elif data == "auto_toggle_on":
        await query.message.reply_text("Выбрано: ✅ Включить")

        # Отправляем следующий шаг — выбор типа интервала
        keyboard = [
            [
                InlineKeyboardButton("🕗 По часам", callback_data="interval_hours"),
                InlineKeyboardButton("📅 По дням", callback_data="interval_days")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            "Выберите тип интервала отправки:",
            reply_markup=reply_markup
        )
        return AUTO_REPORT_FREQUENCY

    else:
        await query.message.reply_text("⚠️ Неизвестное действие.")
        return ConversationHandler.END


async def handle_interval_type_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора 'по часам' или 'по дням' через inline-кнопки"""
    query = update.callback_query
    await query.answer()

    data = query.data
    logger.info(f"🔍 handle_interval_type_inline: {data}")

    # Удаляем кнопки
    await query.edit_message_reply_markup(reply_markup=None)

    if data == "interval_hours":
        await query.message.reply_text("Выбрано: 🕗 По часам")
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_hours'}}
        logger.info("💾 Сохранено: interval_hours в user_data")

        # Inline-кнопки для часов
        keyboard = []
        for i in range(0, len(HOUR_OPTIONS), 4):
            row = [InlineKeyboardButton(h, callback_data=f"hour_{h}") for h in HOUR_OPTIONS[i:i + 4]]
            keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Выберите интервал в часах:", reply_markup=reply_markup)
        return AUTO_REPORT_TIME

    elif data == "interval_days":
        await query.message.reply_text("Выбрано: 📅 По дням")
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_days'}}
        logger.info("💾 Сохранено: interval_days в user_data")

        # Inline-кнопки для дней
        keyboard = []
        for i in range(0, len(DAY_OPTIONS), 4):
            row = [InlineKeyboardButton(d, callback_data=f"day_{d}") for d in DAY_OPTIONS[i:i + 4]]
            keyboard.append(row)

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Выберите интервал в днях:", reply_markup=reply_markup)
        return AUTO_REPORT_TIME

    else:
        await query.message.reply_text("⚠️ Неизвестный тип интервала.")
        return ConversationHandler.END


async def handle_time_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора часов или дней через inline-кнопки"""
    query = update.callback_query
    await query.answer()

    data = query.data
    logger.info(f"🔢 handle_time_inline: {data}")

    await query.edit_message_reply_markup(reply_markup=None)

    config = context.user_data.get('auto_report_config', {})
    if not config or 'schedule' not in config:
        await query.message.reply_text("Ошибка конфигурации. Начните заново.")
        return ConversationHandler.END

    sched_type = config['schedule']['type']

    if data.startswith("hour_"):
        hours = data.split("_")[1]
        if hours in HOUR_OPTIONS:
            await query.message.reply_text(f"Выбрано: {hours} ч")
            config['schedule']['hours'] = int(hours)
            await _finalize_auto_report_from_query(query, context)
            return ConversationHandler.END

    elif data.startswith("day_"):
        days_str = data.split("_")[1]
        if days_str in DAY_OPTIONS:
            days = int(days_str)
            await query.message.reply_text(f"Выбрано: {days} дн")
            config['schedule']['days'] = days
            if days == 7:
                logger.info("🗓️ Режим 7 дней — показываем inline-кнопки выбора дня недели")
                keyboard = []
                for i in range(0, 7, 2):
                    row = []
                    for j in range(2):
                        if i + j < 7:
                            row.append(InlineKeyboardButton(DAYS_OF_WEEK[i + j], callback_data=f"weekly_day_{i + j}"))
                    keyboard.append(row)
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.reply_text("Выберите день недели для еженедельного отчёта:",
                                                reply_markup=reply_markup)
                return AUTO_REPORT_WEEKLY_DAY
            else:
                await query.message.reply_text(
                    "Введите время отправки в формате ЧЧ:ММ (например, 10:00):"
                )
                return AUTO_REPORT_DAILY_TIME

    await query.message.reply_text("Неверный выбор. Попробуйте снова.")
    return AUTO_REPORT_TIME


async def handle_weekly_day_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора дня недели для 7 дней"""
    query = update.callback_query
    await query.answer()
    day_of_week = int(query.data.split("_")[-1])
    day_name = DAYS_OF_WEEK[day_of_week]
    logger.info(f"📅 Выбран день недели: {day_name} ({day_of_week})")

    # Удаляем inline-клавиатуру
    await query.edit_message_reply_markup(reply_markup=None)

    await query.message.reply_text(f"Выбрано: {day_name}")

    # Сохраняем выбор
    if 'auto_report_config' not in context.user_data:
        context.user_data['auto_report_config'] = {'schedule': {}}
    context.user_data['auto_report_config']['schedule']['day_of_week'] = day_of_week

    await query.message.reply_text("Введите время отправки в формате ЧЧ:ММ (например, 10:00):")
    return AUTO_REPORT_DAILY_TIME


async def handle_daily_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод времени для 1-6 дней или после выбора дня недели"""
    time_str = update.message.text.strip()
    logger.info(f"🕒 Введено время: {repr(time_str)}")

    try:
        parts = time_str.split(':')
        if len(parts) != 2:
            raise ValueError("Неверный формат")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Неверное время")
    except Exception as e:
        logger.warning(f"⚠️ Неверное время: {e}")
        await update.message.reply_text(
            "Неверный формат времени. Введите ЧЧ:ММ (например, 10:00):"
        )
        return AUTO_REPORT_DAILY_TIME

    await update.message.reply_text(f"Выбрано время: {time_str}")

    if 'auto_report_config' not in context.user_data:
        await update.message.reply_text("Ошибка конфигурации. Начните заново.")
        return ConversationHandler.END

    context.user_data['auto_report_config']['schedule']['time'] = f"{hour:02d}:{minute:02d}"
    await finalize_auto_report(update, context)
    return ConversationHandler.END


async def finalize_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохранение и запуск задачи (для текстового ввода времени)"""
    chat_id = update.effective_chat.id
    await _finalize_auto_report_common(chat_id, context, update.message)


async def _finalize_auto_report_from_query(query, context):
    """Сохранение и запуск задачи (для inline-выбора без времени)"""
    chat_id = query.message.chat_id
    await _finalize_auto_report_common(chat_id, context, query.message)


async def _finalize_auto_report_common(chat_id, context, message):
    """Общая логика финализации автоотчёта"""
    config = context.user_data.get('auto_report_config', {})

    full_config = {
        'enabled': True,
        'report_type': 'all_mp',
        'schedule': config['schedule'],
        'chat_id': chat_id
    }

    # Сохраняем
    reports = load_auto_reports()
    reports[str(chat_id)] = full_config
    save_auto_reports(reports)

    # Удаляем старую задачу
    current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat_id}")
    for job in current_jobs:
        job.schedule_removal()

    # Запускаем новую
    schedule_job(context.application, send_all_mp_remains_automatic, full_config, {'chat_id': chat_id}, chat_id)

    # Формируем ответ
    sched = config['schedule']
    if sched['type'] == 'interval_hours':
        details = f"Каждые {sched['hours']} ч"
    elif sched['type'] == 'interval_days':
        if sched.get('day_of_week') is not None:
            day_name = DAYS_OF_WEEK[sched['day_of_week']]
            details = f"Каждый {day_name} в {sched['time']}"
        else:
            details = f"Каждые {sched['days']} дн в {sched['time']}"

    await message.reply_text(
        f"✅ Автоотчёт по всем маркетплейсам настроен!\n\n"
        f"Интервал: {details}\n\n"
        f"Первый отчёт придёт по расписанию.",
        reply_markup=ReplyKeyboardRemove()
    )
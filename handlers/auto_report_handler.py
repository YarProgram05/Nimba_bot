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

from handlers.all_mp_remains_handler import send_all_mp_remains_automatic

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


async def _delete_message_if_exists(context, chat, message_id):
    """Безопасное удаление сообщения"""
    if message_id:
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {message_id}: {e}")


def get_current_schedule_description(reports, chat_id_str):
    user_config = reports.get(chat_id_str, {})
    if not user_config.get('enabled'):
        return ""

    sched = user_config.get('schedule', {})
    sched_type = sched.get('type')

    if sched_type == 'interval_hours':
        return f"Каждые {sched['hours']} ч"
    elif sched_type == 'interval_days':
        if 'day_of_week' in sched:
            day_name = DAYS_OF_WEEK.get(sched['day_of_week'], "Неизвестный день")
            return f"Каждый {day_name} в {sched['time']}"
        else:
            return f"Каждые {sched['days']} дн в {sched['time']}"
    return ""


async def _send_message_and_save_id(context, chat, text, reply_markup=None):
    """Отправка сообщения с сохранением ID"""
    sent = await chat.send_message(text, reply_markup=reply_markup)
    context.user_data['current_message_id'] = sent.message_id
    return sent.message_id


# === ШАГ 1: Старт ===
async def start_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("✅ start_auto_report вызван!")

    # Очищаем конфигурацию при новом запуске
    context.user_data.pop('auto_report_config', None)
    context.user_data.pop('current_message_id', None)

    reports = load_auto_reports()
    chat = update.effective_chat
    chat_id = str(chat.id)
    user_config = reports.get(chat_id, {})
    enabled = user_config.get('enabled', False)

    status = "включены" if enabled else "выключены"
    description = ""
    if enabled:
        description = get_current_schedule_description(reports, chat_id)
        if description:
            description = f"\nТекущая настройка: {description}\n"

    keyboard = [
        [
            InlineKeyboardButton("✅ Включить", callback_data="auto_toggle_on"),
            InlineKeyboardButton("❌ Выключить", callback_data="auto_toggle_off")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await _send_message_and_save_id(context, chat,
        f"Автоотчёты по всем маркетплейсам сейчас {status}.{description}\nВыберите действие:",
        reply_markup
    )
    return AUTO_REPORT_TOGGLE


# === ШАГ 2: Включение/выключение ===
async def handle_toggle_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data
    logger.info(f"🔍 handle_toggle_inline: {data}")

    # Удаляем стартовое сообщение
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "auto_toggle_off":
        reports = load_auto_reports()
        chat_id_str = str(chat.id)
        if chat_id_str in reports:
            reports[chat_id_str]['enabled'] = False
            save_auto_reports(reports)
            current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat.id}")
            for job in current_jobs:
                job.schedule_removal()
        await chat.send_message("✅ Автоотчёты отключены.")
        return ConversationHandler.END

    elif data == "auto_toggle_on":
        # Единое сообщение: выбор + меню
        keyboard = [
            [
                InlineKeyboardButton("🕗 По часам", callback_data="interval_hours"),
                InlineKeyboardButton("📅 По дням", callback_data="interval_days")
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_toggle")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            "Выбрано: ✅ Включить\n\nВыберите тип интервала отправки:",
            reply_markup
        )
        return AUTO_REPORT_FREQUENCY

    else:
        await chat.send_message("⚠️ Неизвестное действие.")
        return ConversationHandler.END


# === ШАГ 3: Выбор типа интервала ===
async def handle_interval_type_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data
    logger.info(f"🔍 handle_interval_type_inline: {data}")

    # Удаляем сообщение выбора интервала
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_toggle":
        return await start_auto_report(update, context)

    if data == "interval_hours":
        # Сохраняем конфигурацию
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_hours'}}
        keyboard = []
        for i in range(0, len(HOUR_OPTIONS), 4):
            row = [InlineKeyboardButton(h, callback_data=f"hour_{h}") for h in HOUR_OPTIONS[i:i + 4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_frequency")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            "Выбрано: 🕗 По часам\n\nВыберите интервал в часах:",
            reply_markup
        )
        return AUTO_REPORT_TIME

    elif data == "interval_days":
        # Сохраняем конфигурацию
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_days'}}
        keyboard = []
        for i in range(0, len(DAY_OPTIONS), 4):
            row = [InlineKeyboardButton(d, callback_data=f"day_{d}") for d in DAY_OPTIONS[i:i + 4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_frequency")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            "Выбрано: 📅 По дням\n\nВыберите интервал в днях:",
            reply_markup
        )
        return AUTO_REPORT_TIME

    else:
        await chat.send_message("⚠️ Неизвестный тип интервала.")
        return ConversationHandler.END


# === ШАГ 4: Выбор часов/дней ===
async def handle_time_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data
    logger.info(f"🔢 handle_time_inline: {data}")

    # Удаляем сообщение выбора часов/дней
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_frequency":
        # Возвращаемся к выбору интервала
        keyboard = [
            [
                InlineKeyboardButton("🕗 По часам", callback_data="interval_hours"),
                InlineKeyboardButton("📅 По дням", callback_data="interval_days")
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_toggle")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            "Выберите тип интервала отправки:",
            reply_markup
        )
        return AUTO_REPORT_FREQUENCY

    config = context.user_data.get('auto_report_config', {})
    if not config or 'schedule' not in config:
        await chat.send_message("Ошибка конфигурации. Начните заново.")
        return ConversationHandler.END

    sched_type = config['schedule']['type']

    if data.startswith("hour_"):
        hours = data.split("_")[1]
        if hours in HOUR_OPTIONS:
            await chat.send_message(f"✅ Автоотчёт настроен: каждые {hours} ч")
            config['schedule']['hours'] = int(hours)
            await _finalize_auto_report_common(chat.id, context, chat)
            return ConversationHandler.END

    elif data.startswith("day_"):
        days_str = data.split("_")[1]
        if days_str in DAY_OPTIONS:
            days = int(days_str)
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
                keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_time")])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await _send_message_and_save_id(context, chat,
                    "Выберите день недели для еженедельного отчёта:",
                    reply_markup
                )
                return AUTO_REPORT_WEEKLY_DAY
            else:
                keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_time_input")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await _send_message_and_save_id(context, chat,
                    "Введите время отправки в формате ЧЧ:ММ (например, 10:00):",
                    reply_markup
                )
                return AUTO_REPORT_DAILY_TIME

    await chat.send_message("Неверный выбор. Попробуйте снова.")
    return ConversationHandler.END


# === ШАГ 5: Выбор дня недели ===
async def handle_weekly_day_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data

    # Удаляем сообщение выбора дня недели
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_time":
        config = context.user_data.get('auto_report_config', {})
        if config.get('schedule', {}).get('type') == 'interval_days':
            days = config['schedule'].get('days', 7)
            keyboard = []
            for i in range(0, len(DAY_OPTIONS), 4):
                row = [InlineKeyboardButton(d, callback_data=f"day_{d}") for d in DAY_OPTIONS[i:i + 4]]
                keyboard.append(row)
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_frequency")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await _send_message_and_save_id(context, chat,
                f"Выбрано: {days} дн\n\nВыберите интервал в днях:",
                reply_markup
            )
            return AUTO_REPORT_TIME

    day_of_week = int(data.split("_")[-1])
    day_name = DAYS_OF_WEEK[day_of_week]
    logger.info(f"📅 Выбран день недели: {day_name} ({day_of_week})")

    if 'auto_report_config' not in context.user_data:
        context.user_data['auto_report_config'] = {'schedule': {}}
    context.user_data['auto_report_config']['schedule']['day_of_week'] = day_of_week

    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_time_input")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await _send_message_and_save_id(context, chat,
        f"Выбрано: {day_name}\n\nВведите время отправки в формате ЧЧ:ММ (например, 10:00):",
        reply_markup
    )
    return AUTO_REPORT_DAILY_TIME


# === ШАГ 6: Ввод времени ===
async def handle_daily_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_str = update.message.text.strip()
    logger.info(f"🕒 Введено время: {repr(time_str)}")

    chat = update.effective_chat

    try:
        parts = time_str.split(':')
        if len(parts) != 2:
            raise ValueError("Неверный формат")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Неверное время")
    except Exception as e:
        logger.warning(f"⚠️ Неверное время: {e}")
        keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_time_input")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await chat.send_message(
            "Неверный формат времени. Введите ЧЧ:ММ (например, 10:00):",
            reply_markup=reply_markup
        )
        return AUTO_REPORT_DAILY_TIME

    if 'auto_report_config' not in context.user_data:
        await chat.send_message("Ошибка конфигурации. Начните заново.")
        return ConversationHandler.END

    context.user_data['auto_report_config']['schedule']['time'] = f"{hour:02d}:{minute:02d}"
    await _finalize_auto_report_common(chat.id, context, chat)
    return ConversationHandler.END


# === Обработка "Назад" из ввода времени ===
async def handle_back_from_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat

    # Удаляем сообщение ввода времени
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    config = context.user_data.get('auto_report_config', {})
    sched = config.get('schedule', {})
    days = sched.get('days', 1)

    if days == 7:
        keyboard = []
        for i in range(0, 7, 2):
            row = []
            for j in range(2):
                if i + j < 7:
                    row.append(InlineKeyboardButton(DAYS_OF_WEEK[i + j], callback_data=f"weekly_day_{i + j}"))
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_time")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            "Выберите день недели для еженедельного отчёта:",
            reply_markup
        )
        return AUTO_REPORT_WEEKLY_DAY
    else:
        keyboard = []
        for i in range(0, len(DAY_OPTIONS), 4):
            row = [InlineKeyboardButton(d, callback_data=f"day_{d}") for d in DAY_OPTIONS[i:i + 4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_frequency")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            f"Выбрано: {days} дн\n\nВыберите интервал в днях:",
            reply_markup
        )
        return AUTO_REPORT_TIME


# === Финализация ===
async def _finalize_auto_report_common(chat_id, context, chat):
    config = context.user_data.get('auto_report_config', {})

    full_config = {
        'enabled': True,
        'report_type': 'all_mp',
        'schedule': config['schedule'],
        'chat_id': chat_id
    }

    reports = load_auto_reports()
    reports[str(chat_id)] = full_config
    save_auto_reports(reports)

    current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat_id}")
    for job in current_jobs:
        job.schedule_removal()

    schedule_job(context.application, send_all_mp_remains_automatic, full_config, {'chat_id': chat_id}, chat_id)

    sched = config['schedule']
    if sched['type'] == 'interval_hours':
        details = f"Каждые {sched['hours']} ч"
    elif sched['type'] == 'interval_days':
        if sched.get('day_of_week') is not None:
            day_name = DAYS_OF_WEEK[sched['day_of_week']]
            details = f"Каждый {day_name} в {sched['time']}"
        else:
            details = f"Каждые {sched['days']} дн в {sched['time']}"

    await chat.send_message(
        f"✅ Автоотчёт по всем маркетплейсам настроен!\n\n"
        f"Интервал: {details}\n\n"
        f"Первый отчёт придёт по расписанию.",
        reply_markup=ReplyKeyboardRemove()
    )
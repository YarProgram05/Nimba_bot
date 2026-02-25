# handlers/auto_report_handler.py

import logging
from telegram import Update, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes, ConversationHandler

from utils.auto_report_manager import (
    load_auto_reports,
    save_auto_reports,
    schedule_job,
    get_user_report_config,
    set_user_report_config
)

logger = logging.getLogger(__name__)

# Состояния
from states import (
    AUTO_REPORT_TOGGLE,
    AUTO_REPORT_FREQUENCY,
    AUTO_REPORT_TIME,
    AUTO_REPORT_WEEKLY_DAY,
    AUTO_REPORT_DAILY_TIME,
    AUTO_REPORT_START_TIME,
    AUTO_REPORT_START_DAY,
    SELECTING_AUTO_REPORT_TYPE
)
# Типы отчётов
AUTO_REPORT_TYPES = {
    "all_mp_remains": "Остатки на всех МП"
}

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
    if message_id:
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=message_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {message_id}: {e}")

def get_current_schedule_description_for_type(config):
    if not config.get('enabled'):
        return ""
    sched = config.get('schedule', {})
    sched_type = sched.get('type')

    if sched_type == 'interval_hours':
        return f"Каждые {sched['hours']} ч, начиная с {sched['start_time']}"
    elif sched_type == 'interval_days':
        if 'day_of_week' in sched:
            day_name = DAYS_OF_WEEK.get(sched['day_of_week'], "Неизвестный день")
            return f"Каждый {day_name} в {sched['time']}"
        else:
            start_day_name = DAYS_OF_WEEK.get(sched['start_day'], "??")
            return f"Каждые {sched['days']} дн, начиная с {start_day_name} в {sched['time']}"
    return ""


async def _send_message_and_save_id(context, chat, text, reply_markup=None, parse_mode=None):
    sent = await chat.send_message(text, reply_markup=reply_markup, parse_mode=parse_mode)
    context.user_data['current_message_id'] = sent.message_id
    return sent.message_id

async def _show_report_type_selection(context: ContextTypes.DEFAULT_TYPE, chat):
    """Показывает меню выбора типа автоотчёта."""
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"select_report_type_{key}")]
        for key, name in AUTO_REPORT_TYPES.items()
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    sent = await chat.send_message(
        "Выберите тип автоотчёта:",
        reply_markup=reply_markup
    )
    context.user_data['current_message_id'] = sent.message_id


# === ШАГ 1: Старт ===
async def start_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("✅ start_auto_report вызван!")

    # Очищаем старые данные
    keys_to_clear = [k for k in context.user_data.keys() if k.startswith('auto_report_') or k == 'current_message_id']
    for k in keys_to_clear:
        context.user_data.pop(k, None)

    chat = update.effective_chat
    await _show_report_type_selection(context, chat)
    return SELECTING_AUTO_REPORT_TYPE

async def handle_select_report_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("select_report_type_"):
        await query.message.reply_text("⚠️ Неизвестный тип отчёта.")
        return ConversationHandler.END

    report_type = data.split("_", 3)[-1]
    if report_type not in AUTO_REPORT_TYPES:
        await query.message.reply_text("⚠️ Неизвестный тип отчёта.")
        return ConversationHandler.END

    context.user_data['selected_report_type'] = report_type
    context.user_data['selected_report_label'] = AUTO_REPORT_TYPES[report_type]

    # <<< УДАЛЯЕМ ПРЕДЫДУЩЕЕ СООБЩЕНИЕ (меню выбора типа) >>>
    current_msg_id = context.user_data.get('current_message_id')
    chat = query.message.chat
    if current_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat.id, message_id=current_msg_id)
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {current_msg_id}: {e}")

    # Получаем конфиг
    chat_id = chat.id
    chat_id_str = str(chat_id)
    reports = load_auto_reports()
    user_config = get_user_report_config(reports, chat_id_str, report_type)
    enabled = user_config.get('enabled', False)

    status = "включены" if enabled else "выключены"
    description = ""
    if enabled:
        description = get_current_schedule_description_for_type(user_config)
        if description:
            description = f"\nТекущая настройка: {description}\n"

    keyboard = [
        [
            InlineKeyboardButton("✅ Включить", callback_data="auto_toggle_on"),
            InlineKeyboardButton("❌ Выключить", callback_data="auto_toggle_off")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # <<< ОТПРАВЛЯЕМ НОВОЕ СООБЩЕНИЕ И СОХРАНЯЕМ ЕГО ID >>>
    sent = await chat.send_message(
        f"Автоотчёт «{AUTO_REPORT_TYPES[report_type]}» сейчас {status}.{description}\nВыберите действие:",
        reply_markup=reply_markup
    )
    context.user_data['current_message_id'] = sent.message_id

    return AUTO_REPORT_TOGGLE


# === ШАГ 2: Включение/выключение ===
async def handle_toggle_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data
    logger.info(f"🔍 handle_toggle_inline: {data}")

    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "auto_toggle_off":
        report_type = context.user_data.get('selected_report_type', 'all_mp_remains')

        # <<< ИСПРАВЛЕНИЕ: определяем chat_id_str >>>
        chat_id = chat.id
        chat_id_str = str(chat_id)

        reports = load_auto_reports()
        user_configs = reports.get(chat_id_str, {})
        if report_type in user_configs:
            user_configs[report_type]['enabled'] = False
            save_auto_reports(reports)

        # Удаляем задачи для этого типа
        current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat.id}_{report_type}")
        for job in current_jobs:
            job.schedule_removal()

        await chat.send_message(f"✅ Автоотчёт «{AUTO_REPORT_TYPES.get(report_type, 'Неизвестный')}» отключён.")
        return ConversationHandler.END

    elif data == "auto_toggle_on":
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

    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_toggle":
        # <<< ИСПРАВЛЕНИЕ: не вызываем start_auto_report! >>>
        await _show_report_type_selection(context, chat)
        return SELECTING_AUTO_REPORT_TYPE

    if data == "interval_hours":
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

    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_frequency":
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
            config['schedule']['hours'] = int(hours)
            keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_start_time")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await _send_message_and_save_id(context, chat,
                "Введите время начала отсчёта в формате ЧЧ:ММ (например, 10:00):",
                reply_markup
            )
            return AUTO_REPORT_START_TIME

    elif data.startswith("day_"):
        days_str = data.split("_")[1]
        if days_str in DAY_OPTIONS:
            days = int(days_str)
            config['schedule']['days'] = days
            # <<< СОХРАНЯЕМ В КОНТЕКСТ >>>
            context.user_data['auto_report_config']['schedule']['days'] = days
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
                # Отправляем объяснение с жирным шрифтом через HTML
                explanation = (
                    "ℹ️ Важно! Чтобы настроить автоотчёт правильно:\n\n"
                    "Укажите день отсчёта <b>из текущей недели</b> (Пн–Вс).\n\n"
                    "Бот рассчитает ближайшую дату отправки, начиная с этого дня.\n\n"
                    "Выберите день начала отсчёта:"
                )
                keyboard = []
                for i in range(0, 7, 2):
                    row = []
                    for j in range(2):
                        if i + j < 7:
                            row.append(InlineKeyboardButton(DAYS_OF_WEEK[i + j], callback_data=f"start_day_{i + j}"))
                    keyboard.append(row)
                keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_time")])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await _send_message_and_save_id(
                    context, chat, explanation, reply_markup, parse_mode="HTML"
                )
                return AUTO_REPORT_START_DAY

    await chat.send_message("Неверный выбор. Попробуйте снова.")
    return ConversationHandler.END


# === ШАГ 5: Выбор дня недели (только для 7 дней) ===
async def handle_weekly_day_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data

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


# === ШАГ 6: Ввод времени (для дней и недель) ===
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


# === ШАГ 7: Ввод времени начала (для часов) ===
async def handle_start_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    time_str = update.message.text.strip()
    logger.info(f"🕒 Введено время начала: {repr(time_str)}")

    chat = update.effective_chat

    try:
        parts = time_str.split(':')
        if len(parts) != 2:
            raise ValueError("Неверный формат")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Неверное время")
    except Exception as e:
        logger.warning(f"⚠️ Неверное время начала: {e}")
        keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_start_time")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await chat.send_message(
            "Неверный формат времени. Введите ЧЧ:ММ (например, 10:00):",
            reply_markup=reply_markup
        )
        return AUTO_REPORT_START_TIME

    if 'auto_report_config' not in context.user_data:
        await chat.send_message("Ошибка конфигурации. Начните заново.")
        return ConversationHandler.END

    context.user_data['auto_report_config']['schedule']['start_time'] = f"{hour:02d}:{minute:02d}"
    await _finalize_auto_report_common(chat.id, context, chat)
    return ConversationHandler.END


# === ШАГ 8: Выбор дня начала (для дней, кроме 7) ===
async def handle_start_day_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    data = query.data

    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    if data == "back_to_time":
        # Возвращаемся к выбору интервала в днях
        config = context.user_data.get('auto_report_config', {})
        if config.get('schedule', {}).get('type') == 'interval_days':
            days = config['schedule'].get('days', 1)
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

    start_day = int(data.split("_")[-1])
    day_name = DAYS_OF_WEEK[start_day]
    logger.info(f"📅 Выбран день начала: {day_name} ({start_day})")

    if 'auto_report_config' not in context.user_data:
        context.user_data['auto_report_config'] = {'schedule': {}}
    context.user_data['auto_report_config']['schedule']['start_day'] = start_day

    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_from_time_input")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await _send_message_and_save_id(context, chat,
        f"Выбрано: {day_name}\n\nВведите время отправки в формате ЧЧ:ММ (например, 10:00):",
        reply_markup
    )
    return AUTO_REPORT_DAILY_TIME


# === Обработка "Назад" из ввода времени (для дней и недель) ===
async def handle_back_from_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    # <<< ПОЛУЧАЕМ days из контекста >>>
    config = context.user_data.get('auto_report_config', {})
    sched = config.get('schedule', {})
    days = sched.get('days', 1)

    if days == 7:
        # Еженедельный режим
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
        # Циклический режим (<7 дней) — показываем объяснение!
        explanation = (
            "ℹ️ Важно! Чтобы настроить автоотчёт правильно:\n\n"
            "Укажите день отсчёта <b>из текущей недели</b> (Пн–Вс).\n\n"
            "Бот рассчитает ближайшую дату отправки, начиная с этого дня.\n\n"
            "Выберите день начала отсчёта:"
        )
        keyboard = []
        for i in range(0, 7, 2):
            row = []
            for j in range(2):
                if i + j < 7:
                    row.append(InlineKeyboardButton(DAYS_OF_WEEK[i + j], callback_data=f"start_day_{i + j}"))
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_time")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(
            context, chat, explanation, reply_markup, parse_mode="HTML"
        )
        return AUTO_REPORT_START_DAY


# === Обработка "Назад" из ввода времени начала (для часов) ===
async def handle_back_from_start_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat = query.message.chat
    current_msg_id = context.user_data.get('current_message_id')
    await _delete_message_if_exists(context, chat, current_msg_id)

    config = context.user_data.get('auto_report_config', {})
    if config.get('schedule', {}).get('type') == 'interval_hours':
        hours = config['schedule'].get('hours', 1)
        keyboard = []
        for i in range(0, len(HOUR_OPTIONS), 4):
            row = [InlineKeyboardButton(h, callback_data=f"hour_{h}") for h in HOUR_OPTIONS[i:i + 4]]
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_frequency")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await _send_message_and_save_id(context, chat,
            f"Выбрано: {hours} ч\n\nВыберите интервал в часах:",
            reply_markup
        )
        return AUTO_REPORT_TIME


# === Финализация ===
async def _finalize_auto_report_common(chat_id, context, chat):
    report_type = context.user_data['selected_report_type']
    config = context.user_data.get('auto_report_config', {})

    full_config = {
        'enabled': True,
        'schedule': config['schedule']
    }

    reports = load_auto_reports()
    set_user_report_config(reports, str(chat_id), report_type, full_config)
    save_auto_reports(reports)

    # Удаляем старые задачи для этого типа
    current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat_id}_{report_type}")
    for job in current_jobs:
        job.schedule_removal()

    # Запускаем новую
    from handlers.all_mp_remains_handler import send_all_mp_remains_automatic
    callback = send_all_mp_remains_automatic  # пока только один тип

    schedule_job(context.application, callback, full_config, {'chat_id': chat_id, 'report_type': report_type}, chat_id, report_type)

    # Формируем описание
    sched = config['schedule']
    if sched['type'] == 'interval_hours':
        details = f"Каждые {sched['hours']} ч, начиная с {sched['start_time']}"
    elif sched['type'] == 'interval_days':
        if 'day_of_week' in sched:
            day_name = DAYS_OF_WEEK[sched['day_of_week']]
            details = f"Каждый {day_name} в {sched['time']}"
        else:
            start_day_name = DAYS_OF_WEEK.get(sched['start_day'], "Неизвестный день")
            details = f"Каждые {sched['days']} дн, начиная с {start_day_name} в {sched['time']}"

    await chat.send_message(
        f"✅ Автоотчёт «{context.user_data['selected_report_label']}» настроен!\n\n"
        f"Интервал: {details}\n\n"
        f"Первый отчёт придёт по расписанию.",
        reply_markup=ReplyKeyboardRemove()
    )
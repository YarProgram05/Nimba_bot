# handlers/auto_report_handler.py

import logging
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler
from zoneinfo import ZoneInfo

from utils.auto_report_manager import load_auto_reports, save_auto_reports, schedule_job

logger = logging.getLogger(__name__)

# Состояния
from states import AUTO_REPORT_TOGGLE, AUTO_REPORT_FREQUENCY, AUTO_REPORT_TIME, AUTO_REPORT_DAY

# Единственная callback-функция
from handlers.all_mp_remains_handler import send_all_mp_remains_automatic

# Константы
INTERVAL_TYPE_OPTIONS = {
    "hours": "🕗 По часам",
    "days": "📅 По дням"
}

HOUR_OPTIONS = ["1", "2", "3", "4", "5", "6", "12", "24"]
DAY_OPTIONS = ["1", "2", "3", "4", "5", "6", "7"]


async def start_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало настройки автоотчётов"""
    logger.info("✅ start_auto_report вызван!")

    reports = load_auto_reports()
    chat_id = str(update.effective_chat.id)
    user_config = reports.get(chat_id, {})
    enabled = user_config.get('enabled', False)

    status = "включены" if enabled else "выключены"
    reply_markup = ReplyKeyboardMarkup(
        [["✅ Включить", "❌ Выключить"]],
        one_time_keyboard=True,
        resize_keyboard=True
    )
    await update.message.reply_text(
        f"Автоотчёты по всем маркетплейсам сейчас {status}.\n\nВыберите действие:",
        reply_markup=reply_markup
    )
    return AUTO_REPORT_TOGGLE


async def handle_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка включения/выключения"""
    text = update.message.text
    chat_id = update.effective_chat.id
    logger.info(f"🔍 handle_toggle: получено (repr): {repr(text)} от chat_id={chat_id}")

    text_clean = text.strip()
    if text_clean == "❌ Выключить":
        reports = load_auto_reports()
        chat_id_str = str(chat_id)
        if chat_id_str in reports:
            reports[chat_id_str]['enabled'] = False
            save_auto_reports(reports)
            current_jobs = context.job_queue.get_jobs_by_name(f"auto_report_{chat_id}")
            for job in current_jobs:
                job.schedule_removal()
        await update.message.reply_text(
            "✅ Автоотчёты отключены.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    elif text_clean == "✅ Включить":
        reply_markup = ReplyKeyboardMarkup(
            [[INTERVAL_TYPE_OPTIONS["hours"], INTERVAL_TYPE_OPTIONS["days"]]],
            one_time_keyboard=True,
            resize_keyboard=True
        )
        await update.message.reply_text(
            "Выберите тип интервала отправки:",
            reply_markup=reply_markup
        )
        return AUTO_REPORT_FREQUENCY

    else:
        logger.warning(f"⚠️ Неизвестный текст в AUTO_REPORT_TOGGLE: {repr(text)}")
        reply_markup = ReplyKeyboardMarkup(
            [["✅ Включить", "❌ Выключить"]],
            one_time_keyboard=True,
            resize_keyboard=True
        )
        await update.message.reply_text(
            "Пожалуйста, используйте кнопки:",
            reply_markup=reply_markup
        )
        return AUTO_REPORT_TOGGLE


async def handle_interval_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор: по часам или по дням"""
    text = update.message.text
    logger.info(f"🔍 handle_interval_type: {repr(text)}")

    if text == INTERVAL_TYPE_OPTIONS["hours"]:
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_hours'}}
        buttons = [HOUR_OPTIONS[i:i + 4] for i in range(0, len(HOUR_OPTIONS), 4)]
        reply_markup = ReplyKeyboardMarkup(buttons, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Выберите интервал в часах:", reply_markup=reply_markup)
        return AUTO_REPORT_TIME

    elif text == INTERVAL_TYPE_OPTIONS["days"]:
        context.user_data['auto_report_config'] = {'schedule': {'type': 'interval_days'}}
        buttons = [DAY_OPTIONS[i:i + 4] for i in range(0, len(DAY_OPTIONS), 4)]
        reply_markup = ReplyKeyboardMarkup(buttons, one_time_keyboard=True, resize_keyboard=True)
        await update.message.reply_text("Выберите интервал в днях:", reply_markup=reply_markup)
        return AUTO_REPORT_TIME

    else:
        reply_markup = ReplyKeyboardMarkup(
            [[INTERVAL_TYPE_OPTIONS["hours"], INTERVAL_TYPE_OPTIONS["days"]]],
            one_time_keyboard=True,
            resize_keyboard=True
        )
        await update.message.reply_text("Пожалуйста, выберите тип интервала:", reply_markup=reply_markup)
        return AUTO_REPORT_FREQUENCY


async def handle_time_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора числа (часов или дней)"""
    text = update.message.text.strip()
    logger.info(f"🔢 Введено число: {repr(text)}")

    config = context.user_data.get('auto_report_config', {})
    sched_type = config['schedule']['type']

    if sched_type == 'interval_hours':
        if text in HOUR_OPTIONS:
            config['schedule']['hours'] = int(text)
            await finalize_auto_report(update, context)
            return ConversationHandler.END
        else:
            buttons = [HOUR_OPTIONS[i:i + 4] for i in range(0, len(HOUR_OPTIONS), 4)]
            reply_markup = ReplyKeyboardMarkup(buttons, one_time_keyboard=True, resize_keyboard=True)
            await update.message.reply_text("Выберите интервал из кнопок:", reply_markup=reply_markup)
            return AUTO_REPORT_TIME

    elif sched_type == 'interval_days':
        if text in DAY_OPTIONS:
            config['schedule']['days'] = int(text)
            await update.message.reply_text(
                "Введите время отправки в формате ЧЧ:ММ (например, 10:00):",
                reply_markup=ReplyKeyboardRemove()
            )
            return AUTO_REPORT_DAY
        else:
            buttons = [DAY_OPTIONS[i:i + 4] for i in range(0, len(DAY_OPTIONS), 4)]
            reply_markup = ReplyKeyboardMarkup(buttons, one_time_keyboard=True, resize_keyboard=True)
            await update.message.reply_text("Выберите интервал из кнопок:", reply_markup=reply_markup)
            return AUTO_REPORT_TIME

    return ConversationHandler.END


async def handle_daily_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ввод времени для интервала в днях"""
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
        return AUTO_REPORT_DAY

    context.user_data['auto_report_config']['schedule']['time'] = f"{hour:02d}:{minute:02d}"
    await finalize_auto_report(update, context)
    return ConversationHandler.END


async def finalize_auto_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохранение и запуск задачи"""
    chat_id = update.effective_chat.id
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
    else:  # interval_days
        details = f"Каждые {sched['days']} дн в {sched['time']}"

    await update.message.reply_text(
        f"✅ Автоотчёт по всем маркетплейсам настроен!\n\n"
        f"Интервал: {details}\n\n"
        f"Первый отчёт придёт по расписанию.",
        reply_markup=ReplyKeyboardRemove()
    )
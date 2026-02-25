# utils/auto_report_manager.py
import os
import json
import logging
from datetime import datetime, time as dtime, timedelta
from telegram.ext import Application, CallbackContext
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "data"))
AUTO_REPORTS_FILE = os.path.join(DATA_DIR, "auto_reports.json")


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def get_next_weekday_at_time(target_weekday, target_time, tz):
    now = datetime.now(tz)
    current_weekday = now.weekday()
    days_ahead = (target_weekday - current_weekday) % 7
    next_date = now.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0
    ) + timedelta(days=days_ahead)
    if days_ahead == 0 and now.time() > target_time:
        next_date += timedelta(weeks=1)
    return next_date


def get_next_interval_day(start_day, interval_days, target_time, tz):
    now = datetime.now(tz)

    # Понедельник текущей недели
    monday_this_week = now - timedelta(days=now.weekday())

    # Дата дня отсчёта в текущей неделе
    start_date_this_week = monday_this_week.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0
    ) + timedelta(days=start_day)

    # Ищем первую дату >= сейчас
    k = 0
    while k < 100:
        candidate = start_date_this_week + timedelta(days=k * interval_days)
        if candidate > now:
            return candidate
        k += 1

    # Fallback
    return now.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=0,
        microsecond=0
    ) + timedelta(days=interval_days)


# utils/auto_report_manager.py

def load_auto_reports():
    _ensure_data_dir()
    if os.path.exists(AUTO_REPORTS_FILE):
        try:
            with open(AUTO_REPORTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки auto_reports.json: {e}")
            return {}
    return {}


def save_auto_reports(reports):
    _ensure_data_dir()
    try:
        with open(AUTO_REPORTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения auto_reports.json: {e}")


def get_user_report_config(reports, chat_id_str, report_type):
    """Возвращает конфиг для конкретного типа отчёта у пользователя."""
    return reports.get(chat_id_str, {}).get(report_type, {})


def set_user_report_config(reports, chat_id_str, report_type, config):
    """Устанавливает конфиг для конкретного типа отчёта."""
    if chat_id_str not in reports:
        reports[chat_id_str] = {}
    reports[chat_id_str][report_type] = config


# Обновлённая schedule_all_jobs
def schedule_all_jobs(application: Application):
    from handlers.all_mp_remains_handler import send_all_mp_remains_automatic

    REPORT_TYPE_TO_CALLBACK = {
        'all_mp_remains': send_all_mp_remains_automatic
    }

    reports = load_auto_reports()
    for chat_id_str, user_configs in reports.items():
        if not isinstance(user_configs, dict):
            continue

        chat_id = int(chat_id_str)

        for report_type, config in user_configs.items():
            if not config.get('enabled'):
                continue

            callback = REPORT_TYPE_TO_CALLBACK.get(report_type)
            if not callback:
                continue

            job_data = {'chat_id': chat_id, 'report_type': report_type}
            current_jobs = application.job_queue.get_jobs_by_name(f"auto_report_{chat_id}_{report_type}")
            for job in current_jobs:
                job.schedule_removal()

            schedule_job(application, callback, config, job_data, chat_id, report_type)


def schedule_job(application, callback, config, job_data, chat_id, report_type):
    moscow_tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(moscow_tz)

    schedule = config['schedule']
    sched_type = schedule['type']

    job_name = f"auto_report_{chat_id}_{report_type}"

    if sched_type == "interval_hours":
        hours = schedule['hours']
        start_time_str = schedule.get('start_time', "00:00")
        start_time = dtime.fromisoformat(start_time_str)

        base_dt = now.replace(
            hour=start_time.hour,
            minute=start_time.minute,
            second=0,
            microsecond=0
        )
        if now.time() > start_time:
            base_dt -= timedelta(days=1)

        next_run = base_dt
        while next_run <= now:
            next_run += timedelta(hours=hours)

        first_run = (next_run - now).total_seconds()
        interval_sec = hours * 3600

        application.job_queue.run_repeating(
            callback=callback,
            interval=interval_sec,
            first=first_run,
            data=job_data,
            name=job_name
        )
        logger.info(f"Запланирован часовой автоотчёт {report_type} для {chat_id}: {next_run}")

    elif sched_type == "interval_days":
        days = schedule['days']
        time_str = schedule['time']
        target_time = dtime.fromisoformat(time_str)

        if days == 7 and 'day_of_week' in schedule:
            target_weekday = schedule['day_of_week']
            next_run = get_next_weekday_at_time(target_weekday, target_time, moscow_tz)
        else:
            start_day = schedule.get('start_day', 0)
            next_run = get_next_interval_day(start_day, days, target_time, moscow_tz)

        first_run = (next_run - now).total_seconds()
        interval_sec = days * 24 * 3600

        application.job_queue.run_repeating(
            callback=callback,
            interval=interval_sec,
            first=first_run,
            data=job_data,
            name=job_name
        )
        logger.info(f"Запланирован дневной автоотчёт {report_type} для {chat_id}: {next_run}")
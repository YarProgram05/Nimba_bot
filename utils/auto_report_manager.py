# utils/auto_report_manager.py
import os
import json
import logging
from datetime import datetime, time as dtime, timedelta
from telegram.ext import Application, CallbackContext
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

AUTO_REPORTS_FILE = "auto_reports.json"


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


def load_auto_reports():
    if os.path.exists(AUTO_REPORTS_FILE):
        try:
            with open(AUTO_REPORTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки auto_reports.json: {e}")
            return {}
    return {}


def save_auto_reports(reports):
    try:
        with open(AUTO_REPORTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения auto_reports.json: {e}")


def schedule_all_jobs(application: Application):
    from handlers.all_mp_remains_handler import send_all_mp_remains_automatic

    reports = load_auto_reports()
    for chat_id_str, config in reports.items():
        if not config.get('enabled'):
            continue

        chat_id = int(chat_id_str)
        job_data = {'chat_id': chat_id}

        report_type = config['report_type']
        if report_type == 'all_mp':
            callback = send_all_mp_remains_automatic
        else:
            continue

        current_jobs = application.job_queue.get_jobs_by_name(f"auto_report_{chat_id}")
        for job in current_jobs:
            job.schedule_removal()

        schedule_job(application, callback, config, job_data, chat_id)


def schedule_job(application, callback, config, job_data, chat_id):
    moscow_tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(moscow_tz)

    schedule = config['schedule']
    sched_type = schedule['type']

    if sched_type == "interval_hours":
        hours = schedule['hours']
        start_time_str = schedule.get('start_time', "00:00")
        start_time = dtime.fromisoformat(start_time_str)

        first_run_dt = now.replace(
            hour=start_time.hour,
            minute=start_time.minute,
            second=0,
            microsecond=0
        )
        if now > first_run_dt:
            first_run_dt += timedelta(days=1)
        first_run = (first_run_dt - now).total_seconds()

        interval_sec = hours * 3600
        application.job_queue.run_repeating(
            callback=callback,
            interval=interval_sec,
            first=first_run,
            data=job_data,
            name=f"auto_report_{chat_id}"
        )

    elif sched_type == "interval_days":
        days = schedule['days']
        time_str = schedule['time']
        target_time = dtime.fromisoformat(time_str)

        if days == 7 and 'day_of_week' in schedule:
            target_weekday = schedule['day_of_week']
            next_run = get_next_weekday_at_time(target_weekday, target_time, moscow_tz)
            first_run = (next_run - now).total_seconds()
            interval_sec = 7 * 24 * 3600

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
            name=f"auto_report_{chat_id}"
        )
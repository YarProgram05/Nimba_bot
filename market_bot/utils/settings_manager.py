import json
import os
import logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "data"))
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def load_settings():
    _ensure_data_dir()
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки settings.json: {e}")
            return {}
    return {}


def save_settings(settings):
    _ensure_data_dir()
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения settings.json: {e}")


def get_stock_thresholds(chat_id):
    settings = load_settings()
    return settings.get(str(chat_id), {}).get("stock_thresholds")


def set_stock_thresholds(chat_id, red_level, yellow_level):
    settings = load_settings()
    chat_key = str(chat_id)
    if chat_key not in settings:
        settings[chat_key] = {}
    settings[chat_key]["stock_thresholds"] = {
        "red": red_level,
        "yellow": yellow_level
    }
    save_settings(settings)

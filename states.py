# states.py — состояния диалогов Telegram-бота

# ───────────────────────────────
# 🏠 Главное меню
# ───────────────────────────────
SELECTING_ACTION = 0


# ───────────────────────────────
# 🛒 Ozon — продажи
# ───────────────────────────────
OZON_SALES_CABINET_CHOICE = 1
OZON_SALES_DATE_START    = 2
OZON_SALES_DATE_END      = 3


# ───────────────────────────────
# 🛒 Wildberries — продажи
# ───────────────────────────────
WB_SALES_CABINET_CHOICE = 4
WB_SALES_DATE_START     = 5
WB_SALES_DATE_END       = 6


# ───────────────────────────────
# 📦 Остатки товаров
# ───────────────────────────────
OZON_REMAINS_CABINET_CHOICE = 7
WB_REMAINS_CABINET_CHOICE   = 8
ALL_MP_REMAINS              = 9


# ───────────────────────────────
# 📎 Ручные отчёты (файлы)
# ───────────────────────────────
BARCODE_FILES            = 10
CSV_FILES                = 11


# ───────────────────────────────
# ⏱️ Автоотчёты
# ───────────────────────────────
SELECTING_AUTO_REPORT_TYPE = 12
AUTO_REPORT_TOGGLE         = 13
AUTO_REPORT_FREQUENCY      = 14
AUTO_REPORT_TIME           = 15  # выбор интервала (в днях/часах)
AUTO_REPORT_WEEKLY_DAY     = 16  # выбор дня недели (callback)
AUTO_REPORT_START_DAY      = 17  # выбор начального дня (для еженедельных)
AUTO_REPORT_DAILY_TIME     = 18  # ввод времени выполнения (текст)
AUTO_REPORT_START_TIME     = 19  # ввод времени начала (текст)


# ───────────────────────────────
# 🏷️ Типы автоотчётов
# ───────────────────────────────
AUTO_REPORT_TYPE_ALL_MP_REMAINS = "all_mp_remains"

AUTO_REPORT_TYPE_LABELS = {
    AUTO_REPORT_TYPE_ALL_MP_REMAINS: "Остатки на всех МП"
}
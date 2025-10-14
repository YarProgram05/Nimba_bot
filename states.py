# Состояния разговоров
SELECTING_ACTION = 0

# Wildberries
WB_REPORT_FILES = 1
WB_REMAINS_FILES = 2
WB_REMAINS_CABINET_CHOICE = 3
WB_REPORT_CABINET_CHOICE = 11  # ← странно, но допустимо

# Ozon — остатки
OZON_REMAINS_CABINET_CHOICE = 4

# Штрихкоды и CSV
BARCODE_FILES = 5
CSV_FILES = 6

# Ozon — продажи (новое)
OZON_SALES_CABINET_CHOICE = 7
OZON_SALES_DATE_START = 8
OZON_SALES_DATE_END = 9

ALL_MP_REMAINS = 10

# Для автоотчётов
AUTO_REPORT_TOGGLE = 12
AUTO_REPORT_FREQUENCY = 13
AUTO_REPORT_TIME = 14           # выбор числа (часов/дней)
AUTO_REPORT_WEEKLY_DAY = 15     # выбор дня недели (ТОЛЬКО callback)
AUTO_REPORT_DAILY_TIME = 16     # ввод времени (для 1-6 дней и после выбора дня)
AUTO_REPORT_START_TIME = 17
AUTO_REPORT_START_DAY = 18
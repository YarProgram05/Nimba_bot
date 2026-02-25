from telegram import ReplyKeyboardMarkup


def get_main_menu():
    return ReplyKeyboardMarkup(
        [
            ["Продажи Ozon", "Продажи WB"],
            ["Остатки товаров Ozon", "Остатки товаров WB"],
            ["Остатки на всех МП"],
            ["Автоотчёты"],
            ["Генерация штрихкодов"],
            ["Конвертация CSV в XLSX"],
            ["Формирование ТЗ"],
            ["Настройки"],
            ["Помощь"]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

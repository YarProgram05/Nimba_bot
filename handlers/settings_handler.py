import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext, ConversationHandler

from states import SELECTING_ACTION, SETTINGS_MENU, SETTINGS_STOCK_RED, SETTINGS_STOCK_YELLOW
from utils.settings_manager import set_stock_thresholds, get_stock_thresholds
from utils.menu import get_main_menu

logger = logging.getLogger(__name__)


async def _edit_or_reply(query, text: str, reply_markup=None) -> None:
    """Предпочтительно обновляет текущее сообщение (edit_text), чтобы не плодить сообщения."""
    try:
        if query and query.message:
            await query.message.edit_text(text, reply_markup=reply_markup)
            return
    except Exception as e:
        logger.debug(f"settings: edit_text failed, fallback to reply_text: {e}")

    # fallback
    if query and query.message:
        await query.message.reply_text(text, reply_markup=reply_markup)


def _is_positive_int(text):
    try:
        value = int(str(text).strip())
        return value >= 0, value
    except (ValueError, TypeError):
        return False, None


def _format_thresholds(thresholds):
    if not thresholds:
        return "Текущие настройки: не заданы"
    red = thresholds.get("red")
    yellow = thresholds.get("yellow")
    if red is None or yellow is None:
        return "Текущие настройки: не заданы"
    return f"Текущие настройки:\nКрасный: {red}, Желтый: {yellow}"


async def _show_settings_menu(message=None, *, query=None) -> int:
    keyboard = [
        [InlineKeyboardButton("Контроль остатков", callback_data="settings_stock_control")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="settings_back")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    text = "⚙️ Настройки\n\nВыберите раздел:"

    if query is not None:
        await _edit_or_reply(query, text, reply_markup=reply_markup)
    else:
        await message.reply_text(text, reply_markup=reply_markup)
    return SETTINGS_MENU


async def _show_stock_control_menu(message=None, chat_id=None, *, query=None) -> int:
    thresholds = get_stock_thresholds(chat_id)
    text = _format_thresholds(thresholds)
    keyboard = [
        [InlineKeyboardButton("Изменить", callback_data="settings_stock_edit")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="settings_stock_back")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    full_text = f"{text}\n\nВыберите действие:"

    if query is not None:
        await _edit_or_reply(query, full_text, reply_markup=reply_markup)
    else:
        await message.reply_text(full_text, reply_markup=reply_markup)
    return SETTINGS_MENU


async def start_settings(update: Update, context: CallbackContext) -> int:
    return await _show_settings_menu(update.message)


async def handle_settings_choice(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "settings_back":
        # Назад в главное меню: только обновляем текущее сообщение, не отправляя новое
        await _edit_or_reply(query, "⬅️ Назад в главное меню.", reply_markup=None)
        return SELECTING_ACTION

    if query.data == "settings_stock_back":
        return await _show_settings_menu(query=query)

    if query.data == "settings_stock_control":
        return await _show_stock_control_menu(chat_id=update.effective_chat.id, query=query)

    if query.data == "settings_stock_edit":
        await _edit_or_reply(
            query,
            "Введите критический уровень остатков (красный).\n"
            "Например: 5",
            reply_markup=None,
        )
        return SETTINGS_STOCK_RED

    await _edit_or_reply(query, "⚠️ Неизвестная настройка.", reply_markup=None)
    return ConversationHandler.END


async def handle_stock_red_input(update: Update, context: CallbackContext) -> int:
    ok, value = _is_positive_int(update.message.text)
    if not ok:
        await update.message.reply_text("Введите целое число 0 или больше.")
        return SETTINGS_STOCK_RED

    context.user_data["stock_thresholds_pending"] = {"red": value}
    await update.message.reply_text(
        "Теперь введите предупредительный уровень (желтый).\n"
        "Он должен быть больше или равен красному уровню."
    )
    return SETTINGS_STOCK_YELLOW


async def handle_stock_yellow_input(update: Update, context: CallbackContext) -> int:
    ok, yellow_value = _is_positive_int(update.message.text)
    if not ok:
        await update.message.reply_text("Введите целое число 0 или больше.")
        return SETTINGS_STOCK_YELLOW

    pending = context.user_data.get("stock_thresholds_pending", {})
    red_value = pending.get("red")
    if red_value is None:
        await update.message.reply_text("Не удалось найти красный порог. Начните настройку заново.")
        return ConversationHandler.END

    if yellow_value < red_value:
        await update.message.reply_text("Желтый порог должен быть больше или равен красному. Попробуйте снова.")
        return SETTINGS_STOCK_YELLOW

    chat_id = update.effective_chat.id
    thresholds = {"red": red_value, "yellow": yellow_value}
    set_stock_thresholds(chat_id, red_value, yellow_value)
    context.user_data["stock_thresholds"] = thresholds
    context.user_data.pop("stock_thresholds_pending", None)

    await update.message.reply_text(
        f"✅ Настройки сохранены.\nКрасный: {red_value}, Желтый: {yellow_value}",
        reply_markup=get_main_menu()
    )
    return SELECTING_ACTION

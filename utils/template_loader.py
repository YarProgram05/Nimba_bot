# utils/template_loader.py

import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

# Флаг для переключения между Excel и базой данных
USE_DATABASE = True

try:
    from utils.database import get_database
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    get_database = None  # Определяем как None для избежания ошибок
    logger.warning("Модуль database недоступен, будет использоваться Excel")


def load_template(sheet_name):
    """
    Загружает шаблон из Excel файла.
    Возвращает:
      - art_to_id: { артикул: template_id }
      - id_to_name: { template_id: название }
      - main_ids_ordered: [список template_id в порядке появления]

    Используется для обработчиков WB.
    """
    try:
        template_path = "База данных артикулов для выкупов и начислений.xlsx"
        if not os.path.exists(template_path):
            template_path = os.path.join(os.path.dirname(__file__), "..",
                                         "База данных артикулов для выкупов и начислений.xlsx")

        df = pd.read_excel(template_path, sheet_name=sheet_name)

        # Создаем словари для соответствий
        art_to_id = {}
        id_to_name = {}
        main_ids_ordered = []

        # Обработка основных строк с ID и Articles
        for _, row in df.iterrows():
            if not pd.isna(row.get('ID')):
                id_val = int(row['ID'])
                article_name = row['Articles']
                id_to_name[id_val] = article_name
                main_ids_ordered.append(id_val)

                # Добавляем основное название
                art_str = str(article_name).strip().lower()
                art_to_id[art_str] = id_val

        # Обработка строк с Articles_cabinet (для WB)
        for _, row in df.iterrows():
            if not pd.isna(row.get('ID_mix')) and not pd.isna(row.get('Articles_cabinet')):
                id_mix_val = int(row['ID_mix'])
                cabinet_art = str(row['Articles_cabinet']).strip().lower()

                # Связываем с основным ID
                art_to_id[cabinet_art] = id_mix_val

                # Если ID_mix нет в основных, добавляем
                if id_mix_val not in id_to_name:
                    id_to_name[id_mix_val] = f"ID {id_mix_val}"
                    main_ids_ordered.append(id_mix_val)

        return art_to_id, id_to_name, main_ids_ordered

    except Exception as e:
        logger.error(f"Ошибка при загрузке шаблона {sheet_name}: {e}", exc_info=True)
        return {}, {}, []


def get_cabinet_articles_by_template_id(sheet_name):
    """
    Возвращает:
      - template_id_to_name: { template_id: "Шаблонное название" }
      - template_id_to_cabinet_arts: { template_id: [real_art1, real_art2, ...] }

    Логика:
      - Все строки с ID → определяют шаблонные артикулы.
      - Все строки с ID_mix → привязывают Articles_cabinet к template_id = ID_mix,
        даже если нет отдельной строки с ID = ID_mix (но тогда название = "ID {X}")

    Если USE_DATABASE=True и база данных доступна, данные берутся из SQLite.
    Иначе используется Excel файл напрямую.
    """
    # Если включено использование базы данных и она доступна
    if USE_DATABASE and DATABASE_AVAILABLE:
        try:
            db = get_database()
            return db.get_cabinet_articles_by_template_id(sheet_name)
        except Exception as e:
            logger.error(f"Ошибка при получении данных из БД: {e}", exc_info=True)
            logger.warning("Переключаемся на чтение из Excel")
            # Продолжаем выполнение, чтобы попытаться прочитать из Excel

    # Чтение напрямую из Excel (запасной вариант или если БД отключена)
    try:
        template_path = "База данных артикулов для выкупов и начислений.xlsx"
        if not os.path.exists(template_path):
            template_path = os.path.join(os.path.dirname(__file__), "..",
                                         "База данных артикулов для выкупов и начислений.xlsx")

        df = pd.read_excel(template_path, sheet_name=sheet_name)

        template_id_to_name = {}
        template_id_to_cabinet_arts = {}

        # Шаг 1: собрать все ID → Articles
        for _, row in df.iterrows():
            if not pd.isna(row.get('ID')):
                template_id = int(row['ID'])
                article_name = str(row['Articles']).strip()
                template_id_to_name[template_id] = article_name

        # Шаг 2: собрать все ID_mix → Articles_cabinet
        for _, row in df.iterrows():
            id_mix = None
            cabinet_art = None

            # Используем ID_mix, если есть
            if not pd.isna(row.get('ID_mix')):
                id_mix = int(row['ID_mix'])

            # ИЛИ, если нет ID_mix, но есть ID и Articles_cabinet — используем ID
            elif not pd.isna(row.get('ID')) and not pd.isna(row.get('Articles_cabinet')):
                id_mix = int(row['ID'])

            if id_mix is not None:
                cabinet_art = str(row.get('Articles_cabinet', '')).strip()

            if id_mix is not None and cabinet_art:
                # Гарантируем, что template_id_to_name содержит запись
                if id_mix not in template_id_to_name:
                    template_id_to_name[id_mix] = f"ID {id_mix}"

                template_id_to_cabinet_arts.setdefault(id_mix, []).append(cabinet_art)

        return template_id_to_name, template_id_to_cabinet_arts

    except Exception as e:
        logger.error(f"Ошибка в get_cabinet_articles_by_template_id для листа {sheet_name}: {e}")
        return {}, {}


def get_template_order(sheet_name):
    """
    Возвращает список template_id в порядке следования строк в Excel.
    Используется для сохранения порядка "как в базе" в отчетах.
    """
    try:
        template_path = "База данных артикулов для выкупов и начислений.xlsx"
        if not os.path.exists(template_path):
            template_path = os.path.join(os.path.dirname(__file__), "..",
                                         "База данных артикулов для выкупов и начислений.xlsx")

        df = pd.read_excel(template_path, sheet_name=sheet_name)
        main_ids_ordered = []
        seen = set()

        for _, row in df.iterrows():
            if not pd.isna(row.get('ID')):
                tid = int(row['ID'])
                if tid not in seen:
                    main_ids_ordered.append(tid)
                    seen.add(tid)
            elif not pd.isna(row.get('ID_mix')):
                tid_mix = int(row['ID_mix'])
                if tid_mix not in seen:
                    main_ids_ordered.append(tid_mix)
                    seen.add(tid_mix)

        return main_ids_ordered
    except Exception as e:
        logger.error(f"Ошибка при получении порядка ID для листа {sheet_name}: {e}")
        return []

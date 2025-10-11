# utils/template_loader.py

import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


def get_cabinet_articles_by_template_id(sheet_name):
    """
    Возвращает:
      - template_id_to_name: { template_id: "Шаблонное название" }
      - template_id_to_cabinet_arts: { template_id: [real_art1, real_art2, ...] }

    Логика:
      - Все строки с ID → определяют шаблонные артикулы.
      - Все строки с ID_mix → привязывают Articles_cabinet к template_id = ID_mix,
        даже если нет отдельной строки с ID = ID_mix (но тогда название = "ID {X}")
    """
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
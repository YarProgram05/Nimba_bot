import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


def load_template(sheet_name):
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

        # Обработка строк с Mixed_Articles
        for _, row in df.iterrows():
            if not pd.isna(row.get('ID_mix')) and not pd.isna(row.get('Mixed_Articles')):
                id_mix_val = int(row['ID_mix'])
                mixed_art = str(row['Mixed_Articles']).strip().lower()

                # Связываем с основным ID
                art_to_id[mixed_art] = id_mix_val

                # Если ID_mix нет в основных, добавляем
                if id_mix_val not in id_to_name:
                    id_to_name[id_mix_val] = mixed_art
                    main_ids_ordered.append(id_mix_val)

        return art_to_id, id_to_name, main_ids_ordered

    except Exception as e:
        logger.error(f"Ошибка при загрузке шаблона: {e}")
        return {}, {}, []


def get_article_mapping(sheet_name):
    """Получаем отображение артикулов из шаблона"""
    try:
        template_path = "База данных артикулов для выкупов и начислений.xlsx"
        if not os.path.exists(template_path):
            template_path = os.path.join(os.path.dirname(__file__), "..",
                                         "База данных артикулов для выкупов и начислений.xlsx")

        df = pd.read_excel(template_path, sheet_name=sheet_name)

        mapping = {}

        # Обработка строк
        for _, row in df.iterrows():
            if not pd.isna(row.get('ID')) and not pd.isna(row.get('Articles')):
                article_name = str(row['Articles']).strip().lower()
                mapping[article_name] = int(row['ID'])

            if not pd.isna(row.get('ID_mix')) and not pd.isna(row.get('Mixed_Articles')):
                mixed_art = str(row['Mixed_Articles']).strip().lower()
                mapping[mixed_art] = int(row['ID_mix'])

        return mapping
    except Exception as e:
        logger.error(f"Ошибка при загрузке маппинга артикулов: {e}")
        return {}
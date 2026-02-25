# utils/database.py
"""
Модуль для работы с SQLite базой данных артикулов.
Предоставляет функции для синхронизации с Excel и получения данных.
"""

import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class ArticleDatabase:
    """Класс для работы с базой данных артикулов"""

    def __init__(self, db_path: str = None):
        """
        Инициализация базы данных

        Args:
            db_path: Путь к файлу базы данных SQLite.
                     Если None, используется путь по умолчанию.
        """
        if db_path is None:
            # Путь к базе данных в корне проекта market_bot
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(os.path.dirname(current_dir), 'articles_database.db')

        self.db_path = db_path
        self.excel_path = self._get_excel_path()
        self._init_database()

    def _get_excel_path(self) -> str:
        """Получить путь к Excel файлу"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(
            os.path.dirname(current_dir),
            "База данных артикулов для выкупов и начислений.xlsx"
        )
        return excel_path

    def _init_database(self):
        """Инициализация структуры базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Таблица для артикулов (основная)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER NOT NULL,
                article_name TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                PRIMARY KEY (id, sheet_name)
            )
        ''')

        # Таблица для маппинга артикулов кабинетов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS article_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                cabinet_article TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                UNIQUE(template_id, cabinet_article, sheet_name)
            )
        ''')

        # Таблица для отслеживания последней синхронизации
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                excel_modified_date TIMESTAMP,
                records_synced INTEGER,
                status TEXT
            )
        ''')

        # Индексы для ускорения поиска
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_article_sheet 
            ON articles(sheet_name, id)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_template 
            ON article_mappings(template_id, sheet_name)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mapping_cabinet 
            ON article_mappings(cabinet_article, sheet_name)
        ''')

        conn.commit()
        conn.close()
        logger.info(f"База данных инициализирована: {self.db_path}")

    def needs_sync(self) -> bool:
        """
        Проверяет, нужна ли синхронизация с Excel

        Returns:
            True, если нужна синхронизация
        """
        if not os.path.exists(self.excel_path):
            logger.warning(f"Excel файл не найден: {self.excel_path}")
            return False

        # Проверяем дату модификации Excel файла
        excel_modified = datetime.fromtimestamp(os.path.getmtime(self.excel_path))

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT excel_modified_date, sync_date 
            FROM sync_log 
            WHERE status = 'success'
            ORDER BY sync_date DESC 
            LIMIT 1
        ''')

        result = cursor.fetchone()
        conn.close()

        if result is None:
            # Первая синхронизация
            logger.info("База данных пустая, требуется синхронизация")
            return True

        last_sync_excel_date = datetime.fromisoformat(result[0])

        if excel_modified > last_sync_excel_date:
            logger.info(
                f"Excel изменен: {excel_modified} > {last_sync_excel_date}, "
                "требуется синхронизация"
            )
            return True

        return False

    def sync_from_excel(self, force: bool = False) -> bool:
        """
        Синхронизирует базу данных с Excel файлом

        Args:
            force: Принудительная синхронизация даже если файл не изменился

        Returns:
            True если синхронизация прошла успешно
        """
        if not force and not self.needs_sync():
            logger.info("Синхронизация не требуется")
            return True

        if not os.path.exists(self.excel_path):
            logger.error(f"Excel файл не найден: {self.excel_path}")
            return False

        try:
            logger.info(f"Начинаем синхронизацию с {self.excel_path}")
            excel_modified = datetime.fromtimestamp(os.path.getmtime(self.excel_path))

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Очищаем старые данные
            cursor.execute('DELETE FROM article_mappings')
            cursor.execute('DELETE FROM articles')

            total_records = 0

            # Читаем все листы из Excel
            xls = pd.ExcelFile(self.excel_path)

            for sheet_name in xls.sheet_names:
                df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
                logger.info(f"Обработка листа: {sheet_name}, строк: {len(df)}")

                # Определяем имя колонки для кабинетных артикулов
                cabinet_col = 'Articles_cabinet' if 'Отдельно' in sheet_name else 'Mixed_Articles'

                # Собираем все уникальные ID которые встречаются
                all_template_ids = set()

                # Сначала загружаем все уникальные ID и их названия
                for _, row in df.iterrows():
                    if pd.notna(row.get('ID')):
                        article_id = int(row['ID'])
                        article_name = str(row['Articles']).strip()
                        all_template_ids.add(article_id)

                        cursor.execute('''
                            INSERT OR REPLACE INTO articles (id, article_name, sheet_name)
                            VALUES (?, ?, ?)
                        ''', (article_id, article_name, sheet_name))
                        total_records += 1

                # Затем загружаем маппинги
                for _, row in df.iterrows():
                    template_id = None
                    cabinet_article = None

                    # Если есть ID_mix, используем его
                    if pd.notna(row.get('ID_mix')):
                        template_id = int(row['ID_mix'])
                    # Иначе если есть ID и кабинетный артикул, используем ID
                    elif pd.notna(row.get('ID')) and pd.notna(row.get(cabinet_col)):
                        template_id = int(row['ID'])

                    if template_id is not None and pd.notna(row.get(cabinet_col)):
                        cabinet_article = str(row[cabinet_col]).strip()

                        if cabinet_article:
                            # Если template_id еще не добавлен в articles, добавляем с именем "ID X"
                            if template_id not in all_template_ids:
                                all_template_ids.add(template_id)
                                cursor.execute('''
                                    INSERT OR REPLACE INTO articles (id, article_name, sheet_name)
                                    VALUES (?, ?, ?)
                                ''', (template_id, f"ID {template_id}", sheet_name))
                                total_records += 1

                            cursor.execute('''
                                INSERT OR REPLACE INTO article_mappings 
                                (template_id, cabinet_article, sheet_name)
                                VALUES (?, ?, ?)
                            ''', (template_id, cabinet_article, sheet_name))
                            total_records += 1

            # Записываем лог синхронизации
            cursor.execute('''
                INSERT INTO sync_log (excel_modified_date, records_synced, status)
                VALUES (?, ?, ?)
            ''', (excel_modified.isoformat(), total_records, 'success'))

            conn.commit()
            conn.close()

            logger.info(
                f"Синхронизация успешно завершена. "
                f"Загружено {total_records} записей"
            )
            return True

        except Exception as e:
            logger.error(f"Ошибка при синхронизации: {e}", exc_info=True)

            # Записываем ошибку в лог
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO sync_log (excel_modified_date, records_synced, status)
                    VALUES (?, ?, ?)
                ''', (None, 0, f'error: {str(e)}'))
                conn.commit()
                conn.close()
            except:
                pass

            return False

    def get_cabinet_articles_by_template_id(
        self,
        sheet_name: str
    ) -> Tuple[Dict[int, str], Dict[int, List[str]]]:
        """
        Возвращает маппинг артикулов для указанного листа

        Args:
            sheet_name: Имя листа (например, "Отдельно Озон Nimba")

        Returns:
            Кортеж из двух словарей:
            - template_id_to_name: {template_id: "Название артикула"}
            - template_id_to_cabinet_arts: {template_id: ["артикул1", "артикул2"]}
        """
        # Проверяем, нужна ли синхронизация
        if self.needs_sync():
            logger.info("Выполняем автоматическую синхронизацию перед получением данных")
            self.sync_from_excel()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Получаем все шаблонные артикулы
        cursor.execute('''
            SELECT id, article_name
            FROM articles
            WHERE sheet_name = ?
        ''', (sheet_name,))

        template_id_to_name = {row[0]: row[1] for row in cursor.fetchall()}

        # Получаем маппинги кабинетных артикулов
        cursor.execute('''
            SELECT template_id, cabinet_article
            FROM article_mappings
            WHERE sheet_name = ?
            ORDER BY template_id
        ''', (sheet_name,))

        template_id_to_cabinet_arts = {}
        for template_id, cabinet_article in cursor.fetchall():
            # Гарантируем, что template_id есть в template_id_to_name
            if template_id not in template_id_to_name:
                template_id_to_name[template_id] = f"ID {template_id}"

            if template_id not in template_id_to_cabinet_arts:
                template_id_to_cabinet_arts[template_id] = []

            template_id_to_cabinet_arts[template_id].append(cabinet_article)

        conn.close()

        logger.debug(
            f"Получено для {sheet_name}: "
            f"{len(template_id_to_name)} шаблонов, "
            f"{len(template_id_to_cabinet_arts)} маппингов"
        )

        return template_id_to_name, template_id_to_cabinet_arts

    def get_sync_info(self) -> dict:
        """
        Получает информацию о последней синхронизации

        Returns:
            Словарь с информацией о синхронизации
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT sync_date, excel_modified_date, records_synced, status
            FROM sync_log
            ORDER BY sync_date DESC
            LIMIT 1
        ''')

        result = cursor.fetchone()

        if result:
            info = {
                'last_sync': result[0],
                'excel_modified': result[1],
                'records_synced': result[2],
                'status': result[3]
            }
        else:
            info = {
                'last_sync': None,
                'excel_modified': None,
                'records_synced': 0,
                'status': 'never_synced'
            }

        # Добавляем информацию о текущем состоянии Excel
        if os.path.exists(self.excel_path):
            info['excel_current_modified'] = datetime.fromtimestamp(
                os.path.getmtime(self.excel_path)
            ).isoformat()
            info['needs_sync'] = self.needs_sync()
        else:
            info['excel_current_modified'] = None
            info['needs_sync'] = False

        conn.close()
        return info


# Глобальный экземпляр базы данных
_db_instance = None


def get_database() -> ArticleDatabase:
    """Получить глобальный экземпляр базы данных (singleton)"""
    global _db_instance
    if _db_instance is None:
        _db_instance = ArticleDatabase()
    return _db_instance

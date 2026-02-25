import pandas as pd
import logging

logger = logging.getLogger(__name__)


def create_report(grouped, unmatched, id_to_name, main_ids_ordered, output_path):
    try:
        # Сводные данные
        total_purchases = sum(item['purchases'] for item in grouped.values()) + sum(
            item['purchases'] for item in unmatched.values())
        total_cancels = sum(item['cancels'] for item in grouped.values()) + sum(
            item['cancels'] for item in unmatched.values())
        total_income = sum(item['income'] for item in grouped.values()) + sum(
            item['income'] for item in unmatched.values())

        # Расчет показателей
        profit_per_unit = total_income / total_purchases if total_purchases else 0
        purchase_percent = total_purchases / (total_purchases + total_cancels) * 100 if (
                total_purchases + total_cancels) else 0

        summary_df = pd.DataFrame({
            'Показатель': ['Выкупы, шт', 'Валовая маржа, руб', 'Прибыль на 1 ед, руб', 'Отмены, шт', 'Процент выкупов'],
            'Значение': [
                total_purchases,
                total_income,
                profit_per_unit,
                total_cancels,
                f"{purchase_percent:.2f}%"
            ]
        })

        # Детальные данные
        detailed_data = []

        # 1. Основные товары в порядке шаблона
        for id_val in main_ids_ordered:
            if id_val in grouped:
                data = grouped[id_val]
                profit = data['income'] / data['purchases'] if data['purchases'] else 0
                detailed_data.append({
                    'Наименование': data['name'],
                    'Выкупы, шт': data['purchases'],
                    'Валовая маржа, руб': data['income'],
                    'Прибыль на 1 ед, руб': profit,
                    'Отмены, шт': data['cancels']
                })
            else:
                detailed_data.append({
                    'Наименование': id_to_name.get(id_val, f"ID {id_val}"),
                    'Выкупы, шт': 0,
                    'Валовая маржа, руб': 0,
                    'Прибыль на 1 ед, руб': 0,
                    'Отмены, шт': 0
                })

        # 2. Неопознанные артикулы
        for art, data in unmatched.items():
            profit = data['income'] / data['purchases'] if data['purchases'] else 0
            detailed_data.append({
                'Наименование': data.get('name', f"НЕОПОЗНАННЫЙ: {art}"),
                'Выкупы, шт': data.get('purchases', 0),
                'Валовая маржа, руб': data.get('income', 0),
                'Прибыль на 1 ед, руб': profit,
                'Отмены, шт': data.get('cancels', 0)
            })

        detailed_df = pd.DataFrame(detailed_data)

        # Сохранение в Excel
        with pd.ExcelWriter(output_path) as writer:
            summary_df.to_excel(writer, sheet_name='Сводный', index=False)
            detailed_df.to_excel(writer, sheet_name='Подробный', index=False)

        return True
    except Exception as e:
        logger.error(f"Ошибка при создании отчета: {e}")
        return False


def create_remains_report(report_data, output_path):
    """Создание отчета по остаткам"""
    try:
        # Создаем DataFrame из данных
        df = pd.DataFrame(report_data)

        # Сохранение в Excel
        df.to_excel(output_path, index=False)

        return True
    except Exception as e:
        logger.error(f"Ошибка при создании отчета по остаткам: {e}")
        return False
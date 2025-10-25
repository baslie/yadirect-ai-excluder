#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа площадок Яндекс Директ РСЯ
Автор: ИИ-агент по оптимизации контекстной рекламы
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import sys
import io

# Установка кодировки UTF-8 для вывода
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

warnings.filterwarnings('ignore')


def load_and_preprocess_data(file_path):
    """
    Загрузка и предобработка данных из CSV
    Пропускает первые 4 строки (заголовок клиента, итоги, пустая строка)
    """
    print("=" * 80)
    print("ШАГ 1: ЗАГРУЗКА И ПРЕДОБРАБОТКА ДАННЫХ")
    print("=" * 80)

    # Читаем файл, пропуская первые 4 строки
    df = pd.read_csv(file_path, sep=';', skiprows=4, encoding='utf-8-sig')

    print(f"✓ Загружено строк: {len(df)}")
    print(f"✓ Колонки: {list(df.columns)}")

    # Переименовываем колонки для удобства
    df.columns = [
        'Тип_площадки', 'Площадка', 'Показов', 'Кликов', 'CTR_%',
        'Расход_руб', 'Ср_цена_клика_руб', 'Отказы_%', 'Глубина_стр',
        'Цена_цели_руб', 'Конверсии'
    ]

    # Удаляем строки с пустыми названиями площадок
    df = df[df['Площадка'].notna() & (df['Площадка'] != '')]

    # Функция для очистки числовых значений
    def clean_numeric(value):
        if pd.isna(value) or value == '-' or value == '':
            return 0
        if isinstance(value, str):
            value = value.replace(',', '.').replace(' ', '').strip()
            try:
                return float(value)
            except:
                return 0
        return float(value)

    # Применяем очистку ко всем числовым колонкам
    numeric_cols = ['Показов', 'Кликов', 'CTR_%', 'Расход_руб',
                    'Ср_цена_клика_руб', 'Ср_ставка_за_клик_руб',
                    'Отказы_%', 'Ср_цена_тыс_показов_руб', 'Глубина_стр',
                    'Цена_цели_руб', 'Конверсии']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)

    print(f"✓ После очистки осталось строк: {len(df)}")
    print()

    return df


def calculate_averages(df):
    """
    Расчет средних показателей по всем площадкам
    """
    print("=" * 80)
    print("ШАГ 2: РАСЧЁТ СРЕДНИХ ПОКАЗАТЕЛЕЙ")
    print("=" * 80)

    # Средние показатели
    avg_ctr = df['CTR_%'].mean()
    avg_cpc = df['Ср_цена_клика_руб'].mean()
    avg_bounce = df['Отказы_%'].mean()
    avg_depth = df['Глубина_стр'].mean()
    avg_conversions = df['Конверсии'].mean()
    avg_spend = df['Расход_руб'].mean()

    # Средняя стоимость конверсии ТОЛЬКО по площадкам с конверсиями > 0
    df_with_conversions = df[df['Конверсии'] > 0]
    avg_cpa = df_with_conversions['Цена_цели_руб'].mean() if len(df_with_conversions) > 0 else 0

    averages = {
        'avg_ctr': avg_ctr,
        'avg_cpc': avg_cpc,
        'avg_bounce': avg_bounce,
        'avg_depth': avg_depth,
        'avg_conversions': avg_conversions,
        'avg_spend': avg_spend,
        'avg_cpa': avg_cpa
    }

    print(f"✓ Средний CTR: {avg_ctr:.2f}%")
    print(f"✓ Средняя цена клика: {avg_cpc:.2f} руб.")
    print(f"✓ Средний показатель отказов: {avg_bounce:.2f}%")
    print(f"✓ Средняя глубина просмотра: {avg_depth:.2f} стр.")
    print(f"✓ Среднее количество конверсий: {avg_conversions:.2f}")
    print(f"✓ Средний расход: {avg_spend:.2f} руб.")
    print(f"✓ Средняя стоимость конверсии (только с конверсиями): {avg_cpa:.2f} руб.")
    print(f"✓ Площадок с конверсиями: {len(df_with_conversions)}")
    print()

    return averages


def identify_platform_type(platform_name):
    """
    Определение типа площадки
    """
    platform_name = str(platform_name).lower()

    # Яндекс-площадки
    if 'yandex' in platform_name or platform_name == 'dzen.ru':
        return 'Яндекс-площадка'

    # DSP-площадки
    if platform_name.startswith('dsp-'):
        return 'DSP-площадка'

    # Мобильные приложения
    mobile_prefixes = ['com.', 'ru.', 'by.', 'fm.', 'org.', 'cz.',
                      'net.', 'biz.', 'game.', 'afisha.', 'asian.',
                      'air.', 'and.', 'io.', 'con.', 'tap.']
    for prefix in mobile_prefixes:
        if platform_name.startswith(prefix):
            # Проверяем, что это не домен второго уровня
            if platform_name.count('.') >= 2 or not ('yandex' in platform_name or 'dzen' in platform_name):
                return 'Мобильное приложение'

    # .com домены
    if platform_name.endswith('.com'):
        return 'Сайт (.com)'

    return 'Сайт'


def apply_blocking_criteria(df, averages):
    """
    Применение критериев минусации к каждой площадке
    """
    print("=" * 80)
    print("ШАГ 3: ПРИМЕНЕНИЕ КРИТЕРИЕВ МИНУСАЦИИ")
    print("=" * 80)

    results = []

    for idx, row in df.iterrows():
        platform = row['Площадка']
        platform_type = identify_platform_type(platform)

        # Проверяем, является ли площадка Яндекс-площадкой для применения мягких критериев
        is_yandex = platform_type == 'Яндекс-площадка'

        # Коэффициент для Яндекс-площадок
        yandex_coef = 1.5 if is_yandex else 1.0

        blocking_reason = None
        priority = None
        recommendation = "ОСТАВИТЬ"
        criteria_number = None
        deviation = ""
        special_features = []

        # Добавляем тип площадки в особенности
        if platform_type != 'Сайт':
            special_features.append(platform_type)

        # КРИТЕРИЙ 2.2а: Экстремально высокий CTR (≥ 50%)
        if row['CTR_%'] >= 50 and row['Показов'] >= 10:
            blocking_reason = "Экстремально высокий CTR (мошеннический трафик)"
            priority = "КРИТИЧНЫЙ"
            recommendation = "БЛОКИРОВАТЬ НЕМЕДЛЕННО"
            criteria_number = "2.2а"
            deviation = f"CTR {row['CTR_%']:.2f}% (норма 1-2%)"
            special_features.append("Экстремальный CTR")

        # КРИТЕРИЙ 2.2б: Подозрительно высокий CTR (10-50%)
        elif row['CTR_%'] >= (10 * yandex_coef) and row['CTR_%'] < 50 and row['Показов'] >= 10:
            if row['Конверсии'] == 0:
                blocking_reason = "Подозрительно высокий CTR без конверсий"
                priority = "ВЫСОКИЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.2б"
                deviation = f"CTR {row['CTR_%']:.2f}% при норме 1-2%, конверсий = 0"
                special_features.append("Высокий CTR")
            elif row['Конверсии'] > 0 and row['Цена_цели_руб'] > (averages['avg_cpa'] * 2.5 * yandex_coef):
                blocking_reason = "Высокий CTR + дорогие конверсии"
                priority = "ВЫСОКИЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.2б + 2.1"
                deviation = f"CTR {row['CTR_%']:.2f}%, Цена цели {row['Цена_цели_руб']:.2f}₽ > {averages['avg_cpa'] * 2.5 * yandex_coef:.2f}₽"

        # КРИТЕРИЙ 2.1A: Высокая стоимость конверсии
        if blocking_reason is None and row['Конверсии'] > 0:
            threshold_cpa = averages['avg_cpa'] * 2.5 * yandex_coef
            if row['Цена_цели_руб'] > threshold_cpa and averages['avg_cpa'] > 0:
                blocking_reason = "Высокая стоимость конверсии"
                priority = "КРИТИЧНЫЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.1A"
                deviation = f"Цена цели {row['Цена_цели_руб']:.2f}₽ в {row['Цена_цели_руб']/averages['avg_cpa']:.1f}х раз выше средней {averages['avg_cpa']:.2f}₽"

        # КРИТЕРИЙ 2.1Б: Нулевые конверсии при расходе
        if blocking_reason is None and row['Конверсии'] == 0:
            min_spend = 50 * yandex_coef
            if row['Расход_руб'] >= min_spend and row['Кликов'] >= 10:
                blocking_reason = "Нулевые конверсии при значительном расходе"
                priority = "КРИТИЧНЫЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.1Б"
                deviation = f"Расход {row['Расход_руб']:.2f}₽, кликов {int(row['Кликов'])}, конверсий = 0"

        # КРИТЕРИЙ 2.3: Критически низкий CTR
        if blocking_reason is None:
            low_ctr_threshold = 0.20 / yandex_coef
            if row['CTR_%'] < low_ctr_threshold and row['Показов'] >= 1000:
                blocking_reason = "Критически низкий CTR"
                priority = "ВЫСОКИЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.3"
                deviation = f"CTR {row['CTR_%']:.4f}% < {low_ctr_threshold:.2f}%"

        # КРИТЕРИЙ 2.8: Подозрительно низкая цена клика + высокий CTR
        if blocking_reason is None:
            if row['Ср_цена_клика_руб'] < (averages['avg_cpc'] * 0.3) and row['CTR_%'] > 15:
                blocking_reason = "Подозрительно низкая цена клика + высокий CTR"
                priority = "ВЫСОКИЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.8"
                deviation = f"Цена клика {row['Ср_цена_клика_руб']:.2f}₽ ({row['Ср_цена_клика_руб']/averages['avg_cpc']*100:.0f}% от средней), CTR {row['CTR_%']:.2f}%"

        # КРИТЕРИЙ 2.4: Низкая вовлеченность
        if blocking_reason is None:
            bounce_threshold = averages['avg_bounce'] * 1.2 * yandex_coef
            if (row['Отказы_%'] > bounce_threshold or row['Глубина_стр'] <= 1.0) and row['Конверсии'] == 0 and row['Кликов'] >= 20:
                blocking_reason = "Низкая вовлеченность пользователей"
                priority = "СРЕДНИЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.4"
                if row['Отказы_%'] > bounce_threshold:
                    deviation = f"Отказы {row['Отказы_%']:.2f}% > {bounce_threshold:.2f}%"
                else:
                    deviation = f"Глубина просмотра {row['Глубина_стр']:.2f} стр."

        # КРИТЕРИЙ 2.5: Мобильные приложения (усиленный анализ)
        if platform_type == 'Мобильное приложение' and blocking_reason is None:
            if (row['CTR_%'] > 15 and row['Конверсии'] == 0) or \
               (row['Ср_цена_клика_руб'] < 0.5 and row['CTR_%'] > 20) or \
               (row['Расход_руб'] > 30 and row['Конверсии'] == 0):
                blocking_reason = "Мобильное приложение: подозрительные показатели"
                priority = "ДОПОЛНИТЕЛЬНЫЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.5"
                deviation = f"CTR {row['CTR_%']:.2f}%, расход {row['Расход_руб']:.2f}₽, конверсий = 0"

        # КРИТЕРИЙ 2.5Б: DSP-площадки (усиленный анализ)
        if platform_type == 'DSP-площадка' and blocking_reason is None:
            if (row['CTR_%'] > 15 and row['Конверсии'] == 0) or \
               (row['Ср_цена_клика_руб'] < 0.5 and row['CTR_%'] > 20) or \
               (row['Расход_руб'] > 30 and row['Конверсии'] == 0):
                blocking_reason = "DSP-площадка: подозрительные показатели"
                priority = "ДОПОЛНИТЕЛЬНЫЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.5Б"
                deviation = f"CTR {row['CTR_%']:.2f}%, расход {row['Расход_руб']:.2f}₽, конверсий = 0"

        # КРИТЕРИЙ 2.6: Домены .com
        if platform_type == 'Сайт (.com)' and blocking_reason is None:
            if row['Конверсии'] == 0 and row['Расход_руб'] > 30:
                blocking_reason = "Домен .com без конверсий при расходе"
                priority = "ДОПОЛНИТЕЛЬНЫЙ"
                recommendation = "БЛОКИРОВАТЬ"
                criteria_number = "2.6"
                deviation = f"Расход {row['Расход_руб']:.2f}₽, конверсий = 0"

        # Если площадка попала под критерии блокировки, добавляем в результаты
        if blocking_reason is not None:
            # Формируем детальное обоснование
            justification = f"{blocking_reason}. "
            justification += f"Показов: {int(row['Показов'])}, кликов: {int(row['Кликов'])}, "
            justification += f"CTR: {row['CTR_%']:.2f}%, расход: {row['Расход_руб']:.2f}₽. "

            if row['Конверсии'] > 0:
                justification += f"Конверсий: {int(row['Конверсии'])}, цена цели: {row['Цена_цели_руб']:.2f}₽. "
            else:
                justification += f"Конверсий: 0. "

            justification += f"Отказы: {row['Отказы_%']:.2f}%, глубина: {row['Глубина_стр']:.2f} стр. "
            justification += deviation

            results.append({
                'Площадка': platform,
                'Тип': platform_type,
                'Критерий_минусации': blocking_reason,
                'Номер_критерия': criteria_number,
                'Приоритет_блокировки': priority,
                'Показов': int(row['Показов']),
                'Кликов': int(row['Кликов']),
                'CTR_%': row['CTR_%'],
                'Конверсий': int(row['Конверсии']),
                'Стоимость_конверсии_руб': row['Цена_цели_руб'] if row['Цена_цели_руб'] > 0 else 0,
                'Расход_руб': row['Расход_руб'],
                'Показатель_отказов_%': row['Отказы_%'],
                'Глубина_просмотра': row['Глубина_стр'],
                'Отклонение_от_среднего': deviation,
                'Обоснование': justification,
                'Рекомендация': recommendation,
                'Особенности': ', '.join(special_features) if special_features else ''
            })

    print(f"✓ Площадок к блокировке найдено: {len(results)}")
    print()

    return pd.DataFrame(results)


def segment_platforms(df, averages):
    """
    Сегментация площадок на эффективные/средние/неэффективные
    """
    print("=" * 80)
    print("ШАГ 4: СЕГМЕНТАЦИЯ ПЛОЩАДОК")
    print("=" * 80)

    effective = []
    medium = []
    ineffective = []

    for idx, row in df.iterrows():
        # Категория A: ЭФФЕКТИВНЫЕ
        if (row['Конверсии'] > 0 and
            row['Цена_цели_руб'] <= averages['avg_cpa'] and
            0.5 <= row['CTR_%'] <= 2 and
            row['Отказы_%'] <= averages['avg_bounce']):
            effective.append(row['Площадка'])

        # Категория C: НЕЭФФЕКТИВНЫЕ (будут в отчете блокировки)
        # Проверяем критерии из предыдущего шага
        elif (row['CTR_%'] >= 50 or
              (row['CTR_%'] >= 10 and row['Конверсии'] == 0) or
              (row['Конверсии'] > 0 and row['Цена_цели_руб'] > averages['avg_cpa'] * 2.5) or
              (row['Конверсии'] == 0 and row['Расход_руб'] >= 50 and row['Кликов'] >= 10) or
              (row['CTR_%'] < 0.20 and row['Показов'] >= 1000)):
            ineffective.append(row['Площадка'])

        # Категория B: СРЕДНИЕ
        else:
            medium.append(row['Площадка'])

    total = len(df)
    print(f"✓ Категория A (ЭФФЕКТИВНЫЕ): {len(effective)} ({len(effective)/total*100:.1f}%)")
    print(f"✓ Категория B (СРЕДНИЕ): {len(medium)} ({len(medium)/total*100:.1f}%)")
    print(f"✓ Категория C (НЕЭФФЕКТИВНЫЕ): {len(ineffective)} ({len(ineffective)/total*100:.1f}%)")
    print()

    return {
        'effective': effective,
        'medium': medium,
        'ineffective': ineffective
    }


def create_analytical_report(df, blocking_df, averages, segments):
    """
    Создание аналитической справки
    """
    print("=" * 80)
    print("ШАГ 5: СОЗДАНИЕ АНАЛИТИЧЕСКОЙ СПРАВКИ")
    print("=" * 80)

    report = []
    report.append("=" * 80)
    report.append("АНАЛИТИЧЕСКАЯ СПРАВКА")
    report.append("Анализ площадок Яндекс Директ РСЯ")
    report.append("=" * 80)
    report.append("")

    # 6.1 Общая статистика
    report.append("1. ОБЩАЯ СТАТИСТИКА")
    report.append("-" * 80)
    report.append(f"Период анализа: 24.10.2025 - 25.10.2025")
    report.append(f"Всего площадок проанализировано: {len(df)}")
    report.append(f"Площадок к блокировке: {len(blocking_df)} ({len(blocking_df)/len(df)*100:.1f}%)")
    report.append(f"Площадок к наблюдению (средние): {len(segments['medium'])} ({len(segments['medium'])/len(df)*100:.1f}%)")
    report.append(f"Эффективных площадок: {len(segments['effective'])} ({len(segments['effective'])/len(df)*100:.1f}%)")
    report.append("")

    # 6.2 Финансовая оценка
    total_spend = df['Расход_руб'].sum()
    ineffective_spend = blocking_df['Расход_руб'].sum() if len(blocking_df) > 0 else 0

    report.append("2. ФИНАНСОВАЯ ОЦЕНКА")
    report.append("-" * 80)
    report.append(f"Общий расход на все площадки: {total_spend:.2f} руб.")
    report.append(f"Расход на неэффективные площадки: {ineffective_spend:.2f} руб.")
    report.append(f"Доля расхода на неэффективные площадки: {ineffective_spend/total_spend*100:.1f}%")

    # Средняя стоимость конверсии по эффективным площадкам
    effective_platforms = df[df['Площадка'].isin(segments['effective'])]
    if len(effective_platforms[effective_platforms['Конверсии'] > 0]) > 0:
        avg_cpa_effective = effective_platforms[effective_platforms['Конверсии'] > 0]['Цена_цели_руб'].mean()
        report.append(f"Средняя стоимость конверсии по эффективным площадкам: {avg_cpa_effective:.2f} руб.")
    else:
        report.append(f"Средняя стоимость конверсии по эффективным площадкам: данных нет")

    if len(blocking_df[blocking_df['Конверсий'] > 0]) > 0:
        avg_cpa_ineffective = blocking_df[blocking_df['Конверсий'] > 0]['Стоимость_конверсии_руб'].mean()
        report.append(f"Средняя стоимость конверсии по неэффективным площадкам: {avg_cpa_ineffective:.2f} руб.")
    else:
        report.append(f"Средняя стоимость конверсии по неэффективным площадкам: конверсий нет")

    report.append(f"Потенциальная экономия бюджета при блокировке: {ineffective_spend:.2f} руб.")
    report.append("")

    # 6.3 Распределение по критериям
    report.append("3. РАСПРЕДЕЛЕНИЕ ПО КРИТЕРИЯМ МИНУСАЦИИ")
    report.append("-" * 80)

    if len(blocking_df) > 0:
        criteria_groups = blocking_df.groupby('Номер_критерия').agg({
            'Площадка': 'count',
            'Расход_руб': 'sum'
        }).reset_index()
        criteria_groups.columns = ['Критерий', 'Количество', 'Расход']
        criteria_groups['Процент'] = criteria_groups['Количество'] / len(blocking_df) * 100

        for _, row in criteria_groups.iterrows():
            report.append(f"Критерий {row['Критерий']}: {int(row['Количество'])} площадок ({row['Процент']:.1f}%), расход {row['Расход']:.2f} руб.")
    else:
        report.append("Площадок к блокировке не найдено.")

    report.append("")

    # 6.3а Распределение по типам площадок
    report.append("4. РАСПРЕДЕЛЕНИЕ ПО ТИПАМ ПЛОЩАДОК")
    report.append("-" * 80)

    if len(blocking_df) > 0:
        type_groups = blocking_df.groupby('Тип').agg({
            'Площадка': 'count',
            'Расход_руб': 'sum'
        }).reset_index()
        type_groups.columns = ['Тип', 'Количество', 'Расход']
        type_groups['Процент'] = type_groups['Количество'] / len(blocking_df) * 100

        for _, row in type_groups.iterrows():
            report.append(f"{row['Тип']}: {int(row['Количество'])} площадок ({row['Процент']:.1f}%), расход {row['Расход']:.2f} руб.")

    report.append("")

    # 6.4 Топ-10 самых расходных неэффективных площадок
    report.append("5. ТОП-10 САМЫХ РАСХОДНЫХ НЕЭФФЕКТИВНЫХ ПЛОЩАДОК")
    report.append("-" * 80)

    if len(blocking_df) > 0:
        top10 = blocking_df.nlargest(10, 'Расход_руб')
        for idx, row in top10.iterrows():
            report.append(f"{row['Площадка']}: {row['Расход_руб']:.2f} руб., "
                         f"CTR {row['CTR_%']:.2f}%, конверсий {int(row['Конверсий'])}, "
                         f"{row['Критерий_минусации']}")

    report.append("")

    # 6.5 Рекомендации
    report.append("6. РЕКОМЕНДАЦИИ")
    report.append("-" * 80)

    # Приоритет блокировки
    if len(blocking_df) > 0:
        critical = blocking_df[blocking_df['Приоритет_блокировки'] == 'КРИТИЧНЫЙ']
        high = blocking_df[blocking_df['Приоритет_блокировки'] == 'ВЫСОКИЙ']

        report.append("ПРИОРИТЕТ БЛОКИРОВКИ:")
        report.append(f"1. КРИТИЧНЫЙ приоритет: {len(critical)} площадок - блокировать НЕМЕДЛЕННО")
        report.append(f"   Экономия: {critical['Расход_руб'].sum():.2f} руб.")
        report.append(f"2. ВЫСОКИЙ приоритет: {len(high)} площадок - блокировать после проверки")
        report.append(f"   Экономия: {high['Расход_руб'].sum():.2f} руб.")
        report.append("")

        # Топ-10 мобильных приложений
        mobile = blocking_df[blocking_df['Тип'] == 'Мобильное приложение']
        if len(mobile) > 0:
            report.append("ТОП-10 МОБИЛЬНЫХ ПРИЛОЖЕНИЙ ПО РАСХОДУ:")
            top_mobile = mobile.nlargest(10, 'Расход_руб')
            for idx, row in top_mobile.iterrows():
                report.append(f"  - {row['Площадка']}: {row['Расход_руб']:.2f} руб., CTR {row['CTR_%']:.2f}%")
            report.append(f"Рекомендация: Мобильные приложения демонстрируют высокий CTR без конверсий.")
            report.append(f"Это указывает на случайные клики. Блокировать {len(mobile)} приложений.")
            report.append("")

        # DSP-площадки
        dsp = blocking_df[blocking_df['Тип'] == 'DSP-площадка']
        if len(dsp) > 0:
            report.append("АНАЛИЗ DSP-ПЛОЩАДОК:")
            report.append(f"К блокировке: {len(dsp)} площадок, расход {dsp['Расход_руб'].sum():.2f} руб.")
            for idx, row in dsp.iterrows():
                report.append(f"  - {row['Площадка']}: {row['Расход_руб']:.2f} руб., {row['Критерий_минусации']}")
            report.append("Рекомендация: Программатик-площадки показывают низкую эффективность.")
            report.append("")

        # Яндекс-площадки
        yandex = blocking_df[blocking_df['Тип'] == 'Яндекс-площадка']
        if len(yandex) > 0:
            report.append("АНАЛИЗ ЯНДЕКС-ПЛОЩАДОК:")
            report.append(f"К блокировке: {len(yandex)} площадок, расход {yandex['Расход_руб'].sum():.2f} руб.")
            for idx, row in yandex.iterrows():
                report.append(f"  - {row['Площадка']}: {row['Расход_руб']:.2f} руб., {row['Критерий_минусации']}")
            report.append("ВНИМАНИЕ: Яндекс-площадки обычно качественные. Проверьте критерии повторно.")
            report.append("")

    report.append("=" * 80)
    report.append("КОНЕЦ АНАЛИТИЧЕСКОЙ СПРАВКИ")
    report.append("=" * 80)

    print("✓ Аналитическая справка создана")
    print()

    return '\n'.join(report)


def main():
    """
    Главная функция
    """
    print("\n")
    print("=" * 80)
    print(" " * 20 + "АНАЛИЗ ПЛОЩАДОК ЯНДЕКС ДИРЕКТ РСЯ")
    print("=" * 80)
    print("\n")

    # Путь к файлу (можно передать как аргумент командной строки)
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Используется файл из аргумента: {file_path}\n")
    else:
        # Путь по умолчанию (измените на актуальный)
        file_path = r'C:\Users\Roman\Desktop\direct\data\input\2025-10-24_2025-10-25_brelokavto.csv'
        print(f"Используется файл по умолчанию: {file_path}\n")

    # Шаг 1: Загрузка и предобработка
    df = load_and_preprocess_data(file_path)

    # Шаг 2: Расчет средних показателей
    averages = calculate_averages(df)

    # Шаг 3: Применение критериев минусации
    blocking_df = apply_blocking_criteria(df, averages)

    # Шаг 4: Сегментация площадок
    segments = segment_platforms(df, averages)

    # Шаг 5: Формирование CSV с площадками к блокировке
    print("=" * 80)
    print("ШАГ 6: СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
    print("=" * 80)

    if len(blocking_df) > 0:
        output_csv = r'C:\Users\Roman\Desktop\direct\data\output\Площадки_к_блокировке.csv'
        blocking_df.to_csv(output_csv, index=False, encoding='utf-8-sig', sep=';')
        print(f"✓ Файл сохранен: {output_csv}")
    else:
        print("✓ Площадок к блокировке не найдено - файл не создан")

    # Шаг 6: Создание аналитической справки
    report = create_analytical_report(df, blocking_df, averages, segments)

    output_txt = r'C:\Users\Roman\Desktop\direct\data\output\Аналитическая_справка.txt'
    with open(output_txt, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✓ Аналитическая справка сохранена: {output_txt}")
    print()

    # Краткое резюме
    print("=" * 80)
    print("КРАТКОЕ РЕЗЮМЕ")
    print("=" * 80)
    print(f"Всего площадок проанализировано: {len(df)}")
    print(f"К блокировке: {len(blocking_df)} площадок")

    if len(blocking_df) > 0:
        print(f"Потенциальная экономия: {blocking_df['Расход_руб'].sum():.2f} руб.")

        # Главные проблемные критерии
        top_criteria = blocking_df.groupby('Номер_критерия').size().nlargest(3)
        print("\nГлавные проблемные критерии:")
        for criteria, count in top_criteria.items():
            print(f"  - Критерий {criteria}: {count} площадок")

        # Топ-3 самых расходных
        print("\nТоп-3 самых расходных неэффективных площадки:")
        top3 = blocking_df.nlargest(3, 'Расход_руб')
        for idx, row in top3.iterrows():
            print(f"  {row['Площадка']}: {row['Расход_руб']:.2f} руб., {row['Критерий_минусации']}")
    else:
        print("Неэффективных площадок не обнаружено!")

    print("\n" + "=" * 80)
    print("АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()

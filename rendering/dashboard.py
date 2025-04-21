# Визуализация логов в дашборде

import logging
import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime
import psycopg2
from log2db.config import DATABASE_CONFIG
from log_export.export import export_to_dataframe
from rendering.layout import dash_layout

app = dash.Dash(__name__,
                requests_pathname_prefix='/dashboard/')


def fetch_logs_data(start_date=None, end_date=None, status_code=None, request_type=None):
    '''
    Экспортирует данные из БД.
    Применяет фильтрацию.
    '''
    logging.info("Начало извлечения данных для дашборда...")

    try:
        with psycopg2.connect(**DATABASE_CONFIG) as conn:
            df = export_to_dataframe(conn)
        logging.info("Данные успешно извлечены из базы данных")
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных из базы: {e}")
        raise

    try:
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
        logging.debug("Поле timestamp_utc преобразовано в datetime с таймзоной UTC")
    except Exception as e:
        logging.error(f"Ошибка при преобразовании timestamp_utc в datetime: {e}")
        raise

    filtered_rows_before = len(df)

    if isinstance(end_date, str) and end_date.lower() == "all":
        end_date = None
    elif isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
        if end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")

    try:
        if isinstance(start_date, str) and start_date.lower() != "all":
            start_date = pd.to_datetime(start_date).tz_localize("UTC")
        elif isinstance(start_date, pd.Timestamp) and start_date.tzinfo is None:
            start_date = start_date.tz_localize("UTC")

        if isinstance(end_date, str) and end_date.lower() != "all":
            end_date = pd.to_datetime(end_date)
            if end_date.tzinfo is None:
                end_date = end_date.tz_localize("UTC")
        elif isinstance(end_date, pd.Timestamp) and end_date.tzinfo is None:
            end_date = end_date.tz_localize("UTC")

        if start_date:
            df = df[df['timestamp_utc'] >= start_date]
            logging.debug(f"Фильтр по start_date применён: {start_date}")
        if end_date:
            df = df[df['timestamp_utc'] <= end_date]
            logging.debug(f"Фильтр по end_date применён: {end_date}")

        if status_code and status_code != 'all':
            df = df[df['status_code'] == int(status_code)]
            logging.debug(f"Фильтр по status_code применён: {status_code}")

        if request_type and request_type != 'all':
            df = df[df['request_type'] == request_type]
            logging.debug(f"Фильтр по request_type применён: {request_type}")

        filtered_rows_after = len(df)
        logging.info(f"Фильтрация завершена. Строк до фильтрации: {filtered_rows_before}, после: {filtered_rows_after}")
    except Exception as e:
        logging.error(f"Ошибка при применении фильтров: {e}")
        raise

    return df


def fetch_request_types(df):
    try:
        request_types = [{'label': 'Все', 'value': 'all'}] + [
            {'label': rt, 'value': rt} for rt in df['request_type'].unique()
        ]
        logging.debug("Список типов запросов успешно сформирован")
        return request_types
    except Exception as e:
        logging.error(f"Ошибка при формировании списка типов запросов: {e}")
        raise

logging.info("Инициализация: извлечение данных для формирования списка типов запросов...")
try:
    with psycopg2.connect(**DATABASE_CONFIG) as conn:
        df_initial = export_to_dataframe(conn)
    request_types = fetch_request_types(df_initial)
    logging.info("Инициализация списка типов запросов завершена")
except Exception as e:
    logging.error(f"Ошибка при инициализации списка типов запросов: {e}")
    raise


# Макет дашборда
app.layout = dash_layout

@app.callback(
    [
        Output('requests-over-time', 'figure'),
        Output('status-code-distribution', 'figure'),
        Output('top-api-paths', 'figure'),
        Output('avg-response-time', 'figure'),
        Output('status-code-by-request-type', 'figure') # Добавлен новый Output
    ],
    [
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('status-code-dropdown', 'value'),
        Input('request-type-dropdown', 'value')
    ]
)

def update_graphs(start_date, end_date, status_code, request_type):
    logging.info("Обновление графиков дашборда...")

    try:
        df = fetch_logs_data(start_date, end_date, status_code, request_type)
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных для графиков: {e}")
        raise

    # График 1: Запросы по времени (по часам суток)
    try:
        # Убедимся, что timestamp_utc распознается с временной зоной
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)  # Интерпретируем как UTC
        df['timestamp_local'] = df['timestamp_utc'].dt.tz_convert('Europe/Amsterdam') # Конвертируем в CET (ваша текущая временная зона)
        df['hour'] = df['timestamp_local'].dt.hour # Извлекаем час по локальному времени

        hourly_counts = df.groupby('hour').size().reset_index(name='count')

        # Создаем график
        fig1 = px.bar(hourly_counts, x='hour', y='count',
                      title='Количество запросов по часам суток (CET)',
                      labels={'hour': 'Час суток (CET)', 'count': 'Количество запросов'},
                      category_orders={'hour': list(range(24))}) # Упорядочиваем часы от 0 до 23

        fig1.update_xaxes(type='category') # Отображаем только представленные значения на оси X

        logging.debug("График 'Количество запросов по часам суток (CET)' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Количество запросов по часам суток': {e}")
        raise

    # График 2: Распределение статус-кодов
    try:
        status_counts = df['status_code'].value_counts().reset_index()
        status_counts.columns = ['status_code', 'count']
        status_counts = status_counts.sort_values(by='count', ascending=False) # Сортируем по убыванию количества
        status_counts['status_code_str'] = status_counts['status_code'].astype(str) # Для корректного отображения в легенде

        fig2 = px.bar(status_counts, x='status_code_str', y='count', color='status_code_str',
                      title='Распределение статус-кодов',
                      labels={'status_code_str': 'Статус-код', 'count': 'Количество'},
                      color_discrete_sequence=px.colors.sequential.Plasma,
                      category_orders={'status_code_str': status_counts['status_code_str'].tolist()}) # Учитываем порядок после сортировки

        fig2.update_layout(showlegend=True) # Показываем легенду
        fig2.update_xaxes(type='category') # Отображаем только представленные значения на оси X

        logging.debug("График 'Распределение статус-кодов' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Распределение статус-кодов': {e}")
        raise

    # График 3: Топ-10 API-путей
    try:
        top_api = df['api_path'].value_counts().head(10).reset_index()
        top_api.columns = ['api_path', 'count']
        fig3 = px.bar(top_api, x='count', y='api_path', orientation='h', title='Топ-10 API-путей', color='api_path', color_discrete_sequence = px.colors.sequential.Viridis)
        logging.debug("График 'Топ-10 API-путей' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Топ-10 API-путей': {e}")
        raise

    # График 4
    try:
        avg_response = df.groupby('api_path')['response_time'].mean().reset_index().sort_values(by='response_time', ascending=False).head(10)
        fig4 = px.bar(avg_response, x='response_time', y='api_path', orientation='h', title='Среднее время ответа по API-путям (Топ-5)', color='api_path', color_discrete_sequence = px.colors.sequential.Viridis_r)
        logging.debug("График 'Среднее время ответа по API-путям' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Среднее время ответа по API-путям': {e}")
        raise

    # График 5 (альтернативный): Распределение типов запросов по статус-кодам (по вертикали в группе)
    try:
        status_request_counts = df.groupby(['request_type', 'status_code']).size().reset_index(name='count')

        fig5 = px.bar(
            status_request_counts,
            x='request_type',  # Типы запросов на оси X
            y='count',
            color='status_code',  # Статус-коды как цветовые группы
            color_discrete_sequence=['#7BC17E', '#FFA15A', '#EF553B'],  # Зеленый, оранжевый, красный для 2xx/4xx/5xx
            title='Распределение статус-кодов по типам запросов',
            labels={
                'request_type': 'Тип запроса',
                'count': 'Количество запросов',
                'status_code': 'Статус-код'
            },
            barmode='group',  # Столбцы статус-кодов рядом внутри группы
            category_orders={
                'request_type': ['GET', 'POST', 'PUT', 'DELETE'],  # Порядок типов запросов
                'status_code': ['200', '404', '500']  # Порядок столбцов внутри группы
            },
            text='count'  # Подписи значений
        )

        # Финализируем настройки
        fig5.update_layout(
            legend_title_text='Статус-код',
            plot_bgcolor='white',
            bargap=0.3,  # Расстояние между группами (типами запросов)
            bargroupgap=0.1,  # Расстояние между столбцами статус-кодов внутри группы
            uniformtext_minsize=10
        )

        fig5.update_traces(
            texttemplate='%{text}',
            textposition='inside',
            marker_line_width=0.5,
            marker_line_color='white',
            opacity=0.9
        )
        logging.debug("График успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика: {e}")
        raise
    
    logging.info("Графики успешно обновлены")
    return fig1, fig2, fig3, fig4, fig5


if __name__ == '__main__':
    logging.info("Запуск приложения Dash...")
    app.run(debug=True)
    logging.info("Приложение Dash запущено")
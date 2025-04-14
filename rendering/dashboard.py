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

app = dash.Dash(__name__)

def fetch_logs_data(conn, start_date=None, end_date=None, status_code=None, request_type=None):
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
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        logging.debug("Поле timestamp_utc преобразовано в datetime")
    except Exception as e:
        logging.error(f"Ошибка при преобразовании timestamp_utc в datetime: {e}")
        raise

    # Применяем фильтры
    filtered_rows_before = len(df)
    try:
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
app.layout = html.Div([
    html.H1("Визуализация логов", style={'textAlign': 'center', 'fontSize': '24px', 'marginBottom': '10px'}),

    # Фильтры
    html.Div([
        html.Label("Даты:", style={'marginRight': '5px'}),
        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 12, 31),
            display_format='YYYY-MM-DD',
            style={'display': 'inline-block', 'marginRight': '10px'}
        ),
        html.Label("Статус-код:", style={'marginRight': '5px'}),
        dcc.Dropdown(
            id='status-code-dropdown',
            options=[
                {'label': 'Все', 'value': 'all'},
                {'label': '200', 'value': 200},
                {'label': '404', 'value': 404},
                {'label': '500', 'value': 500}
            ],
            value='all',
            style={'width': '100px', 'display': 'inline-block', 'verticalAlign': 'middle', 'marginRight': '10px'}
        ),
        html.Label("Тип запроса:", style={'marginRight': '5px'}),
        dcc.Dropdown(
            id='request-type-dropdown',
            options=request_types,
            value='all',
            style={'width': '100px', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
    ], style={'textAlign': 'center', 'marginBottom': '10px'}),

    # Графики (сетка 2x2)
    html.Div([
        # Первая строка: два графика
        html.Div([
            dcc.Graph(id='requests-over-time', style={'width': '50%', 'display': 'inline-block'}),
            dcc.Graph(id='status-code-distribution', style={'width': '50%', 'display': 'inline-block'}),
        ], style={'display': 'flex'}),
        # Вторая строка: два графика
        html.Div([
            dcc.Graph(id='top-api-paths', style={'width': '50%', 'display': 'inline-block'}),
            dcc.Graph(id='avg-response-time', style={'width': '50%', 'display': 'inline-block'}),
        ], style={'display': 'flex'}),
    ])
])

@app.callback(
    [
        Output('requests-over-time', 'figure'),
        Output('status-code-distribution', 'figure'),
        Output('top-api-paths', 'figure'),
        Output('avg-response-time', 'figure')
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

    # График 1
    try:
        df_time = df.groupby(df['timestamp_utc'].dt.floor('H')).size().reset_index(name='count')
        fig1 = px.line(df_time, x='timestamp_utc', y='count', title='Запросы по времени')
        logging.debug("График 'Запросы по времени' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Запросы по времени': {e}")
        raise

    # График 2
    try:
        fig2 = px.histogram(df, x='status_code', title='Распределение статус-кодов', nbins=20)
        logging.debug("График 'Распределение статус-кодов' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Распределение статус-кодов': {e}")
        raise

    # График 3
    try:
        top_api = df['api_path'].value_counts().head(10).reset_index()
        top_api.columns = ['api_path', 'count']
        fig3 = px.bar(top_api, x='count', y='api_path', orientation='h', title='Топ-10 API-путей')
        logging.debug("График 'Топ-10 API-путей' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Топ-10 API-путей': {e}")
        raise

    # График 4
    try:
        avg_response = df.groupby('api_path')['response_time'].mean().reset_index().sort_values(by='response_time', ascending=False).head(10)
        fig4 = px.bar(avg_response, x='response_time', y='api_path', orientation='h', title='Среднее время ответа по API-путям (Топ-10)')
        logging.debug("График 'Среднее время ответа по API-путям' успешно построен")
    except Exception as e:
        logging.error(f"Ошибка при построении графика 'Среднее время ответа по API-путям': {e}")
        raise

    logging.info("Графики успешно обновлены")
    return fig1, fig2, fig3, fig4


if __name__ == '__main__':
    logging.info("Запуск приложения Dash...")
    app.run_server(debug=True)
    logging.info("Приложение Dash запущено")
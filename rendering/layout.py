from dash import html, dcc
from datetime import datetime

dash_layout = html.Div([
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
                {'label': '303', 'value': 303},
                {'label': '304', 'value': 304},
                {'label': '403', 'value': 403},
                {'label': '404', 'value': 404},
                {'label': '500', 'value': 500},
                {'label': '502', 'value': 502}
            ],
            value='all',
            style={'width': '100px', 'display': 'inline-block', 'verticalAlign': 'middle', 'marginRight': '10px'}
        ),
        html.Label("Тип запроса:", style={'marginRight': '5px'}),
        dcc.Dropdown(
            id='request-type-dropdown',
            options=[
                {'label': 'Все', 'value': 'all'},
                {'label': 'GET', 'value': 'GET'},
                {'label': 'POST', 'value': 'POST'},
                {'label': 'PUT', 'value': 'PUT'},
                {'label': 'DELETE', 'value': 'DELETE'},
            ],
            value='all',
            style={'width': '100px', 'display': 'inline-block', 'verticalAlign': 'middle'}
        ),
    ], style={'textAlign': 'center', 'marginBottom': '10px'}),

    # Графики (сетка 3x2)
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
        # Третья строка: один график (можно добавить еще один, если понадобится)
        html.Div([
            dcc.Graph(id='status-code-by-request-type', style={'width': '50%', 'display': 'inline-block'}),
            html.Div(style={'width': '50%', 'display': 'inline-block'}), # Пустой div для выравнивания, если только 5 графиков
        ], style={'display': 'flex'}),
    ])
])
from dash import html, dcc
from datetime import datetime

dash_layout = html.Div([
    # Основной контейнер
    html.Div([
        # Сайдбар
        html.Div([
            html.H2("📊 Дашборд логов", style={
                'color': 'white',
                'textAlign': 'center',
                'marginBottom': '30px'
            }),

            html.Label("Фильтрация", style={'color': 'white', 'fontWeight': 'bold'}),
            html.Div([
                # DatePickerRange
                dcc.DatePickerRange(
                    id='date-picker-range',
                    start_date=datetime(2023, 1, 1),
                    end_date=datetime(2023, 12, 31),
                    display_format='YYYY-MM-DD',
                    style={'marginBottom': '15px', 'width': '100%'},
                    className='dcc-date-picker-range'
                ),
                # Dropdown для статус-кодов
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
                    placeholder="Статус-код",
                    style={'marginBottom': '10px'},
                    className='dcc-select'
                ),
                # Dropdown для типов запросов
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
                    placeholder="Тип запроса",
                    className='dcc-select'
                )
            ], className="dcc-select-container"),
            
            # Логотип и подпись
            html.Div([
                html.Img(src='assets/logo.png', style={
                    'width': '150px',
                    'display': 'block',
                    'margin': '40px auto 10px auto'
                }),
                html.P("ЦК МФТИ, 2025", style={
                    'color': 'rgba(255, 255, 255, 0.5)',
                    'textAlign': 'center',
                    'marginBottom': '20px',
                    'fontSize': '14px'
                })
            ], style={
                'marginTop': 'auto',
                'paddingBottom': '20px'
                }),
            
        ], className="sidebar", style={'display': 'flex', 'flexDirection': 'column'}),

        # Основная область контента
        html.Div([
            dcc.Tabs([
                dcc.Tab(label='📊 Общее', children=[
                    html.Div([
                        dcc.Graph(id='requests-over-time', className='dash-graph'),
                        dcc.Graph(id='status-code-distribution', className='dash-graph'),
                        dcc.Graph(id='top-api-paths', className='dash-graph'),
                        dcc.Graph(id='avg-response-time', className='dash-graph'),
                        dcc.Graph(id='status-code-by-request-type', className='dash-graph'),
                    ], className='grid-container')
                ]),
                dcc.Tab(label='⏱ Активность', children=[
                    html.Div([
                        dcc.Graph(id='requests-over-time-activity', className='dash-graph'),
                        dcc.Graph(id='top-api-paths-activity', className='dash-graph'),
                    ], className='grid-container')
                ]),
                dcc.Tab(label='🧾 Ответы', children=[
                    html.Div([
                        dcc.Graph(id='status-code-distribution-responses', className='dash-graph'),
                        dcc.Graph(id='status-code-by-request-type-responses', className='dash-graph'),
                    ], className='grid-container')
                ]),
                dcc.Tab(label='🛣 API', children=[
                    html.Div([
                        dcc.Graph(id='top-api-paths-activity-2', className='dash-graph'),
                        dcc.Graph(id='avg-response-time-2', className='dash-graph'),
                    ], className='grid-container')
                ]),
                dcc.Tab(label='⚡ Производительность', children=[
                    html.Div([
                        dcc.Graph(id='avg-response-time-performance', className='dash-graph'),
                        dcc.Graph(id='requests-over-time-performance', className='dash-graph'),
                    ], className='grid-container')
                ]),
            ], style={'padding': '20px'})
        ], className="main-content")
    ])
], style={'fontFamily': 'Segoe UI, sans-serif'})
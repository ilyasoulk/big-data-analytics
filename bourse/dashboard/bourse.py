import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import sqlalchemy
import logging

logging.basicConfig(level=logging.DEBUG,  # Set minimum log level to DEBUG
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, log level, and message
                    handlers=[
                        logging.FileHandler("debug.log"),  # Log to a file
                        logging.StreamHandler()  # Log to standard output (console)
                    ])

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

companies_df = pd.read_sql_query("SELECT name FROM companies;", engine)
companies = companies_df['name'].to_numpy()

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True) # , external_stylesheets=external_stylesheets)
server = app.server
app.layout = html.Div([
        html.H2(
            f"Fuck Ricou",
            style={"textAlign": "center", "color": "#fff"}  # Couleur du texte blanc
            ,
        ),
        html.Div([
            dcc.RadioItems(
                id="chandelier",
                options=[
                    {"label": "Ligne", "value": "Ligne"},
                    {"label": "Chandelier", "value": "Chandelier"},
                    {"label": "Query", "value": "Query"}
                ],
                value="Chandelier",
                style={"flex": 1, "background-color": "#d1c4e9", "border-radius": "5px", "padding": "10px"}
            ),
            dcc.Textarea(
                id='sql-query',
                value='''
                    SELECT * FROM pg_catalog.pg_tables
                        WHERE schemaname != 'pg_catalog' AND 
                                schemaname != 'information_schema';
                ''',
                style={'width': '100%', 'height': 100},
            ),
            html.Button('Execute', id='execute-query', n_clicks=0),
            html.Div(id='query-result'),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=pd.to_datetime("2000-01-01"),
                max_date_allowed=pd.to_datetime("today"),
                initial_visible_month=pd.to_datetime("today"),
                start_date=pd.to_datetime("2000-01-01"),
                end_date=pd.to_datetime("today"),
                display_format="YYYY-MM-DD",
                clearable=True,
                style={"flex": 1, "background-color": "#d1c4e9", "border-radius": "5px", "padding": "10px"}
            ),
            dcc.Dropdown(
                id="checklist",
                options=companies,
                value=companies[:3],
                multi=True,
                style={"flex": 1, "background-color": "#d1c4e9", "border-radius": "5px", "padding": "10px"}
            ), 
            dcc.Checklist(
                id="markets",
                options=["amsterdam", "bruxelle", "paris", "amsterdam"],
                value=['bruxelle'],  
                inline=True,
                style={
                    "flex": 1, "background-color": "#d1c4e9", "border-radius": "5px", "padding": "10px", "align-items": "center", "display": "flex", 
                },
              
            )
            ],
            style={"display": "flex", "flex-direction": "row", "row-gap": "10px", "column-gap": "10px", "margin-bottom": "20px"}
        ),
        html.Div(id="graphs"),
    ],
    style={
        "background": "linear-gradient(to bottom, #471463, #1f072b)",
        "padding": "20px",  # Remplissage autour du contenu
        "min-height": "100vh"
    }
)

@app.callback(ddep.Output('graphs', 'children'),
               [
                ddep.Input('chandelier', 'value'),
                ddep.Input('checklist', 'value'),
                ddep.Input('markets', 'value'),
                ddep.Input('date-range', 'start_date'),
                ddep.Input('date-range', 'end_date'),
                ddep.Input('execute-query', 'n_clicks')
            ],
            ddep.State('sql-query', 'value')
)

def update_graph(style, companies, markets, start_date, end_date, n_clicks, query):
        graphs = []
        if style == "Chandelier":
            for company in companies:
                if not query:
                    query = f"""SELECT date, open, close, high, low, volume FROM daystocks ds
                            JOIN companies c ON ds.cid = c.id
                            JOIN markets m ON c.mid = m.id
                            WHERE c.name = '{company}'"""

                    if markets:
                        markets_condition = "('" + "', '".join(markets) + "')"
                        query += f" AND m.alias IN {markets_condition}"

                    if start_date and end_date:
                        query += f" AND date BETWEEN '{start_date}' AND '{end_date}';"
                    else:
                        query += ";"

                df = pd.read_sql_query(query, engine)

                df['date'] = pd.to_datetime(df['date'])
                df.sort_values(by='date', inplace=True)

                ma_size = 10
                bol_size = 2

                df['bollinger_bands'] = df['close'].rolling(ma_size).mean()

                df['upper_band'] = df['bollinger_bands'] + df['close'].rolling(ma_size).std() * bol_size
                df['lower_band'] = df['bollinger_bands'] - df['close'].rolling(ma_size).std() * bol_size

                candlestick = {
                    'x': df['date'],
                    'open': df['open'],
                    'close': df['close'],
                    'high': df['high'],
                    'low': df['low'],
                    'type': 'candlestick',
                    'name': f'Candlestick - {company}',
                    'legendgroup': company,
                    'increasing': {'line': {'color': '#B7C9E2'}},
                    'decreasing': {'line': {'color': '#8E9CB0'}}
                }

                bollinger_traces = []
                for parameter, color in zip(['bollinger_bands', 'lower_band', 'upper_band'], ['#ADD8E6', '#FF7F7F', '#90EE90']):
                    bollinger_traces.append(go.Scatter(x=df['date'],
                                                    y=df[parameter],
                                                    showlegend=True,
                                                    name=f"{parameter.replace('_', ' ').title()} - {company}",
                                                    line_color=color,
                                                    mode='lines',
                                                    line={'dash': 'dash'},
                                                    marker_line_width=2,
                                                    marker_size=10,
                                                    opacity=0.8))

                volume_trace = go.Bar(x=df['date'],
                              y=df['volume'],
                              name=f'Volume - {company}',
                              marker_color='rgba(0, 0, 255, 0.5)')

                graphs.append(dcc.Graph(
                    id=f"{company}-Candlestick",
                    figure={
                        'data': [candlestick] + bollinger_traces,
                        'layout': {
                            'margin': {'b': 0, 'r': 10, 'l': 60, 't': 0},
                            'legend': {'x': 0},
                            'xaxis': {'rangeslider': {'visible': True}}
                        }
                    },
                ))

                graphs.append(dcc.Graph(
                    id=f"{company}-Volume",
                    figure={
                        'data': [volume_trace],
                        'layout': {
                            'margin': {'b': 30, 'r': 10, 'l': 60, 't': 0},
                            'legend': {'x': 0},
                            'xaxis': {'rangeslider': {'visible': True}}
                        }
                    },
                ))
        else:
            for company in companies:
                if not query:
                    query = f"""SELECT s.date, s.value, c.name
                            FROM stocks s
                            JOIN companies c ON s.cid = c.id
                            JOIN markets m ON c.mid = m.id
                            WHERE c.name = '{company}'"""

                    if markets:
                        markets_condition = "('" + "', '".join(markets) + "')"
                        query += f" AND m.alias IN {markets_condition};"
                    else:
                        query += ";"

                df = pd.read_sql_query(query, engine)
                df['date'] = pd.to_datetime(df['date'])
                df.sort_values(by='date', inplace=True)
                fig = px.line(df, x='date', y='value', color='name')
                fig.update_layout(title=company,
                    xaxis_title='Date',
                    yaxis_title='Price',
                )
                graphs.append(dcc.Graph(
                    id=f'{company} - Line',
                    figure=fig,
                ))

        return graphs
            


if __name__ == '__main__':
    app.run(debug=True)

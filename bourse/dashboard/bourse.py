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
        style={"textAlign": "center"},
    ),
    html.Div("Type de graphe chacal"),
    dcc.RadioItems(
        id="chandelier",
        options=["Ligne", "Chandelier"],
        value="Chandelier",
    ),
    dcc.DatePickerRange(
        id="date-range",
        min_date_allowed=pd.to_datetime("2000-01-01"),
        max_date_allowed=pd.to_datetime("today"),
        initial_visible_month=pd.to_datetime("today"),
        start_date=pd.to_datetime("2000-01-01"),
        end_date=pd.to_datetime("today"),
        display_format="YYYY-MM-DD",
        clearable=True,
    ),
    dcc.Dropdown(
        id="checklist",
        options=companies,
        value=companies[:3],
        multi=True,
    ),
    html.Div(id="graphs"),
    dcc.Checklist(
        id="markets",
        options=["amsterdam", "bruxelle", "paris"],
        value=["bruxelle"],
        inline=True,
    ),
    ]
)

@app.callback(ddep.Output('graphs', 'children'),
               [
                ddep.Input('chandelier', 'value'),
                ddep.Input('checklist', 'value'),
                ddep.Input('markets', 'value'),
                ddep.Input('date-range', 'start_date'),
                ddep.Input('date-range', 'end_date')
            ]
)

def update_graph(style, companies, markets, start_date, end_date):
    if not companies:
        return html.H3(
            "Select a company.",
            style={'marginTop': 20, 'marginBottom': 20}
        )
    else:
        graphs = []
        if style == "Chandelier":
            for company in companies:
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
                            'xaxis': {'rangeslider': {'visible': False}}
                        }
                    }
                ))

                graphs.append(dcc.Graph(
                    id=f"{company}-Volume",
                    figure={
                        'data': [volume_trace],
                        'layout': {
                            'margin': {'b': 30, 'r': 10, 'l': 60, 't': 0},
                            'legend': {'x': 0},
                            'xaxis': {'rangeslider': {'visible': False}}
                        }
                    }
                ))
        else:
            for company in companies:
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
                fig.update_layout(title='Line Chart',
                    xaxis_title='Date',
                    yaxis_title='Price')

                graphs.append(dcc.Graph(
                    id=f'{company} - Line',
                    figure=fig
                ))

        return graphs
            


if __name__ == '__main__':
    app.run(debug=True)

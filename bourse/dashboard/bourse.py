import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import plotly.graph_objects as go
import plotly.express as px
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
    dcc.Checklist(
        id="toggle-rangeslider",
        options=[{"label": "Include Rangeslider", "value": "slider"}],
        value=["slider"],
    ),
    dcc.Dropdown(
        id="checklist",
        options=companies,
        value=companies[:3],
        multi=True,
    ),
    dcc.Graph(id='graphs'),
    dcc.Checklist(
        id="markets",
        options=["amsterdam", "bruxelle", "paris"],
        value=["bruxelle"],
        inline=True,
    ),
    ]
)

@app.callback(ddep.Output('graphs', 'figure'),
               [
                ddep.Input('chandelier', 'value'),
                ddep.Input('checklist', 'value'),
                ddep.Input('toggle-rangeslider', 'value'),
                ddep.Input('markets', 'value')
            ]
)

def update_graph(style, companies, slider, markets):
    if not companies:
        return html.H3(
            "Select a company.",
            style={'marginTop': 20, 'marginBottom': 20}
        )
    else:
        if style == "Chandelier":
            companies_condition = "('" + "', '".join(companies) + "')"
            query = f"""SELECT date, open, close, high, low FROM daystocks ds
                    JOIN companies c ON ds.cid = c.id
                    JOIN markets m ON c.mid = m.id
                    WHERE c.name IN {companies_condition}"""

            if markets:
                markets_condition = "('" + "', '".join(markets) + "')"
                query += f" AND m.alias IN {markets_condition};"
            else:
                query += ";"

            df = pd.read_sql_query(query, engine)

            df['date'] = pd.to_datetime(df['date'])
            df.sort_values(by='date', inplace=True)

            ma_size = 10
            bol_size = 2

            df['moving_average'] = df['close'].rolling(ma_size).mean()

            df['bol_upper'] = df['moving_average'] + df['close'].rolling(ma_size).std() * bol_size
            df['bol_lower'] = df['moving_average'] - df['close'].rolling(ma_size).std() * bol_size


            fig = go.Figure(
                data = [
                    go.Candlestick(
                        x=df['date'],
                        open=df['open'],
                        close=df['close'],
                        high=df['high'],
                        low=df['low']
                    )
                ]
            )

            for parameter in ['moving_average', 'bol_lower', 'bol_upper']:
                fig.add_trace(go.Scatter(
                    x = df['date'],
                    y = df[parameter],
                    showlegend = False,
                    line_color = 'gray',
                    mode='lines',
                    line={'dash': 'dash'},
                    marker_line_width=2, 
                    marker_size=10,
                    opacity = 0.8))
        else:
            companies_condition = "('" + "', '".join(companies) + "')"
            query = f"""SELECT s.date, s.value, c.name
                    FROM stocks s
                    JOIN companies c ON s.cid = c.id
                    JOIN markets m ON c.mid = m.id
                    WHERE c.name IN {companies_condition}"""

            if markets:
                markets_condition = "('" + "', '".join(markets) + "')"
                query += f" AND m.alias IN {markets_condition};"
            else:
                query += ";"

            df = pd.read_sql_query(query, engine)
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values(by='date', inplace=True)
            fig = px.line(df, x='date', y='value', color='name')
            
        fig.update_layout(xaxis_rangeslider_visible="slider" in slider)
        return fig

    return None


if __name__ == '__main__':
    app.run(debug=True)


# html.Div([
#                 dcc.Textarea(
#                     id='sql-query',
#                     value='''
#                         SELECT * FROM pg_catalog.pg_tables
#                             WHERE schemaname != 'pg_catalog' AND 
#                                   schemaname != 'information_schema';
#                     ''',
#                     style={'width': '100%', 'height': 100},
#                     ),
#                 html.Button('Execute', id='execute-query', n_clicks=0),
#                 html.Div(id='query-result')
#              ])
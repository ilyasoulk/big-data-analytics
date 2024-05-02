import dash
from dash import dcc
from dash import html
import dash.dependencies as ddep
import plotly.graph_objects as go
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
    dcc.Checklist(
        id="checklist",
        options=["amsterdam", "bruxelle", "paris"],
        value=["paris"],
        inline=True,
    ),
    dcc.Graph(id='graphs')
    ]
)

@app.callback(ddep.Output('graphs', 'figure'),
               [
                ddep.Input('chandelier', 'value'),
                ddep.Input('checklist', 'value'),
                ddep.Input('toggle-rangeslider', 'value')
            ]
)

def update_graph(style, markets, slider):
    if not markets:
        return html.H3(
            "Select a market.",
            style={'marginTop': 20, 'marginBottom': 20}
        )
    else:
        market_condition = "('" + "', '".join(markets) + "')"
        query = f"""SELECT date, open, close, high, low FROM daystocks ds
                JOIN companies c ON ds.cid = c.id
                JOIN markets m ON c.mid = m.id
                WHERE m.alias IN {market_condition};"""

        df = pd.read_sql_query(query, engine)
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
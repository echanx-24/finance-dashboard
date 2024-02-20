import sqlite3 as sql
import pandas as pd
import plotly.graph_objects as go
import yfinance as yf
import warnings

def query_data(ticker, connection):
    query = f'''
        SELECT date, revenue, costofrevenue, grossprofit, research, sga, otheroperating, operatingexp, operatinginc
        FROM income_statement_qtr WHERE ticker == '{ticker}'
    '''

    df = pd.read_sql(query, connection)
    df = df.iloc[-8:]

    df = df.rename(columns={"DATE":"Date", "FRAME":"Quarter", "REVENUE":"Revenue", "COSTOFREVENUE":"Cost of Revenue",
                            "GROSSPROFIT":"Gross Profit", "RESEARCH":"R&D", "SGA":"SG&A", "OTHEROPERATING":"Other Operating",
                            "OPERATINGEXP":"Operating Expense", "OPERATINGINC":"Operating Income"})

    df["Gross Margin"] = df["Gross Profit"]/df["Revenue"]
    df["Operating Margin"] = df["Operating Income"]/df["Revenue"]

    for i in df.columns:
        if df[i].dtype == "int64":
            df[i] = df[i]/1000

    df = df.set_index("Date")

    df = df[["Revenue", "Cost of Revenue", "Gross Profit", "Gross Margin", "R&D", "SG&A",
             "Other Operating", "Operating Expense", "Operating Income", "Operating Margin"]]

    return df

def bar_chart(df, ticker, color="#023047"):

    fig = go.Figure()

    fig.add_trace(go.Bar(x=df.index, y=df["Revenue"], name = "Revenue", marker_color=color, hovertemplate="%{y:,d}", offsetgroup=1))
    fig.add_trace(go.Bar(x=df.index, y=df["Cost of Revenue"], name="Cost Of Revenue", marker_color="#780000", hovertemplate ="%{y:,d}", offsetgroup=2))
    fig.add_trace(go.Bar(x=df.index, y=df["R&D"], name="R&D", marker_color="#c1121f", hovertemplate="%{y:,d}", offsetgroup=2, base=df["Cost of Revenue"]))
    fig.add_trace(go.Bar(x = df.index, y = df["SG&A"], name = "SG&A", marker_color = "#f08080", hovertemplate = "%{y:,d}", offsetgroup=2, base=df["R&D"]+df["Cost of Revenue"]))
    fig.add_trace(go.Bar(x = df.index, y = df["Other Operating"], name = "Other Operating", marker_color = "#ffdab9", hovertemplate = "%{y:,d}",
                         offsetgroup=2, base=df["R&D"]+df["Cost of Revenue"]+df["SG&A"]))

    fig.update_xaxes(showgrid = False, tickfont = dict(size=16, color="black"))
    fig.update_yaxes(showgrid = True, tickfont = dict(size=16, color="black"), gridcolor = "#D9D9D9")
    fig.update_layout(showlegend = True,hovermode = "x unified",
                    legend = dict(orientation="h", x=1, y=1.02, yanchor="bottom", xanchor="right", font=dict(color="black", size=18)),
                    hoverlabel = dict(font=dict(size = 18, color = "black", family = "arial")),
                    paper_bgcolor = "white", plot_bgcolor = "white",
                    title=dict(text=f"<b>{ticker} Financials Chart</b>", font=dict(size=20, color="black")), title_x=0)

    return fig

def kpi(df):
    ttm_rev = df["Revenue"].iloc[-4:].sum()

    prior_rev = df["Revenue"].iloc[:-4].sum()

    growth = (ttm_rev - prior_rev)/prior_rev
    growth = f"{round(growth*100,2)}%"

    ttm_profit = df["Gross Profit"].iloc[-4:].sum()
    prior_profit = df["Gross Profit"].iloc[:-4].sum()
    ttm_margin = ttm_profit/ttm_rev
    prior_margin = prior_profit/prior_rev
    margin_growth = (ttm_margin-prior_margin)/prior_margin
    ttm_margin = f"{round(ttm_margin*100,2)}%"
    margin_growth = f"{round(margin_growth*100,2)}%"

    ttm_operating_income = ttm_profit - df["Operating Expense"].iloc[-4:].sum()
    prior_operating_income = prior_profit - df["Operating Expense"].iloc[-8:-4].sum()
    operating_income_growth = f"{round((ttm_operating_income-prior_operating_income)/abs(prior_operating_income)*100,2)}%"

    return f"{int(ttm_rev):,d}", growth, ttm_margin, margin_growth, \
        f"{int(ttm_operating_income):,d}", operating_income_growth

def table_chart(df, color):

    df = df[["Revenue", "Cost of Revenue", "Gross Profit", "R&D", "SG&A",
             "Other Operating", "Operating Expense", "Operating Income"]]
    
    df = df.iloc[::-1].T
    df = df.reset_index()
    df = df.rename(columns={"index":""})
    
    coeff = len(list(df.columns))
    fig = go.Figure()
    fig.add_trace(go.Table(
    header=dict(values=list(df.columns), fill_color=color,
               font=dict(family="Arial", size=18, color="white"), line_color="black", height=40,
               align=["center"]*coeff),
    cells=dict(values=[df[i] for i in df.columns],
               fill_color=[["#ffffff", "#d9d9d9"]*coeff],format=[[",d"] if i != "" else [None] for i in df.columns],
               font=dict(family="Arial", size=18, color="black"), height=30, line_color="black",
               align=["center"]*coeff
            )))
    fig.update_layout(height=750)

    return fig

def get_name(ticker):
    conn = sql.connect("dashboard/financials.db")
    name = pd.read_sql(f"SELECT name FROM company WHERE ticker == '{ticker}'", conn)
    name = name.iloc[0].item()
    # name = cursor.execute("SELECT name FROM company WHERE ticker == ?", (ticker,))
    return name.title()

def candlestick_chart(ticker):
    warnings.filterwarnings('ignore')
    data = yf.Ticker(ticker)
    df = data.history(period="6mo")

    def EMA(df, n, column="Close"):
        return df[column].ewm(span=n).mean()

    df["EMA14"] = EMA(df, 14)
    df = df.dropna()

    df.index = df.index.strftime('%Y-%m-%d')

    price = round(float(df["Close"].iloc[-1]),2)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(x = df.index, open = df["Open"], close = df["Close"], high = df["High"], low = df["Low"],
                                increasing_line_color = "black", increasing_line_width = 0.25, increasing_fillcolor = "#023047",
                                decreasing_line_color = "black", decreasing_line_width = 0.25, decreasing_fillcolor = "#DC3855",
                                showlegend = False, name = "<b>Candlestick</b>"))
    fig.add_trace(go.Scatter(x=df.index, y=df["EMA14"], name="<b>EMA 14</b>", line_color="#e26b0a"))

    fig.update_xaxes(rangeslider_visible = False, tickfont = dict(color = "black", size = 16), showline = True, showgrid = True,
                    gridcolor = "#D9D9D9", rangebreaks = [dict(bounds=["sat", "mon"])])
    fig.update_yaxes(tickfont = dict(color = "black", size = 16), showline = True, linecolor = "black",
                    showgrid = True, gridcolor = "#D9D9D9")

    fig.update_layout(hovermode = "x unified", hoverlabel = dict(bgcolor = "white", font = dict(size = 18, color = "black")),
                    showlegend = True, legend = dict(orientation = "h", yanchor = "bottom", y = 1.02, xanchor = "right", x = 1,
                    font = dict(size = 14, color = "black", family = "arial")),
                    yaxis = {"side":"right"}, paper_bgcolor = "white", plot_bgcolor = "white", height = 500,
                    title=dict(text=f"<b>{ticker} Stock Chart</b>", font=dict(size=20, color="black")), title_x=0)

    return price, fig


def donut_chart(df, ticker):
    values = [df["Cost of Revenue"].iloc[-4:].sum(), df["R&D"].iloc[-4:].sum(), df["SG&A"].iloc[-4:].sum(), df["Other Operating"].iloc[-4:].sum()]
    labels = ["Cost of Revenue", "R&D", "SG&A", "Other Operating"]
    colors = ["#780000", "#c1121f", "#f08080", "#ffdab9"]

    fig = go.Figure()
    fig.add_trace(go.Pie(values=values, labels=labels, hole=0.45, marker_colors=colors, textfont_size=20, textfont_color="white"))

    fig.update_layout(hovermode = "x unified", hoverlabel = dict(bgcolor = "white", font = dict(size = 18, color = "black")),
                    showlegend = True, legend = dict(orientation = "h", yanchor = "bottom", y = 1.1, xanchor = "right", x = 1,
                    font = dict(size = 18, color = "black", family = "arial")),
                    yaxis = {"side":"right"}, paper_bgcolor = "white", plot_bgcolor = "white", height = 500,
                    title=dict(text=f"<b>{ticker} TTM Expense Chart</b>", font=dict(size=20, color="black")), title_x=0)
    
    return fig
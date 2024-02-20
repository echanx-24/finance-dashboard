from dashboard import query_data, bar_chart, kpi, table_chart, get_name, candlestick_chart
import streamlit as st
import sqlite3 as sql

connection = sql.connect("dashboard/financials.db")

if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="Finance Dashboard")

    st.title("Finance Dashboard")
    st.divider()

    symbol_list = ["PEGA", "PATH", "CRM", "BB", "DOCN", "AKAM", "SNOW", "DDOG", "AMD", "NOW", "MSFT", "SQ", "TSLA",
               "NVDA", "ADBE", "ROKU", "AAPL", "INTC", "GOOGL", "RBLX", "ZM", "U", "PANW", "SNPS", "CRWD",
               "TEAM", "ZS", "VEEV", "MDB", "NET", "PTC", "BSY", "NTNX", "GEN", "TOST", "MSTR", "DOCU", "DBX", "ALTR",
               "FIVN", "WK", "BLKB", "AI", "APPN", "SWI", "BLZE", "OSPN", "ME", "RBLX"]

    ticker = st.sidebar.selectbox(label="First Company", index=0, options=symbol_list)
    ticker2 = st.sidebar.selectbox(label="Second Company", index=1, options=symbol_list)
    
    name = get_name(ticker)
    name2 = get_name(ticker2)
    col1, col2 = st.columns(2)

    with col1:
        financial_data = query_data(ticker, connection)
        chart = bar_chart(financial_data)
        # table = table_chart(financial_data, "#0FA6AB")
        price1, line_chart1 = candlestick_chart(ticker=ticker)

        st.header(f"{name}")
        metric_col1, metric_col2, metric_col3, metric_col4, = st.columns(4)
        ttm_rev, rev_growth, ttm_margin, margin_growth, op_income, op_growth = kpi(financial_data)
        with metric_col1:
            st.metric(label="TTM Revenue (000s)", value=ttm_rev, delta=rev_growth)
        with metric_col2:
            st.metric(label="TTM Margin", value=ttm_margin, delta=margin_growth)
        with metric_col3:
            st.metric(label="TTM Operating Income (000s)", value=op_income, delta=op_growth)
        with metric_col4:
            st.metric(label="Stock Price", value=price1)

        st.plotly_chart(line_chart1, theme=None, use_container_width=True, config={"displayModeBar":False})
        st.plotly_chart(chart, theme=None, use_container_width=True, config={"displayModeBar":False})
        # st.plotly_chart(table, theme=None, use_container_width=True, config={"displayModeBar":False, "staticPlot":True})

    with col2:
        financial_data2 = query_data(ticker2, connection)
        chart2 = bar_chart(financial_data2)
        # table2 = table_chart(financial_data2, "#0070c0")
        price2, line_chart2 = candlestick_chart(ticker=ticker2)

        st.header(f"{name2}")
        second_metric_col1, second_metric_col2, second_metric_col3, second_metric_col4, = st.columns(4)
        ttm_rev2, rev_growth2, ttm_margin2, margin_growth2, op_income2, op_growth2 = kpi(financial_data2)
        with second_metric_col1:
            st.metric(label="TTM Revenue (000s)", value=ttm_rev2, delta=rev_growth2)
        with second_metric_col2:
            st.metric(label="TTM Margin", value=ttm_margin2, delta=margin_growth2)
        with second_metric_col3:
            st.metric(label="TTM Operating Income (000s)", value=op_income2, delta=op_growth2)
        with second_metric_col4:
            st.metric(label="Stock Price", value=price2)

        st.plotly_chart(line_chart2, theme=None, use_container_width=True, config={"displayModeBar":False})
        st.plotly_chart(chart2, theme=None, use_container_width=True, config={"displayModeBar":False})
        # st.plotly_chart(table2, theme=None, use_container_width=True, config={"displayModeBar":False, "staticPlot":True})
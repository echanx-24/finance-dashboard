from dashboard import query_data, bar_chart, kpi, table_chart, get_name, candlestick_chart, donut_chart
import streamlit as st
import sqlite3 as sql

connection = sql.connect("dashboard/financials.db")

if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="Finance Dashboard")
    
    hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
    """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

    st.title("Finance Dashboard")
    st.markdown("Author: <b>Ethan Chan</b>", unsafe_allow_html=True)

    st.divider()

    with st.expander("About Project", expanded=True):
            
            st.markdown(
            """
            This Streamlit Dashboard is a personal project that focuses on aggregating financial data for public companies 
            into a simple and digestable format for end users. Data is taken from the SEC's EDGAR API. 
            I am NOT affiliated with the SEC.

            <b>Disclaimer</b>: the content on this dashboard should be used for informational purposes only and should NOT 
            be used as financial advice. Invest at your own risk.

            Full codebase can be accessed on GitHub: https://github.com/echanx-24/sec-streamlit
            """,
            unsafe_allow_html=True)

    symbol_list = ["DOCN", "AKAM", "SNOW", "DDOG", "PEGA", "PATH", "CRM", "BB", "AMD", "NOW", "MSFT", "SQ", "TSLA",
               "NVDA", "ADBE", "ROKU", "AAPL", "INTC", "GOOGL", "RBLX", "ZM", "U", "PANW", "SNPS", "CRWD",
               "TEAM", "ZS", "VEEV", "MDB", "NET", "PTC", "BSY", "NTNX", "GEN", "TOST", "MSTR", "DOCU", "DBX", "ALTR",
               "FIVN", "WK", "BLKB", "AI", "APPN", "SWI", "BLZE", "OSPN", "ME", "RBLX"]

    about = st.sidebar.markdown("Public Company Selection")
    ticker = st.sidebar.selectbox(label="First Company", index=0, options=symbol_list)
    ticker2 = st.sidebar.selectbox(label="Second Company", index=1, options=symbol_list)
    
    name = get_name(ticker)
    name2 = get_name(ticker2)
    col1, col2 = st.columns(2)

    with col1:
        financial_data = query_data(ticker, connection)
        chart = bar_chart(financial_data, ticker)
        # table = table_chart(financial_data, "#0FA6AB")
        price1, line_chart1 = candlestick_chart(ticker=ticker)
        donut = donut_chart(financial_data, ticker)

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
        st.plotly_chart(donut, theme=None, use_container_width=True, config={"displayModeBar":False})

    with col2:
        financial_data2 = query_data(ticker2, connection)
        chart2 = bar_chart(financial_data2, ticker2)
        # table2 = table_chart(financial_data2, "#0070c0")
        price2, line_chart2 = candlestick_chart(ticker=ticker2)
        donut2 = donut_chart(financial_data2, ticker2)

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
        st.plotly_chart(donut2, theme=None, use_container_width=True, config={"displayModeBar":False})
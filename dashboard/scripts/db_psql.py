import psycopg2
import requests
import pandas as pd

def ticker_data():

    headers = {
    "User-Agent": "echanx24@gmail.com"
    }

    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=headers)
    json_data = response.json()
    df = pd.DataFrame(json_data)
    df = df.T
    df["cik_str"] = df["cik_str"].astype(str).str.zfill(10)
    df.index.name = "ID"
    df = df[["cik_str","title", "ticker"]]
    df_tuples = list(df.itertuples(index=False, name=None))

    return df_tuples

data = ticker_data()

connection = psycopg2.connect(host="localhost", dbname="postgres", user="postgres",
                              password="postgres", port=5432)

cur = connection.cursor()

# cur.execute("DROP TABLE IF EXISTS company CASCADE")

create_table = ('''
        CREATE TABLE IF NOT EXISTS company
            (id SERIAL PRIMARY KEY,
            cik TEXT NOT NULL,
            name TEXT NOT NULL,
            ticker TEXT NOT NULL UNIQUE);''')

# cur.execute(create_table)
# cur.executemany("INSERT INTO company (cik, name, ticker) VALUES (%s,%s,%s)", data)

# cur.execute("DROP TABLE IF EXISTS income_statement_qtr")

profit_table = """
    CREATE TABLE IF NOT EXISTS income_statement_qtr
        (ID SERIAL PRIMARY KEY,
        date TEXT NOT NULL,
        year INT NOT NULL,
        qtr TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        revenue BIGINT NOT NULL,
        costOfRevenue BIGINT NOT NULL,
        grossProfit BIGINT NOT NULL,
        research BIGINT NOT NULL,
        sga BIGINT NOT NULL,
        otherOperating BIGINT NOT NULL,
        operatingExp BIGINT NOT NULL,
        operatingInc BIGINT NOT NULL,
        ticker TEXT NOT NULL,
        FOREIGN KEY (ticker) REFERENCES company (ticker));"""

cur.execute("DROP TABLE IF EXISTS cash_flow_statement CASCADE")

cash_table = """
    CREATE TABLE IF NOT EXISTS cash_flow_statement
        (ID SERIAL PRIMARY KEY,
        date TEXT NOT NULL,
        year INT NOT NULL,
        qtr TEXT NOT NULL,
        operatingCashFlowCum BIGINT NOT NULL,
        operatingCashFlowQtr BIGINT NOT NULL,
        investmentPPECum BIGINT NOT NULL,
        investmentPPEQtr BIGINT NOT NULL,
        freeCashFlow BIGINT NOT NULL,
        ticker TEXT NOT NULL,
        FOREIGN KEY (ticker) REFERENCES company (ticker));"""

# cur.execute(profit_table)
cur.execute(cash_table)

connection.commit()
connection.close()
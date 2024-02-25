import sqlite3 as sql
import requests
import pandas as pd

connection = sql.connect("dashboard/financials.db")
cursor = connection.cursor()


create_table = ('''
        CREATE TABLE IF NOT EXISTS COMPANY
            (ID INTEGER PRIMARY KEY,
            CIK TEXT NOT NULL,
            NAME TEXT NOT NULL,
            TICKER TEXT NOT NULL);''')

# cursor.execute(create_table)
# connection.commit()

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

    cursor.executemany("INSERT INTO company (CIK, NAME, TICKER) VALUES (?,?,?)", df_tuples)
    connection.commit()

# upload_tickers = ticker_data()

cursor.execute("DROP TABLE IF EXISTS income_statement_qtr")

profit_table = '''
    CREATE TABLE IF NOT EXISTS income_statement_qtr
        (ID INTEGER PRIMARY KEY,
        DATE TEXT NOT NULL,
        YEAR TEXT NOT NULL,
        QTR TEXT NOT NULL,
        TIMEFRAME TEXT NOT NULL,
        REVENUE INTEGER NOT NULL,
        COSTOFREVENUE INTEGER NOT NULL,
        GROSSPROFIT INTEGER NOT NULL,
        RESEARCH INTEGER NOT NULL,
        SGA INTEGER NOT NULL,
        OTHEROPERATING INTEGER NOT NULL,
        OPERATINGEXP INTEGER NOT NULL,
        OPERATINGINC INTEGER NOT NULL,
        TICKER TEXT NOT NULL,
        FOREIGN KEY(TICKER) REFERENCES COMPANY(TICKER));'''

cursor.execute(profit_table)
connection.commit()

connection.close()
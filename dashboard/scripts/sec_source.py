import requests
import sqlite3 as sql
import polars as pl

class Stock:

    """

    Functions: get_cik, company_facts, get_account, accounts, insert_data

    """

    headers = {
    "User-Agent": "juggerchan@gmail.com"
    }

    connection = sql.connect("dashboard/financials.db")
    cursor = connection.cursor()

    def __init__(self, ticker):
        self.ticker = ticker

    def get_cik(self, cursor=cursor):
        cursor.execute("SELECT cik FROM COMPANY WHERE ticker=?", (self.ticker,))
        cik = cursor.fetchall()

        if len(cik) > 0:
            cik = cik[0][0]
        else:
            cik = None
        return cik

    def company_facts(self):
        cik = self.get_cik()
        if not cik:
            return
        
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        response = requests.get(url, headers=Stock.headers)
        json_data = response.json()
        return json_data
    
    def get_account(self, account):
        data = self.company_facts()
        df = pl.DataFrame(data["facts"]["us-gaap"][account]["units"]["USD"])
        df = df.select(["start", "end", "fy", "fp", "form", "frame", "val"])
        
        times = [2021, 2022, 2023]
        elements = []
        for time in times:
            df2 = df.filter(
                ((pl.col("form")=="10-K") | (pl.col("form")=="10-Q")) & 
                (pl.col("frame").is_not_null()) &
                (pl.col("frame").str.contains(f"CY{time}"))
            )

            if df2.select(pl.col("form").last()).item() == "10-K":
                fiscal_year = df2.select(pl.col("val").last()).item()
                q4_data = fiscal_year - df2.filter(pl.col("form")=="10-Q").select(pl.col("val").sum()).item()
                append_data = pl.DataFrame(
                    {
                        "start": [df2.select(pl.col("start").last()).item()],
                        "end": [df2.select(pl.col("end").last()).item()],
                        "fy": [df2.select(pl.col("fy").last()).item()],
                        "fp": ["Q4"],
                        "form":["10-Q"],
                        "frame": [f"CY{time}QInput"],
                        "val": [q4_data]
                    }
                )
                
                df3 = pl.concat([df2.filter(pl.col("form")=="10-Q"), append_data, df2.select(pl.all().last())])
                elements.append(df3)
            else:
                elements.append(df2)
            
        df_master = pl.concat([elements[0], elements[1], elements[2]])
        df_master = df_master.rename({"val":account, "fp":"quarter"})

        return df_master.filter(pl.col("form")=="10-Q")
    
    def accounts(self, *accounts):
        data = self.company_facts()
        account_list = list(set(data["facts"]["us-gaap"].keys()))
        account_elements = []
        for index, value in enumerate(account_list):
            for account in accounts:
                if account.upper() in value.upper():
                    if account not in account_elements:
                        account_elements.append({"account":value, "index_location":index})
        df = pl.DataFrame(account_elements)

        return df
    
    def insert_data(self, data, cursor=cursor, connection=connection):
        insert = '''
            INSERT INTO INCOME_STATEMENT_QTR
            (DATE, YEAR, QTR, TIMEFRAME, REVENUE, COSTOFREVENUE, GROSSPROFIT, RESEARCH, 
            SGA, OTHEROPERATING, OPERATINGEXP, OPERATINGINC, TICKER)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        if len(data)>1 and type(data) == list:
            cursor.executemany(insert, data)
            connection.commit()
        else:
            cursor.execute(insert)
            connection.commit()
        return
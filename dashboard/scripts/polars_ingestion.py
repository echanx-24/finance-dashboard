import pandas as pd
from sec_source import Stock
from timeit import default_timer as timer
import polars as pl
from collections import deque

class BlackBerry(Stock):
    
    def __init__(self, ticker="BB"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["Revenues", "CostOfRevenue", "ResearchAndDevelopmentExpense",
                    "SellingGeneralAndAdministrativeExpense", "OperatingExpenses"]
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(OtherOperating = pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(GrossProfit = pl.col("Revenues").sub(pl.col("CostOfRevenue")))
        df = df.with_columns(OperatingIncome = pl.col("GrossProfit").sub(pl.col("OperatingExpenses")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))

        return df.rows()

class UiPath(Stock):

    def __init__(self, ticker="PATH"):
        self.ticker = ticker
    
    def build_table(self):
        accounts = ["GrossProfit", "CostOfRevenue", "ResearchAndDevelopmentExpense", "GeneralAndAdministrativeExpense",
                    "SellingAndMarketingExpense", "OperatingExpenses"]

        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(Revenues=pl.col("CostOfRevenue").add(pl.col("GrossProfit")))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(OperatingIncome=pl.col("GrossProfit").sub(pl.col("OperatingExpenses")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))


        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))
        
        return df.rows()  

class CRM(Stock):

    def __init__(self, ticker="CRM"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["GrossProfit", "CostOfGoodsAndServicesSold", "ResearchAndDevelopmentExpense",
                    "GeneralAndAdministrativeExpense", "SellingAndMarketingExpense", "OperatingExpenses"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(Revenues=pl.col("CostOfGoodsAndServicesSold").add(pl.col("GrossProfit")))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(OperatingIncome=pl.col("GrossProfit").sub(pl.col("OperatingExpenses")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.rename({"CostOfGoodsAndServicesSold":"CostOfRevenue"})

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))

        return df.rows()

class GOOGL(Stock):

    def __init__(self, ticker="GOOGL"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["CostOfRevenue", "CostsAndExpenses", "ResearchAndDevelopmentExpense",
                    "SellingAndMarketingExpense", "GeneralAndAdministrativeExpense",
                    "OperatingIncomeLoss"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OperatingExpenses=pl.col("CostsAndExpenses").sub(pl.col("CostOfRevenue")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(Revenues=pl.col("OperatingIncomeLoss").add(pl.col("CostsAndExpenses")))
        df = df.with_columns(GrossProfit=pl.col("Revenues").sub(pl.col("CostOfRevenue")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.rename({"OperatingIncomeLoss":"OperatingIncome"})

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))

        return df.rows()


class MSFT(Stock):

    def __init__(self, ticker="MSFT"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["GrossProfit", "CostOfGoodsAndServicesSold", "OperatingIncomeLoss",
                    "SellingAndMarketingExpense", "GeneralAndAdministrativeExpense",
                    "ResearchAndDevelopmentExpense"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(Revenues=pl.col("GrossProfit").add(pl.col("CostOfGoodsAndServicesSold")))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OperatingExpenses=pl.col("GrossProfit").sub(pl.col("OperatingIncomeLoss")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.rename({"OperatingIncomeLoss":"OperatingIncome",
                        "CostOfGoodsAndServicesSold":"CostOfRevenue"})

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))
        
        return df.rows()

class ADBE(Stock):

    def __init__(self, ticker="ADBE"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["Revenues", "CostOfRevenue", "GrossProfit", "SellingAndMarketingExpense",
                    "GeneralAndAdministrativeExpense", "ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost",
                    "OperatingIncomeLoss", "OperatingExpenses"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.rename({"OperatingIncomeLoss":"OperatingIncome",
                        "ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost":"ResearchAndDevelopmentExpense"})
        
        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))
        
        return df.rows()

class AMD(Stock):

    def __init__(self, ticker="AMD"):
        self.ticker = ticker
    
    def build_table(self):
        accounts = ["CostOfGoodsAndServicesSold", "GrossProfit", "SellingGeneralAndAdministrativeExpense",
                    "ResearchAndDevelopmentExpense", "OperatingIncomeLoss"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)

        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(Revenues=pl.col("GrossProfit").add(pl.col("CostOfGoodsAndServicesSold")))
        df = df.with_columns(OperatingExpenses=pl.col("GrossProfit").sub(pl.col("OperatingIncomeLoss")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("ResearchAndDevelopmentExpense")).sub(pl.col("SellingGeneralAndAdministrativeExpense")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))

        df = df.rename({"OperatingIncomeLoss":"OperatingIncome",
                        "CostOfGoodsAndServicesSold":"CostOfRevenue"})

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))
        
        return df.rows()


class RBLX(Stock):

    def __init__(self, ticker="RBLX"):
        self.ticker = ticker

    def build_table(self):
        accounts = ["CostOfGoodsAndServicesSold", "ResearchAndDevelopmentExpense", "SellingAndMarketingExpense",
                    "GeneralAndAdministrativeExpense", "CostsAndExpenses", "OperatingIncomeLoss"]
        
        queue = deque()
        for account in accounts:
            data = self.get_account(account)
            queue.appendleft(data)
        
        on = ["start", "end", "fy", "quarter", "form", "frame"]
        df = queue.pop()
        for i in range(len(queue)):
            if len(queue) == 0:
                break
            df = df.join(queue.pop(), on=on)
        
        df = df.with_columns(pl.Series(name="ticker", values=[self.ticker]*len(df.select(pl.col("start")))))
        df = df.with_columns(SellingGeneralAndAdministrativeExpense=pl.col("SellingAndMarketingExpense").add(pl.col("GeneralAndAdministrativeExpense")))
        df = df.with_columns(OperatingExpenses=pl.col("CostsAndExpenses").sub(pl.col("CostOfGoodsAndServicesSold")))
        df = df.with_columns(OtherOperating=pl.col("OperatingExpenses").sub(pl.col("SellingGeneralAndAdministrativeExpense")).sub(pl.col("ResearchAndDevelopmentExpense")))
        df = df.with_columns(Revenues=pl.col("CostsAndExpenses").add(pl.col("OperatingIncomeLoss")))
        df = df.with_columns(GrossProfit=pl.col("Revenues").sub(pl.col("CostOfGoodsAndServicesSold")))
        df = df.with_columns(date = pl.col("end").str.to_date("%Y-%m-%d"))
        df = df.with_columns(year = pl.col("date").dt.year())
        df = df.with_columns(pl.col("date").dt.strftime("%b-%Y"))
        df = df.with_columns(pl.Series(name="timeframe",
                                   values=[str(i[0]) + " " + str(j[0]) for i, j in zip(df.select(pl.col("year")).rows(),
                                                                                df.select(pl.col("quarter")).rows())]))
        
        df = df.rename({"OperatingIncomeLoss":"OperatingIncome",
                        "CostOfGoodsAndServicesSold":"CostOfRevenue"})

        df = df.select(pl.col(["date", "year", "quarter", "timeframe", "Revenues",
                           "CostOfRevenue", "GrossProfit","ResearchAndDevelopmentExpense",
                           "SellingGeneralAndAdministrativeExpense","OtherOperating",
                           "OperatingExpenses", "OperatingIncome", "ticker"]))
        
        return df.rows()

symbol_list = ["PEGA", "PATH", "CRM", "BB", "SNOW", "DDOG", "AKAM", "DOCN", "AMD", "NOW", "MSFT", "SQ", "TSLA",
               "NVDA", "ADBE", "ROKU", "AAPL", "INTC", "GOOGL", "RBLX", "ZM", "U", "PANW", "SNPS", "CRWD",
               "TEAM", "ZS", "VEEV", "MDB", "NET", "PTC", "BSY", "NTNX", "GEN", "TOST", "MSTR", "DOCU", "DBX", "ALTR",
               "FIVN", "WK", "BLKB", "AI", "APPN", "SWI", "BLZE", "OSPN", "ME", "RBLX"]

import_list = ["RBLX", "ZM", "U", "DKNG", "SOFI", "HOOD", "SHOP", "ORCL", "SAP", "INTU", "PANW","ADP",
               "SNPS", "CRWD", "TEAM", "ZS", "VEEV", "MDB", "NET", "PTC", "NFLX", "LDOS", "BSY",
               "SGE.L", "NTNX", "GEN", "TOST", "MSTR", "DAY", "MNDY", "CYBR", "PAYC", "DOCU", "DBX", "ALTR",
               "FIVN", "WK", "BLKB", "AI", "APPN", "SWI", "OPRA", "WKME", "SMRT", "BLZE", "OSPN", "ME"]

def determine(tickers):
    elements_found = []
    elements_error = []
    for ticker in tickers:
        # if ticker in symbol_list:
        #     continue
        method1 = BlackBerry(ticker=ticker)
        method2 = UiPath(ticker=ticker)
        method3 = CRM(ticker=ticker)
        method4 = GOOGL(ticker=ticker)
        method5 = MSFT(ticker=ticker)
        method6 = AMD(ticker=ticker)
        method7 = ADBE(ticker=ticker)
        method8 = RBLX(ticker=ticker)
        try:
            data = method1.build_table()
            method1.insert_data(data)
            elements_found.append((ticker, "BB Method"))
            # symbol_list.append(ticker)
        except:
            try:
                data = method2.build_table()
                method2.insert_data(data)
                elements_found.append((ticker, "PATH Method"))
                # symbol_list.append(ticker)
            except:
                try:
                    data = method3.build_table()
                    method3.insert_data(data)
                    elements_found.append((ticker, "CRM Method"))
                    # symbol_list.append(ticker)
                except:
                    try:
                        data = method4.build_table()
                        method4.insert_data(data)
                        elements_found.append((ticker, "GOOGL Method"))
                        # symbol_list.append(ticker)
                    except:
                        try:
                            data = method5.build_table()
                            method5.insert_data(data)
                            elements_found.append((ticker, "MSFT Method"))
                            # symbol_list.append(ticker)
                        except:
                            try:
                                data = method6.build_table()
                                method6.insert_data(data)
                                elements_found.append((ticker, "AMD Method"))
                                # symbol_list.append(ticker)
                            except:
                                try:
                                    data = method7.build_table()
                                    method7.insert_data(data)
                                    elements_found.append((ticker, "ADBE Method"))
                                    # symbol_list.append(ticker)
                                except:
                                    try:
                                        data = method8.build_table()
                                        method8.insert_data(data)
                                        elements_found.append((ticker, "RBLX Method"))
                                    except:
                                        elements_error.append(ticker)

    return elements_found, elements_error

# start = timer()
# check = determine(tickers=symbol_list)

# end = timer()
# print("Success:", check[0])
# print()
# print("Failure:", check[1])
# print("Time:", end-start)

# data["facts"]["dei"] Common Stock Shares Outstanding

akam = GOOGL(ticker="AKAM")
data = akam.company_facts()
df = pl.DataFrame(data["facts"]["us-gaap"]["CostOfRevenue"]["units"]["USD"])
# df = df.filter(pl.col("form")=="10-K")
print(df)

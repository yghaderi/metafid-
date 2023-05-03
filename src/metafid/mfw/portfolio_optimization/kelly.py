import datetime
import pandas as pd

from ..db_psycopg import DB
from ...portfolio_optimization.kelly import Kelly


class KellyMFW:
    def __init__(self, dbname: str, user: str, pass_: str, tickers: tuple, time_frame, window, start_date, end_date,
                 short_selling: bool = False) -> None:
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.df = self.db.join_and_query_where(table1="tsedata_histprice", cols="date_id,symbol_far,final",
                                               join_on_col_t1="symbol_id", table2="tsedata_ticker",
                                               join_on_col_t2="symbol",
                                               where=f"symbol_far in {tickers} AND date_id BETWEEN '{start_date}' and '{end_date}'")
        df = self.df.copy()
        df = df.rename(columns={"date_id":"date"})
        df = df.pivot(columns="symbol_far", values="final", index="date").rename_axis(None, axis=1)
        df.reset_index(inplace=True)
        df["date"] = df["date"].apply(pd.to_datetime)
        self.kelly = Kelly(df=df, time_frame=time_frame, window=window)
        self.short_selling = short_selling

    def kelly_return(self):
        kelly_portfolio_return = self.kelly.kelly_portfolio_return(
            short_selling=self.short_selling).reset_index()
        equal_ratio_portfolio_return = self.kelly.equal_weight_portfolio_return().reset_index()
        return kelly_portfolio_return.merge(equal_ratio_portfolio_return, on="date", how="inner")

    def kelly_weight(self):
        if self.short_selling:
            kelly_allocation = self.kelly.kelly_allocation()
        else:
            kelly_allocation = self.kelly.kelly_allocation()
            kelly_allocation[kelly_allocation < 0] = 0
        kelly_weight = kelly_allocation / kelly_allocation.sum()
        kelly_weight = kelly_weight.to_frame("weight").reset_index().rename(columns={"index": "ticker"})
        kelly_allocation = kelly_allocation.to_frame("allocation").reset_index().rename(columns={"index": "ticker"})
        return kelly_weight.merge(kelly_allocation, on="ticker", how="inner")

    def do_job(self):
        self.db.drop_all(table="po_kellyallocation")
        self.db.insert_data(table="po_kellyallocation", df=self.kelly_weight())

        self.db.drop_all(table="po_kellyreturn")
        self.db.insert_data(table="po_kellyreturn", df=self.kelly_return())

        print(
            f'Drop all "kellyallocation" and "kellyreturn" records and insert new data! The time is: {datetime.datetime.now()}\n',
            end="\r")

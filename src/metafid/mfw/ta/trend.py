import pandas as pd
import numpy as np
import datetime

from ...ta import Trend
from ..db_psycopg import DB


class TrendMFW:
    def __init__(self,dbname: str, user: str, pass_: str, trend_table:str,pct_mm_priority_table:str,  order:int=8,periods:list=[7,30,60,90]) -> None:
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.trend_table = trend_table
        self.order = order
        self.periods = periods
        self.pct_mm_priority_table = pct_mm_priority_table
        self.df = self.db.join_and_query_all(table1="tsedata_histprice", cols="date_id,final,symbol_far",
                                       join_on_col_t1="symbol_id", table2="tsedata_ticker", join_on_col_t2="symbol")

    def trend(self):
        groups = self.df.groupby("symbol_far")
        trend_df = pd.DataFrame()
        for name, group in groups:
            group = group.reset_index().drop("index", axis=1)
            trend_df = pd.concat([trend_df, Trend(date=group.date_id, ticker=group.symbol_far, final=group.final.values,
                                                  order=self.order).trend()])
        trend_df["peak"] = trend_df["peak"] * trend_df["final"]
        trend_df["trough"] = trend_df["trough"] * trend_df["final"]
        trend_df = trend_df.replace({np.NAN: None})
        self.db.drop_all(table=self.trend_table)
        self.db.insert_data(table=self.trend_table, df=trend_df)
        print(f'Drop all {self.trend_table} records and insert new data! The time is: {datetime.datetime.now()}\n', end="\r")

    def pct_mm_priority(self):
        all_df = pd.DataFrame()
        for p in self.periods:
            date = datetime.datetime.today() - datetime.timedelta(days=p)
            df = self.db.join_and_query_where(table1="tsedata_histprice", cols="date_id,final,symbol_far", join_on_col_t1="symbol_id", table2="tsedata_ticker", join_on_col_t2="symbol",  where=f"date_id >='{date}'")
            trend = Trend(date=df.date_id, ticker=df.symbol_far, final=df.final)
            pct_df = trend.pct_mm_priority(period=p)
            if len(all_df):
                all_df = all_df.merge(pct_df, on="ticker", how="left")
            else:
                all_df = pd.concat([all_df,pct_df])
        all_df = all_df.assign(date=datetime.datetime.today())
        self.db.drop_all(table=self.pct_mm_priority_table)
        self.db.insert_data(table=self.pct_mm_priority_table, df=all_df)
        print(f'Drop all {self.pct_mm_priority_table} records and insert new data! The time is: {datetime.datetime.now()}\n', end="\r")


    def do_job(self):
        self.trend()
        self.pct_mm_priority()





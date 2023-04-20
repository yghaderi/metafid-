import pandas as pd
import numpy as np
from datetime import datetime

from ...ta import Trend
from ..db_psycopg import DB


class TrendMFW:
    def __init__(self,dbname: str, user: str, pass_: str, trend_table:str, order:int=8) -> None:
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.trend_table = trend_table
        self.order = order
        self.df = self.db.join_and_query_all(table1="tsedata_histprice", cols="date_id,final,symbol_far",
                                       join_on_col_t1="symbol_id", table2="tsedata_ticker", join_on_col_t2="symbol")

    def do_job(self):
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
        print(f'Drop all {self.trend_table} records and insert new data! The time is: {datetime.now()}\n', end="\r")



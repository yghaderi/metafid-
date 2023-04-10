import numpy as np
from datetime import datetime
import time
import os

from apscheduler.schedulers.background import BackgroundScheduler

from ...deriv.strategy import OptionStrategy
from ...deriv.option import Pricing
from ..db_psycopg import DB
from ...data.tsetmc import TSETMC

drop_cols = ["isin", "time", "open", "close", "no", "volume", "low", "high", "y_final", "eps", "base_vol", "unknown1",
             "unknown2", "sector", "day_ul", "day_ll", "share_no", "mkt_id", "sell_no", "buy_no"]


class OptionStrategyMFW:
    def __init__(self, dbname: str, user: str, pass_: str, ua_table: str, ostg_table: str, pct_daily_cp:float, interval: int):
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.ua = self.db.query_all(table=ua_table, cols="ua,sigma")
        self.mw = TSETMC(drop_cols=drop_cols)
        self.omw = self.mw.option_mv(ua=self.ua.ua)
        self.call = None
        self.put = None
        self.call_put = None
        self.omw_df = self.omw.merge(self.ua, on="ua", how="inner")
        self.pct_daily_cp = pct_daily_cp
        self.interval = interval
        self.ostg_table = ostg_table

    def data(self):
        df = self.omw.merge(self.ua, on="ua", how="inner")

        pricing = Pricing()
        df["bs"] = df.apply(
            lambda x: pricing.black_scholes(s_0=x["ua_final"], k=x["strike_price"], t=x["t"], sigma=float(x["sigma"]),
                                            type_=x["type"]), axis=1)
        df["bs"] = df.bs.fillna(0).apply(lambda x: np.nan if x == np.nan else int(x))

        self.call = df[df.type == "call"]
        self.put = df[df.type == "put"]

        def same_strike_and_ex_date_on_call_put(call, put):
            cols = [i for i in call.columns if not i.startswith("ua_") and i not in ["t", "sigma", "dt", "type"]]
            def cols_(x):
                if (x.endswith("_x")) and (x.startswith("ua_")):
                    return x.replace("_x","")
                elif (x.endswith("_x")):
                    return "call_" + x.replace("_x","")
                elif (x.endswith("_y")):
                    return "put_" + x.replace("_y","")
                else:
                    return x
            df = call.merge(put[cols], on=["ua", "strike_price", "ex_date"], how="inner")
            df.columns = list(map(cols_, df.columns))
            return df
        self.call_put = same_strike_and_ex_date_on_call_put(call=self.call, put=self.put)
        return self

    def option_strategy(self):
        data = self.data()
        ostg = OptionStrategy(call=data.call, put=data.put, call_put=data.call_put, pct_daily_cp=self.pct_daily_cp)
        return ostg.all_strategy()

    def job(self):
        self.db.drop_all(table=self.ostg_table)
        self.db.insert_data(table=self.ostg_table, df=self.option_strategy())
        print(f'Drop all {self.ostg_table} records and insert new data! The time is: {datetime.now()}', end="\r")

    def do_job(self):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.job, 'interval', seconds=self.interval)
        scheduler.start()
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()

import numpy as np
from datetime import datetime
import time
import os

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

from ...deriv.strategy import OptionStrategy
from ...deriv.option import Pricing
from ..db_psycopg import DB
from ...data.tsetmc.tsetmc import MarketWatch


class OptionStrategyMFW:
    def __init__(self, dbname: str, user: str, pass_: str, omw: pd.DataFrame, pct_monthly_cp: float,
                 interval: int):
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.sigma = self.db.query_all(table="derivs_sigma", cols="*").rename(columns={"ua": "ua_symbol"})
        self.omw = omw
        self.pct_monthly_cp = pct_monthly_cp
        self.interval = interval

    def data(self):
        df = self.omw.merge(self.sigma, on="ua_symbol", how="inner")
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
                    return x.replace("_x", "")
                elif (x.endswith("_x")):
                    return "call_" + x.replace("_x", "")
                elif (x.endswith("_y")):
                    return "put_" + x.replace("_y", "")
                else:
                    return x

            df = call.merge(put[cols], on=["ua", "strike_price", "ex_date"], how="inner")
            df.columns = list(map(cols_, df.columns))
            return df

        self.call_put = same_strike_and_ex_date_on_call_put(call=self.call, put=self.put)
        return self

    def option_strategy(self, data):
        return OptionStrategy(call=data.call, put=data.put, call_put=data.call_put, pct_monthly_cp=self.pct_monthly_cp)

    def drop_and_insert(self, table: str, df):
        try:
            self.db.drop_all(table=table)
            self.db.insert_data(table=table, df=df)
            print(f"{datetime.now()}: Drop all '{table}' records and insert new data!", end="\r")
        except:
            print(f"Have a problem in the '{table}'!")

    def job(self):
        ostg = self.option_strategy(self.data())
        self.drop_and_insert(table="derivs_coveredcall", df=ostg.covered_call())
        self.drop_and_insert(table="derivs_marriedput", df=ostg.married_put())

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




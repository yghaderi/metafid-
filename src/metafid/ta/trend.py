from scipy.signal import argrelextrema
import numpy as np
import pandas as pd
class Trend:
    def __init__(self, date, close, order: int = 10):
        self.date = date
        self.close = close
        self.order = order
        self.df = pd.DataFrame({"date": self.date, "close": self.close})

    def peak_trough(self):
        max_idx = argrelextrema(self.close, np.greater_equal, order=self.order)[0]
        min_idx = argrelextrema(self.close, np.less_equal, order=self.order)[0]
        for i in max_idx:
            self.df.loc[i, "peak"] = 1
        for i in min_idx:
            self.df.loc[i, "trough"] = 1
        return self.df

    def separationـpeak_trough(self):
        """
        true: Higher High
        false: Lower High
        """
        df = self.peak_trough()
        df_peak = df.dropna(subset="peak")
        df_trough = df.dropna(subset="trough")
        df_peak = df_peak.assign(hh_lh=df_peak.close > df_peak.close.shift(1))[["date", "hh_lh"]]
        df_trough = df_trough.assign(ll_hl=df_trough.close < df_trough.close.shift(1))[["date", "ll_hl"]]
        df = df.merge(df_peak, on="date", how="left")
        df = df.merge(df_trough, on="date", how="left")
        return df

    def bull_trend(self):
        """
        An uptrend is defined primarily as successively higher peaks and troughs.
        :return:
        """
        df = self.separationـpeak_trough()
        df["hh_lh"] = df.hh_lh.fillna(method="bfill")
        df["ll_hl"] = df.ll_hl.fillna(method="bfill")

        # df = df.assign(trend = df.hh_lh != df.ll_hl)

        def test(hh_lh, ll_hl):
            if hh_lh:
                if not ll_hl:
                    return 1
                return 0
            elif ll_hl:
                if not hh_lh:
                    return -1
                return 0
            return 0

        df["trend"] = df.apply(lambda r: test(hh_lh=r["hh_lh"], ll_hl=r["ll_hl"]), axis=1)
        return df

    def bear_trend(self):
        """
        A downtrend is defined as successively lower peaks and troughs.
        :return:
        """
        pass
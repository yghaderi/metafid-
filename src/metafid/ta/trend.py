from scipy.signal import argrelextrema
import numpy as np
import pandas as pd


class Trend:
    def __init__(self, date,ticker, final, order: int = 8):
        self.date = date
        self.final = final
        self.order = order
        self.ticker = ticker
        self.df = pd.DataFrame({"date": self.date,"ticker":self.ticker, "final": self.final})

    def peak_trough(self):
        """
        Recognise local min and max.
        :return: index of local min-max
        """
        max_idx = argrelextrema(self.final, np.greater_equal, order=self.order)[0]
        min_idx = argrelextrema(self.final, np.less_equal, order=self.order)[0]
        for i in max_idx:
            self.df.loc[i, "peak"] = 1
        for i in min_idx:
            self.df.loc[i, "trough"] = 1
        return self.df

    def separation_peak_trough(self):
        """
        It compares the position of the peak and trough compared to the previous one.
        In hh_lh where True indicate Higher High and False indicate Lower High.
        In ll_hl where True indicate Lower Low and False indicate Higher Low.
        :return: hh_lh and ll_hl columns.
        """
        df = self.peak_trough()
        df_peak = df.dropna(subset="peak")
        df_trough = df.dropna(subset="trough")
        df_peak = df_peak.assign(hh_lh=df_peak.final > df_peak.final.shift(1))[["date", "hh_lh"]]
        df_trough = df_trough.assign(ll_hl=df_trough.final < df_trough.final.shift(1))[["date", "ll_hl"]]
        df = df.merge(df_peak, on="date", how="left")
        df = df.merge(df_trough, on="date", how="left")
        return df

    def trend(self):
        """
        An uptrend is defined primarily as successively higher peaks and troughs.
        A downtrend is defined as successively lower peaks and troughs.
        :return: The trend column, where 1 indicates an uptrend, -1 a downtrend, and 0 a trend-less.
        """
        df = self.separation_peak_trough()
        df["hh_lh"] = df.hh_lh.fillna(method="bfill").fillna(method="ffill")
        df["ll_hl"] = df.ll_hl.fillna(method="bfill").fillna(method="ffill")

        # df = df.assign(trend = df.hh_lh != df.ll_hl)

        def recognize_trend(hh_lh, ll_hl):
            if hh_lh:
                if not ll_hl:
                    return 1
                return 0
            elif ll_hl:
                if not hh_lh:
                    return -1
                return 0
            return 0

        df["trend"] = df.apply(lambda r: recognize_trend(hh_lh=r["hh_lh"], ll_hl=r["ll_hl"]), axis=1)
        return df

    def pct_mm_priority(self, period):
        groups = self.df.groupby(by="ticker")
        df = pd.DataFrame()
        for name, group in groups:
            group.set_index("date", inplace=True)
            min_max = pd.DataFrame([{"ticker": name,
                                     "max_date": group.final.idxmax(),
                                     "max_price": group.final.max(),
                                     "min_date": group.final.idxmin(),
                                     "min_price": group.final.min(),
                                     "last_price": group.final.values[-1]
                                     }])
            df = pd.concat([df, min_max])
        df = df.assign(priority=df.max_date > df.min_date)
        df[f"pct_mm_{period}"] = df.apply(
            lambda x: (x["max_price"] / x["min_price"] - 1 if x["priority"] else x["min_price"] / x["max_price"] - 1)*100,
            axis=1).round(1)
        df[f"pct_last_{period}"] = df.apply(
            lambda x: (x["last_price"] / x["max_price"] - 1 if x["priority"] else x["last_price"] / x[
                "min_price"] - 1)*100, axis=1).round(1)
        return df[["ticker", f"pct_mm_{period}", f"pct_last_{period}"]]


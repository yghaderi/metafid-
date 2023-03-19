import numpy as np
import pandas as pd
from operator import mul, truediv, add
from scipy.stats import norm
import finpy_tse as fpy
import jdatetime as jdt


class Pricing:
    def __init__(self, r: float = 0.25):
        self.N = 252
        self.Y = 365
        self.today = str(jdt.date.today())
        self.ago_700 = str(jdt.date.today() - jdt.timedelta(days=700))
        self.r = r

    def black_scholes(
        self, s_0: float, k: float, t: float, sigma: float, type_: str, div: float = 0
    ):
        """
        Pricing European options for Tehran Stuck Exchange.
        We use Monte Carlo simulations(MCS) and Black-Scholes(BS) models for pricing.
        BS: C= N(d_1)S_t - N(d_2)Ke^(-rt)
            where d_1 = (ln(S_t/K)+(r+sigma^2/2))/sigma*t^(1/2)
            adn d_2 = d_1 - sigma*t^(1/2)
                C	=	call deriv price
                N	=	CDF of the normal distribution
                S_t	=	spot price of an asset
                K	=	strike price
                r	=	risk-free interest rate
                t	=	time to maturity
                sigma	=	volatility of the asset
        :return:
        """
        sigma = mul(sigma, np.sqrt(self.N))
        t = truediv(t, self.Y)

        s = s_0 - div / pow((1 + self.r), t) if type_ == "call" else s_0

        d_1 = add(np.log(s / k), (self.r + mul(truediv(pow(sigma, 2), 2), t))) / (
            sigma * np.sqrt(t)
        )
        d_2 = d_1 - sigma * np.sqrt(t)

        if type_ == "call":
            n_d_1 = norm.cdf(d_1, 0, 1)
            n_d_2 = norm.cdf(d_2, 0, 1)
            val = s * n_d_1 - k * np.exp(-self.r * t) * n_d_2
            return val
        elif type_ == "put":
            n_d_1 = norm.cdf(-d_1, 0, 1)
            n_d_2 = norm.cdf(-d_2, 0, 1)
            val = k * np.exp(-self.r * t) * n_d_2 - s * n_d_1
            return val

    def sigma(self, hist: pd.DataFrame):
        # calc sigma
        grouped = hist.groupby(by="ticker")
        df = pd.DataFrame()
        for name, group in grouped:
            length = 90 if truediv(len(group), 1.6) >= 90 else truediv(len(group), 1.6)
            group = group.assign(log_return=np.log(1 + group.adj_final.pct_change()))
            group = group.assign(sigma=group.log_return.rolling(length).std())
            df = pd.concat([df, group])
        return df

    def hist(self, data: pd.DataFrame):
        def day_to_ex(ex_date, hist_date):
            exd = ex_date.split("-")
            hd = hist_date.split("-")
            t = jdt.date(int(exd[0]), int(exd[1]), int(exd[2])) - jdt.date(
                int(hd[0]), int(hd[1]), int(hd[2])
            )
            return t.days

        ticker_hist = pd.DataFrame()
        for ticker in data.ticker.unique():
            t_df = (
                fpy.Get_Price_History(
                    stock=ticker,
                    start_date=self.ago_700,
                    end_date=self.today,
                    ignore_date=False,
                    adjust_price=True,
                    show_weekday=False,
                    double_date=True,
                )
                .reset_index()
                .rename(
                    columns=lambda x: str.lower(x.replace(" ", "_").replace("-", "_"))
                )
            )
            length = 90 if truediv(len(t_df), 1.6) >= 90 else truediv(len(t_df), 1.6)
            t_df = t_df.assign(log_return=np.log(1 + t_df.adj_final.pct_change()))
            t_df = t_df.assign(sigma=t_df.log_return.rolling(length).std())
            ticker_hist = pd.concat([ticker_hist, t_df])

        option_hist = pd.DataFrame()
        for ticker in data.o_ticker.unique():
            try:
                o_df = (
                    fpy.Get_Price_History(
                        stock=ticker,
                        start_date=self.ago_700,
                        end_date=self.today,
                        ignore_date=False,
                        adjust_price=True,
                        show_weekday=False,
                        double_date=True,
                    )
                    .rename(columns=lambda x: "o_" + str.lower(x.replace(" ", "_")))
                    .rename(columns={"o_date": "date"})
                )
                option_hist = pd.concat([option_hist, o_df])
            except:
                print(f" داده‌ای برایِ «{ticker}» وجود ندارد", end="\r")
        df = ticker_hist.merge(data, on="ticker", how="left")
        df = df.merge(option_hist, on=["date", "o_ticker"], how="left")
        df = df[df.o_adj_final > 0]
        df["t"] = df.apply(
            lambda x: day_to_ex(ex_date=x["ex_date"], hist_date=x["j_date"]), axis=1
        )

        return df

    def bs(self, data: pd.DataFrame):
        """
        Pricing European options for Tehran Stuck Exchange base on Black-Scholes(BS).
        :param data: its pandas dataframe with ["ticker", "o_ticker", "ex_date", "type", "strike_price"] columns.
        :return: pandas dataframe
        """
        df = self.hist(data=data)
        df["bs"] = df.apply(
            lambda x: self.black_scholes(
                s_0=x["adj_final"],
                k=x["strike_price"],
                t=x["t"],
                type_=x["type"],
                sigma=x["sigma"],
            ),
            axis=1,
        )
        return df

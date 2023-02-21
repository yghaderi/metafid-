import numpy as np
import pandas as pd
from scipy.stats import norm


class Pricing:
    def pricing(r: float, type: str, n: int = 250, n_sim: int = 10 ** 6):
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
        pass

    def black_scholes(self, s_0: float, div: float, k: float, t: float, r: float, sigma: float, type: str):

        n = 252
        if type == "call":
            s = s_0 - div / pow((1 + r), t)
        elif type == "put":
            s = s_0

        d_1 = (
                np.log(s / k) + (r + pow(sigma, 2) / 2 * t) / (sigma * np.sqrt(t))
        )
        d_2 = d_1 - sigma * np.sqrt(t)
        if type == "call":
            n_d_1 = norm.cdf(d_1, 0, 1)
            n_d_2 = norm.cdf(d_2, 0, 1)
            val = s * n_d_1 - k * np.exp(-r * t) * n_d_2
            return val
        elif type == "put":
            n_d_1 = norm.cdf(-d_1, 0, 1)
            n_d_2 = norm.cdf(-d_2, 0, 1)
            val = k * np.exp(-r * t) * n_d_2 - s * n_d_1
            return val


def sigma(hist: pd.DataFrame, period: int):
    # calc sigma
    grouped = hist.groupby(by="ticker")
    df = pd.DataFrame()
    for name, group in grouped:
        group = group.assign(log_return=np.log(1 + group.close.pct_change()))
        group = group.assign(sigma=group.log_return.rolling(period).std())
        df = pd.concat([df, group])
    return df

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

    def sigma(self, price: pd.Series):
        # calc sigma
        pct_change = np.log(1 + price.pct_change())
        return pct_change.std()

import numpy as np
from scipy.stats import norm


def pricing(r: float, type: str, n: int = 250, n_sim: int = 10 ** 6):
    """
    Pricing European options for Tehran Stuck Exchange.
    We use Monte Carlo simulations(MCS) and Black-Scholes(BS) models for pricing.
    BS: C= N(d_1)S_t - N(d_2)Ke^(-rt)
        where d_1 = (ln(S_t/K)+(r+sigma^2/2))/sigma*t^(1/2)
        adn d_2 = d_1 - sigma*t^(1/2)
            C	=	call option price
            N	=	CDF of the normal distribution
            S_t	=	spot price of an asset
            K	=	strike price
            r	=	risk-free interest rate
            t	=	time to maturity
            sigma	=	volatility of the asset
    :return:
    """
    pass

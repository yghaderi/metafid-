import numpy as np
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

    def black_scholes(self, S_0, K, T, r, sigma, type):
        d_1 = (
                np.log(S_0 / K) + (r + pow(sigma, 2) / 2 * T) / (sigma * np.sqrt(T))
        )
        d_2 = d_1 - sigma * np.sqrt(T)
        if type == "call":
            N_d_1 = norm.cdf(d_1, 0, 1)
            N_d_2 = norm.cdf(d_2, 0, 1)
            val = S_0 * N_d_1 - K * np.exp(-r * T) * N_d_2
            return val
        elif type == "put":
            N_d_1 = norm.cdf(-d_1, 0, 1)
            N_d_2 = norm.cdf(-d_2, 0, 1)
            val = K * np.exp(-r * T) * N_d_2 - S_0 * N_d_1
            return val

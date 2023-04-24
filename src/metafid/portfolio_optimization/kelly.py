import warnings

warnings.filterwarnings('ignore')

import numpy as np
from numpy.linalg import inv
from numpy.random import dirichlet
import pandas as pd

from sympy import symbols, solve, log, diff
from scipy.optimize import minimize_scalar, newton, minimize
from scipy.integrate import quad
from scipy.stats import norm


class Kelly:
    def __init__(self, df) -> None:
        self.df = df.set_index("date")  # columns = [date,close_ticker_1,close_ticker_2 ...]
        self.share, self.odds, self.probability = symbols('share odds probability')
        self.Value = self.probability * log(1 + self.odds * self.share) + (1 - self.probability) * log(1 - self.share)
        solve(diff(self.Value, self.share), self.share)

        self.f, self.p = symbols('f p')
        self.y = self.p * log(1 + self.f) + (1 - self.p) * log(1 - self.f)
        solve(diff(self.y, self.f), self.f)

    def calc_return(self):
        return self.df.resample('2m').last().pct_change().dropna()

    def return_params(self):
        return self.calc_return().TEDPIX.rolling(4).agg(['mean', 'std']).dropna()

    def norm_integral(self, f, mean, std):
        val, er = quad(lambda s: np.log(1 + f * s) * norm.pdf(s, mean, std),
                       mean - 3 * std,
                       mean + 3 * std)
        return -val

    def norm_dev_integral(self, f, mean, std):
        val, er = quad(lambda s: (s / (1 + f * s)) * norm.pdf(s, mean, std), mean - 3 * std, mean + 3 * std)
        return val

    def get_kelly_share(self, data):
        solution = minimize_scalar(self.norm_integral,
                                   args=(data['mean'], data['std']),
                                   bounds=[0, 2],
                                   method='bounded')
        return solution.x

    def f_single_asset(self):
        return self.calc_return().assign(f=self.return_params().apply(self.get_kelly_share, axis=1))

    # Kelly Rule for Multiple Assets
    def cov_return(self):
        cov = self.calc_return().cov()
        return pd.DataFrame(inv(cov), index=self.df.columns, columns=self.df.columns)

    def kelly_allocation(self):
        return self.calc_return().mean().dot(self.cov_return())

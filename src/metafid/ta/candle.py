import numpy as np
from scipy import stats


class ReCandle:

    def heikin_ashi(self, close, open, high, low):
        """
        Heikin-Ashi Candlestick.
        :param close:
        :param open:
        :param high:
        :param low:
        :return: Heikin-Ashi close-open-high-low.
        """
        self.ha_close = np.mean([close, low, high, open], axis=0)
        self.ha_open = np.mean([self.ha_close, open], axis=0)
        for i in range(1, len(open)):
            self.ha_open[i] = np.mean([self.ha_close[i - 1], self.ha_open[i - 1]])
        self.ha_high = np.max([self.ha_close, self.ha_open, high], axis=0)
        self.ha_low = np.min([self.ha_close, self.ha_open, low], axis=0)
        return self

    def linear_regression(self, y, length: int):
        """
        Linear_Regression Candlestick.
        :param y:
        :param length: int
            Number of previous data to fit on LR
        :return: Liner Regression open-high-low-close
        """
        x = np.array(range(length))
        self.y_hat = np.tile(np.nan, length)
        for i in range(len(y) - length):
            y = y[i:i + length]
            slope, intercept, r, p, std_err = stats.linregress(x, y)
            self.y_hat[i + length - 1] = intercept + slope * (length - 1)
        return self

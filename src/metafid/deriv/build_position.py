class BuildPosition:
    def __init__(self, long_call, short_call, long_put, short_put, long_ua, short_ua, s_t_range: list, ):
        """
        strike price = k
        :param long_call: list of dict long call position params e.g. [{"k":22_000, "premium":2_000, "qty":2}, ...]
        :param short_call:list of dict short call position params e.g. [{"k":22_000, "premium":2_000, "qty":2}, ...]
        :param long_put:list of dict long put position params e.g. [{"k":22_000, "premium":2_000, "qty":3}, ...]
        :param short_put:list of dict short put position params e.g. [{"k":22_000, "premium":2_000, "qty":2}, ...]
        :param long_ua:list of dict long underlying asset position params e.g. [{"splot_price":18_000, "qty":1}, ...]
        :param short_ua: list of dict short underlying asset  position params e.g. [{"splot_price":18_000, "qty":1}, ...]
        :param s_t_range: list of price of assets at maturity

        >> positions = {
            "long_call":[],
            "short_call":[{"k":22_000, "premium":2_000, "qty":2}, {"k":24_000, "premium":1_000, "qty":1}],
            "long_put":[],
            "short_put":[],
            "long_ua":["splot_price":20_000, "qty":3],
            "short_ua":[],
            }
        >>s_t_range = range(15_000, 30_000, 10)
        >>bp = BuildPosition(**positions, s_t_range = s_t_range)
        >>print(bp.simulate_profit())
        [-10000, -9970, ...]
        """
        self.lc = long_call
        self.sc = short_call
        self.lp = long_put
        self.sp = short_put
        self.lua = long_ua
        self.sua = short_ua
        self.s_t_range = s_t_range

    @staticmethod
    def long_call(s_t, k, premium, qty):
        return (max(s_t - k, 0) - premium) * qty

    @staticmethod
    def short_call(s_t, k, premium, qty):
        return (-max(s_t - k, 0) + premium) * qty

    @staticmethod
    def long_put(s_t, k, premium, qty):
        return (max(k - s_t, 0) - premium) * qty

    @staticmethod
    def short_put(s_t, k, premium, qty):
        return (-max(k - s_t, 0) + premium) * qty

    @staticmethod
    def long_ua(spot_price, s_t, qty):
        return (s_t - spot_price) * qty

    @staticmethod
    def short_ua(spot_price, s_t, qty):
        return (spot_price - s_t) * qty

    def simulate_profit(self):
        sim_profit = []
        for i in self.s_t_range:
            profit = 0
            profit += sum([self.long_call(s_t=i, **items) for items in self.lc])
            profit += sum([self.short_call(s_t=i, **items) for items in self.sc])
            profit += sum([self.long_put(s_t=i, **items) for items in self.lp])
            profit += sum([self.short_put(s_t=i, **items) for items in self.sp])
            profit += sum([self.long_ua(s_t=i, **items) for items in self.lua])
            profit += sum([self.short_ua(s_t=i, **items) for items in self.sua])
            sim_profit.append(profit)
        return sim_profit

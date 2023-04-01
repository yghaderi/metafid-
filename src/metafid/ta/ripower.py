import finpy_tse as fpy


class RetailInstitutionalPower:

    def __init__(self, ticker, start_date, end_date):
        self.ticker = ticker
        self.start_date = start_date
        self.end_date = end_date

    def get_price(self):
        price_data = fpy.Get_Price_History(
            stock=self.ticker,
            start_date=self.start_date,
            end_date=self.end_date,
            ignore_date=False,
            adjust_price=True,
            show_weekday=False,
            double_date=True).rename(columns=str.lower).rename(columns={"adj final": "adj_final"})
        price_data = price_data[["date", "volume", "value", "adj_final"]]
        return price_data

    def retail_and_institutional_data(self):
        ri_data = fpy.Get_RI_History(
            stock=self.ticker,
            start_date=self.start_date,
            end_date=self.end_date,
            ignore_date=False,
            show_weekday=False,
            double_date=True).rename(columns=str.lower).drop(["ticker", "name", "market"], axis=1)
        return ri_data

    def ri_power(self):
        hist_price = self.get_price()
        hist_ri = self.retail_and_institutional_data()
        df = hist_price.merge(hist_ri, on="date", how="left")
        df["r_buyer_power"] = df["vol_buy_r"] / df["no_buy_r"]
        df["r_seller_power"] = df["vol_sell_r"] / df["no_sell_r"]
        df["i_buyer_power"] = df["vol_buy_i"] / df["no_buy_i"]
        df["i_buyer_power"] = df["vol_sell_i"] / df["no_sell_i"]
        return df


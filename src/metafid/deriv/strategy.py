class OptionStrategy:

    def covered_call(self, purch_assetـprice: float, current_asset_price: float, premium: float, strike_price: float):
        """
        خرید داراییِ پایه و فروشِ همزمانِ اختیارِ خرید
        سود-زیان برابر ارزشِ فروش اختیار و تفاوت ارزشِ روزِ دارایی ( در صورت افزایش تا حد قیمتِ اعمال) با ارزشِ خریدِ دارایی است.

        :param purch_assetـprice:
        :param current_asset_price:
        :param premium:
        :param strike_price:
        :return: max_ptnl_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                breck_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                crrent_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        self.max_ptnl_profit = strike_price - purch_assetـprice + premium
        self.breck_even = purch_assetـprice - premium
        self.pct_max_profit = self.max_ptnl_profit / self.breck_even * 100
        self.crrent_profit = min(strike_price, current_asset_price) - purch_assetـprice + premium
        self.pct_current_profit = self.crrent_profit / self.breck_even * 100

        return self

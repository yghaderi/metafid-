import pandas as pd


class OptionStrategy:

    def covered_call(self, df):
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
        df = df.assign(max_ptnl_profit=df.strike_price - df.adj_final + df.o_adj_final)
        df = df.assign(break_even=df.adj_final - df.o_adj_final)
        df = df.assign(pct_max_profit=df.max_ptnl_profit / df.break_even * 100)
        df["current_profit"] = df.apply(
            lambda x: min(x["strike_price"], x["adj_final"]) - x["adj_final"] + x["o_adj_final"], axis=1)
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)

        return df

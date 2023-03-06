import pandas as pd
from itertools import combinations
from operator import add


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
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = df.assign(max_ptnl_profit=df.strike_price - df.adj_final + df.o_adj_final)
        df = df.assign(break_even=df.adj_final - df.o_adj_final)
        df = df.assign(pct_max_profit=df.max_ptnl_profit / df.break_even * 100)
        df["current_profit"] = df.apply(
            lambda x: min(x["strike_price"], x["adj_final"]) - x["adj_final"] + x["o_adj_final"], axis=1)
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)

        return df

    def married_put(self, df):
        """
        خرید داراییِ پایه و خریدِ همزمان اختیارِ فروش
        سود-زیان  برابرِ اختلافِ ارزشِ روز دارایی (در صورت کاهش تا حد قیمتِ اعمال) با ارزشِ خرید دارایی منهایِ ارزش خرید اختیارِ فروش است.
        :param df:
        :return: max_ptnl_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = df.assign(max_ptnl_profit="indefinite")
        df = df.assign(break_even=df.adj_final + df.o_adj_final)
        df["current_profit"] = df.apply(
            lambda x: max(x["strike_price"], x["adj_final"]) - x["adj_final"] - x["o_adj_final"], axis=1)
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)
        return df

    def bull_call_spread(self, df):
        """

        :param df:
        :return:
        """
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=False)
            if len(group) > 1:
                combo_o_ticker = list(combinations(group.o_ticker, 2))
                combo_strike_price = list(combinations(group.strike_price, 2))
                combo_o_adj_final = list(combinations(group.o_adj_final, 2))
                max_ptnl_loss = [ps - pb for ps, pb in combo_o_adj_final]
                max_ptnl_profit = list(map(add, [i - j for i, j in combo_strike_price], max_ptnl_loss))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s <= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_ptnl_profit[i])
                    elif all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_ptnl_loss[i])
                    else:
                        current_profit.append(group.adj_final.values[0] - combo_strike_price[i][1] + max_ptnl_loss[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_ptnl_loss": max_ptnl_loss,
                    "max_ptnl_profit": max_ptnl_profit,
                    "current_profit": current_profit,

                })
                df_ = pd.concat([df_, df_group])
        return df_

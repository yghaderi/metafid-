import pandas as pd
from itertools import combinations
from operator import add
from collections import namedtuple


class OptionStrategy:

    def covered_call(self, df):
        """
        خرید داراییِ پایه و فروشِ همزمانِ اختیارِ خرید
        سود-زیان برابر ارزشِ فروش اختیار و تفاوت ارزشِ روزِ دارایی ( در صورت افزایش تا حد قیمتِ اعمال) با ارزشِ خریدِ دارایی است.

        :param purch_assetـprice:
        :param current_asset_price:
        :param premium:
        :param strike_price:
        :return: max_pot_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = df.assign(max_pot_profit=df.strike_price - df.adj_final + df.o_adj_final)
        df = df.assign(break_even=df.adj_final - df.o_adj_final)
        df = df.assign(pct_max_profit=df.max_pot_profit / df.break_even * 100)
        df["current_profit"] = df.apply(
            lambda x: min(x["strike_price"], x["adj_final"]) - x["adj_final"] + x["o_adj_final"], axis=1)
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)

        return df

    def married_put(self, df):
        """
        خرید داراییِ پایه و خریدِ همزمان اختیارِ فروش
        سود-زیان  برابرِ اختلافِ ارزشِ روز دارایی (در صورت کاهش تا حد قیمتِ اعمال) با ارزشِ خرید دارایی منهایِ ارزش خرید اختیارِ فروش است.
        :param df:
        :return: max_pot_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = df.assign(max_pot_profit="indefinite")
        df = df.assign(break_even=df.adj_final + df.o_adj_final)
        df["current_profit"] = df.apply(
            lambda x: max(x["strike_price"], x["adj_final"]) - x["adj_final"] - x["o_adj_final"], axis=1)
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)
        return df

    def bull_call_spread(self, df):
        """
       خرید اختیارِ خرید با قیمتِ اعمالِ پایین و فروشِ اختیارِ خرید با قیمتِ اعمالِ بالا در زمانِ اعمالِ همسان
       بیشینه سود برابر است با تفاوتِ بینِ دو قیمتِ اعمال منهایِ پرمیوم پرداختی
       بیشینه ضرر هم برابر است با پرمیوم پرداختی
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
                combo_bs = list(combinations(group.bs, 2))
                max_pot_loss = [ps - pb for ps, pb in combo_o_adj_final]
                max_pot_profit = list(map(add, [i - j for i, j in combo_strike_price], max_pot_loss))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s <= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_profit[i])
                    elif all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(group.adj_final.values[0] - combo_strike_price[i][1] + max_pot_loss[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "bs": combo_bs,
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_pot_loss": max_pot_loss,
                    "max_pot_profit": max_pot_profit,
                    "current_profit": current_profit,

                })
                df_ = pd.concat([df_, df_group])
        return df_

    def bear_call_spread(self, df):
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=True)
            if len(group) > 1:
                combo_o_ticker = list(combinations(group.o_ticker, 2))
                combo_strike_price = list(combinations(group.strike_price, 2))
                combo_o_adj_final = list(combinations(group.o_adj_final, 2))
                combo_bs = list(combinations(group.bs, 2))
                max_pot_profit = [ps - pb for ps, pb in combo_o_adj_final]
                max_pot_loss = list(map(add, [i - j for i, j in combo_strike_price], max_pot_profit))
                break_even = list(map(add, [i for i, _ in combo_strike_price], max_pot_profit))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_profit[i])
                    elif all(s <= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(group.adj_final.values[0] - combo_strike_price[i][1] + max_pot_loss[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "bs": combo_bs,
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_pot_loss": max_pot_loss,
                    "max_pot_profit": max_pot_profit,
                    "current_profit": current_profit,
                    "break_even": break_even,
                })
                df_ = pd.concat([df_, df_group])
        return df_

    def bull_put_spread(self, df):
        stg = namedtuple("Strategy", "sell buy")
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=False)
            if len(group) > 1:
                combo_o_ticker = list(stg(sell=s, buy=b) for s, b in combinations(group.o_ticker, 2))
                combo_strike_price = list(stg(sell=s, buy=b) for s, b in combinations(group.strike_price, 2))
                combo_o_adj_final = list(stg(sell=s, buy=b) for s, b in combinations(group.o_adj_final, 2))
                combo_bs = list(stg(sell=s, buy=b) for s, b in combinations(group.bs, 2))
                max_pot_profit = [ps - pb for ps, pb in combo_o_adj_final]
                max_pot_loss = list(map(add, [-ss + sb for ss, sb in combo_strike_price], max_pot_profit))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s <= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_profit[i])
                    elif all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            group.adj_final.values[0] - combo_strike_price[i].sell + max_pot_profit[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "bs": combo_bs,
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_pot_loss": max_pot_loss,
                    "max_pot_profit": max_pot_profit,
                    "current_profit": current_profit,

                })
                df_ = pd.concat([df_, df_group])
        return df_

    def bear_put_spread(self, df):
        stg = namedtuple("Strategy", "buy sell")
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=True)
            if len(group) > 1:
                combo_o_ticker = list(stg(buy=b, sell=s) for b, s in combinations(group.o_ticker, 2))
                combo_strike_price = list(stg(buy=b, sell=s) for b, s in combinations(group.strike_price, 2))
                combo_o_adj_final = list(stg(buy=b, sell=s) for b, s in combinations(group.o_adj_final, 2))
                combo_bs = list(stg(buy=b, sell=s) for b, s in combinations(group.bs, 2))
                max_pot_loss = [ps - pb for pb, ps in combo_o_adj_final]
                max_pot_profit = list(map(add, [sb - ss for sb, ss in combo_strike_price], max_pot_loss))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_profit[i])
                    elif all(s <= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            - group.adj_final.values[0] + combo_strike_price[i].sell + max_pot_profit[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "bs": combo_bs,
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_pot_loss": max_pot_loss,
                    "max_pot_profit": max_pot_profit,
                    "current_profit": current_profit,

                })
                df_ = pd.concat([df_, df_group])
        return df_

    def call_back_spread(self, df, times):
        stg = namedtuple("Strategy", "buy sell")
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=False)
            if len(group) > 1:
                combo_o_ticker = list(stg(buy=b, sell=s) for b, s in combinations(group.o_ticker, 2))
                combo_strike_price = list(stg(buy=b, sell=s) for b, s in combinations(group.strike_price, 2))
                combo_o_adj_final = list(stg(buy=b, sell=s) for b, s in combinations(group.o_adj_final, 2))
                combo_bs = list(stg(buy=b, sell=s) for b, s in combinations(group.bs, 2))
                premium = [-pb * times + ps for pb, ps in combo_o_adj_final]
                max_pot_loss = list(map(add, [-sb + ss for sb, ss in combo_strike_price], premium))
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(s >= group.adj_final.values[0] for s in combo_strike_price[i]):
                        current_profit.append(premium[i])
                    elif combo_strike_price[i].buy >= group.adj_final.values[0]:
                        current_profit.append(- group.adj_final.values[0] + combo_strike_price[i].sell + premium[i])
                    else:
                        current_profit.append((- group.adj_final.values[0] + combo_strike_price[i].sell) + (
                                    group.adj_final.values[0] - combo_strike_price[i].buy) * times + premium[i])

                df_group = pd.DataFrame({
                    "o_ticker": combo_o_ticker,
                    "strike_price": combo_strike_price,
                    "t": group.t.values[0],
                    "bs": combo_bs,
                    "adj_final": group.adj_final.values[0],
                    "o_adj_final": combo_o_adj_final,
                    "max_pot_loss": max_pot_loss,
                    "current_profit": current_profit,
                })
                df_ = pd.concat([df_, df_group])
        return df_




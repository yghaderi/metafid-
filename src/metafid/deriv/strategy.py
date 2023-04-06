import pandas as pd
from itertools import combinations
from operator import add
from collections import namedtuple


class OptionStrategy:
    def __init__(self, call: pd.DataFrame, put: pd.DataFrame, pct_daily_cp: float = None) -> None:
        self.call = call
        self.put = put
        self.pct_daily_cp = pct_daily_cp
        self.rep_df = pd.DataFrame(
            columns=["strategy", "position", "ua", "ua_final", "t", "bs", "max_pot_profit", "max_pot_loss",
                     "break_even", "current_profit", "pct_cp", "pct_daily_cp", "evaluation"])

    def rep_columns(self, cols):
        return [i for i in cols if i in self.rep_df.columns]

    def position(self, df):
        def replace_(t: str, char):
            for i in char:
                t = t.replace(i, "")
            return t

        df[["strategy", "position"]] = df.stg.map(str).str.split("(", expand=True)
        df["position"] = df.position.apply(lambda x: replace_(x, ["'", ")"]))
        return df

    def covered_call(self):
        """
        خرید داراییِ پایه و فروشِ همزمانِ اختیارِ خرید
        سود-زیان برابر ارزشِ فروش اختیار و تفاوت ارزشِ روزِ دارایی ( در صورت افزایش تا حد قیمتِ اعمال) با ارزشِ خریدِ دارایی است.

        :param df: with (ua, symbol, strike_price, ua_sell_price, buy_price) columns. ua: Underlying Asset
        :return: max_pot_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_loss : با فرض اینکه قیمتِ دارایی پایه صفر شود، برابر است با پرمیومٍ دریافتی منهای قیمتِ خریدِ دارایی پایه
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = self.call[self.call.buy_price > 0].copy()
        stg = namedtuple("CoveredCall", "buy buy_at sell sell_at")
        df["stg"] = df.apply(lambda x: stg(buy=x["ua"], buy_at=x["ua_final"], sell=x["option"], sell_at=x["buy_price"]),
                             axis=1)
        df = self.position(df)

        df = df.assign(max_pot_profit=df.strike_price - df.ua_final + df.buy_price,
                       max_pot_loss=df.buy_price - df.ua_final,
                       break_even=df.ua_final - df.buy_price)
        df["current_profit"] = df.max_pot_profit
        df = df.assign(pct_cp=df.current_profit / df.break_even * 100).round(2)
        df = df.assign(pct_daily_cp=df.pct_cp / df.t).round(2)
        df = df[self.rep_columns(df.columns)]
        if self.pct_daily_cp:
            return df[df.pct_daily_cp > self.pct_daily_cp]
        else:
            return df

    def married_put(self, df: pd.DataFrame):
        """
        خرید داراییِ پایه و خریدِ همزمان اختیارِ فروش
        سود-زیان  برابرِ اختلافِ ارزشِ روز دارایی (در صورت کاهش تا حد قیمتِ اعمال) با ارزشِ خرید دارایی منهایِ ارزش خرید اختیارِ فروش است.
        :param df: with (ua, symbol, strike_price, ua_sell_price, sell_price) columns. ua: Underlying Asset
        :return: max_pot_profit: حداکثر سود بالقوه- در شرایطی ایجاد می‌شود که قیمتِ داراییِ پایه در تاریخ سر-رسید از قیمتِ اعمال بالاتر-برابر باشد
                break_even : اگر قیمتِ داراییِ پایه از این قیمت پایین-تر بیاد، معامله ،وارد زیان می‌شود
                pct_max_profit :
                current_profit : بر اساسِ قیمتِ کنونیِ داراییِ پایه محاسبه می‌شود
                pct_current_profit :
        """
        df = df[df.sell_price > 0]
        stg = namedtuple("MarriedPut", "buy buy_")
        df["strategy"] = df.apply(lambda x: stg(buy=x["ua"], buy_=x["symbol"]), axis=1)
        df = df.assign(max_pot_profit="indefinite")
        df = df.assign(break_even=df.ua_sell_price + df.sell_price)
        df["current_profit"] = df.apply(
            lambda x: max(x["strike_price"], x["ua_sell_price"])
                      - x["ua_sell_price"]
                      - x["sell_price"],
            axis=1,
        )
        df = df.assign(pct_current_profit=df.current_profit / df.break_even * 100)
        df = df.assign(pct_profit_per_day=df.pct_current_profit / df.t)

        return df.sort_values(by="pct_profit_per_day", ascending=False)

    def bull_call_spread(self, df):
        """
        خرید اختیارِ خرید با قیمتِ اعمالِ پایین و فروشِ اختیارِ خرید با قیمتِ اعمالِ بالا در زمانِ اعمالِ همسان
        بیشینه سود برابر است با تفاوتِ بینِ دو قیمتِ اعمال منهایِ پرمیوم پرداختی
        بیشینه ضرر هم برابر است با پرمیوم پرداختی
         :param df: with (ua, symbol, t, strike_price, ua_final, sell_price, buy_price) columns. ua: Underlying Asset
         :return:max_pot_loss: max_pot_loss
                max_pot_profit: max_pot_profit
                current_profit: current_profit
        """
        stg = namedtuple("BullCallSpread", "sell buy")
        groups = df.groupby(by=["ua", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=False)
            if len(group) > 1:
                combo_symbol = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.symbol, 2)
                )
                combo_strike_price = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.strike_price, 2)
                )
                combo_ob_price = list(
                    stg(sell=s, buy=b)
                    for s, b in combinations(
                        group[["sell_price", "buy_price"]].itertuples(index=False), 2
                    )
                )
                combo_bs = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.bs, 2)
                )
                max_pot_loss = [
                    ps.buy_price - pb.sell_price for ps, pb in combo_ob_price
                ]
                max_pot_profit = list(
                    map(add, [ss - sb for ss, sb in combo_strike_price], max_pot_loss)
                )
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(
                            s <= group.ua_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_profit[i])
                    elif all(
                            s >= group.ua_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            group.ua_final.values[0]
                            - combo_strike_price[i].buy
                            + max_pot_loss[i]
                        )

                df_group = pd.DataFrame(
                    {
                        "symbol": combo_symbol,
                        "strike_price": combo_strike_price,
                        "t": group.t.values[0],
                        "bs": combo_bs,
                        "ua_final": group.ua_final.values[0],
                        "ob_price": combo_ob_price,
                        "max_pot_loss": max_pot_loss,
                        "max_pot_profit": max_pot_profit,
                        "current_profit": current_profit,
                    }
                )
                df_ = pd.concat([df_, df_group])
        return df_

    def bear_call_spread(self, df):
        """
        فروش اختیار خرید با قیمتِ اعمالِ پایین-تر و خرید اختیارِ خریدِ با قیمتِ اعمالِ بالا-تر و تاریخِ اعمالِ همسان
        بیشینه سود برابر است باپرمیوم دریافتی
        بیشینه ضرر هم برابر است با تفاوتِ بین دو قیمتِ اعمال و پرمیومِ دریافتی
        :param df: with (ua, symbol, t, strike_price, ua_final, sell_price, buy_price) columns. ua: Underlying Asset
        :return:max_pot_loss: max_pot_loss
                max_pot_profit: max_pot_profit
                current_profit: current_profit
        """
        stg = namedtuple("BearCallSpread", "sell buy")
        groups = df.groupby(by=["ua", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=True)
            if len(group) > 1:
                combo_symbol = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.symbol, 2)
                )
                combo_strike_price = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.strike_price, 2)
                )
                combo_ob_price = list(
                    stg(sell=s, buy=b)
                    for s, b in combinations(
                        group[["sell_price", "buy_price"]].itertuples(index=False), 2
                    )
                )
                combo_bs = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.bs, 2)
                )
                max_pot_profit = [
                    ps.buy_price - pb.sell_price for ps, pb in combo_ob_price
                ]
                max_pot_loss = list(
                    map(add, [ss - sb for ss, sb in combo_strike_price], max_pot_profit)
                )
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(
                            s >= group.ua_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_profit[i])
                    elif all(
                            s <= group.ua_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            -group.ua_final.values[0]
                            + combo_strike_price[i].sell
                            + max_pot_profit[i]
                        )

                df_group = pd.DataFrame(
                    {
                        "symbol": combo_symbol,
                        "strike_price": combo_strike_price,
                        "t": group.t.values[0],
                        "bs": combo_bs,
                        "ua_final": group.ua_final.values[0],
                        "ob_price": combo_ob_price,
                        "max_pot_loss": max_pot_loss,
                        "max_pot_profit": max_pot_profit,
                        "current_profit": current_profit,
                    }
                )
                df_ = pd.concat([df_, df_group])
        return df_

    #####################
    def bull_put_spread(self, df):
        stg = namedtuple("Strategy", "sell buy")
        groups = df.groupby(by=["ticker", "t"])
        df_ = pd.DataFrame()
        for name, group in groups:
            group.reset_index(inplace=True)
            group = group.sort_values(by=["strike_price"], ascending=False)
            if len(group) > 1:
                combo_o_ticker = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.o_ticker, 2)
                )
                combo_strike_price = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.strike_price, 2)
                )
                combo_o_adj_final = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.o_adj_final, 2)
                )
                combo_bs = list(
                    stg(sell=s, buy=b) for s, b in combinations(group.bs, 2)
                )
                max_pot_profit = [ps - pb for ps, pb in combo_o_adj_final]
                max_pot_loss = list(
                    map(
                        add, [-ss + sb for ss, sb in combo_strike_price], max_pot_profit
                    )
                )
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(
                            s <= group.adj_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_profit[i])
                    elif all(
                            s >= group.adj_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            group.adj_final.values[0]
                            - combo_strike_price[i].sell
                            + max_pot_profit[i]
                        )

                df_group = pd.DataFrame(
                    {
                        "o_ticker": combo_o_ticker,
                        "strike_price": combo_strike_price,
                        "t": group.t.values[0],
                        "bs": combo_bs,
                        "adj_final": group.adj_final.values[0],
                        "o_adj_final": combo_o_adj_final,
                        "max_pot_loss": max_pot_loss,
                        "max_pot_profit": max_pot_profit,
                        "current_profit": current_profit,
                    }
                )
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
                combo_o_ticker = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.o_ticker, 2)
                )
                combo_strike_price = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.strike_price, 2)
                )
                combo_o_adj_final = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.o_adj_final, 2)
                )
                combo_bs = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.bs, 2)
                )
                max_pot_loss = [ps - pb for pb, ps in combo_o_adj_final]
                max_pot_profit = list(
                    map(add, [sb - ss for sb, ss in combo_strike_price], max_pot_loss)
                )
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(
                            s >= group.adj_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_profit[i])
                    elif all(
                            s <= group.adj_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(max_pot_loss[i])
                    else:
                        current_profit.append(
                            -group.adj_final.values[0]
                            + combo_strike_price[i].sell
                            + max_pot_profit[i]
                        )

                df_group = pd.DataFrame(
                    {
                        "o_ticker": combo_o_ticker,
                        "strike_price": combo_strike_price,
                        "t": group.t.values[0],
                        "bs": combo_bs,
                        "adj_final": group.adj_final.values[0],
                        "o_adj_final": combo_o_adj_final,
                        "max_pot_loss": max_pot_loss,
                        "max_pot_profit": max_pot_profit,
                        "current_profit": current_profit,
                    }
                )
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
                combo_o_ticker = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.o_ticker, 2)
                )
                combo_strike_price = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.strike_price, 2)
                )
                combo_o_adj_final = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.o_adj_final, 2)
                )
                combo_bs = list(
                    stg(buy=b, sell=s) for b, s in combinations(group.bs, 2)
                )
                premium = [-pb * times + ps for pb, ps in combo_o_adj_final]
                max_pot_loss = list(
                    map(add, [-sb + ss for sb, ss in combo_strike_price], premium)
                )
                current_profit = []
                for i in range(len(combo_strike_price)):
                    if all(
                            s >= group.adj_final.values[0] for s in combo_strike_price[i]
                    ):
                        current_profit.append(premium[i])
                    elif combo_strike_price[i].buy >= group.adj_final.values[0]:
                        current_profit.append(
                            -group.adj_final.values[0]
                            + combo_strike_price[i].sell
                            + premium[i]
                        )
                    else:
                        current_profit.append(
                            (-group.adj_final.values[0] + combo_strike_price[i].sell)
                            + (group.adj_final.values[0] - combo_strike_price[i].buy)
                            * times
                            + premium[i]
                        )

                df_group = pd.DataFrame(
                    {
                        "o_ticker": combo_o_ticker,
                        "strike_price": combo_strike_price,
                        "t": group.t.values[0],
                        "bs": combo_bs,
                        "adj_final": group.adj_final.values[0],
                        "o_adj_final": combo_o_adj_final,
                        "max_pot_loss": max_pot_loss,
                        "current_profit": current_profit,
                    }
                )
                df_ = pd.concat([df_, df_group])
        return df_

    def all_strategy(self):
        return pd.concat([self.rep_df, self.covered_call()])


#################


def long_straddle(df):
    """
    A long straddle strategy is an options strategy that involves buying a call and a put on the same
    underlying asset with the same strike price and expiration date. The strike price is usually at-the-money
     or close to it. The goal of this strategy is to profit from a very strong move in either direction by the
      underlying asset, often triggered by a newsworthy event.

    :param df: with (strike_price, ua_final, call_buy_price, put_buy_price) columns
    :return:maximum loss: net premium received
            maximum profit: unlimited
            lower break-even: strike price – net premium
            upper break-even: strike price  + net premium
    """
    stg = namedtuple("LongStraddle", "buy_c buy_p")
    df = df.assign(max_pot_loss=-df.call_sell_price - df.put_sell_price)
    df = df.assign(
        lower_break_even=df.strike_price + df.max_pot_loss,
        upper_break_even=df.strike_price - df.max_pot_loss,
    )
    df["current_profit"] = df.apply(
        lambda x: max(
            x["max_pot_loss"],
            abs(x["ua_final"] - x["strike_price"]) + x["max_pot_loss"],
        ),
        axis=1,
    )
    return df.sort_values(by="current_profit", ascending=False)


def short_straddle(df):
    """
    price and expiration date. It is used when the trader believes the underlying asset will not move significantly
     higher or lower over the lives of the options contracts.

    The strategy looks to take advantage of a drop in volatility, time decay, and little or no movement from the
     underlying asset.

    Lower Brea-keven = Strike Price – Net Premium
    Upper Break-even = Strike Price  + Net Premium
    :param df: with (strike_price, ua_final, call_buy_price, put_buy_price) columns
    :return:maximum loss: unlimited
            maximum profit = net premium received
            lower brea-keven = strike price – net premium
            upper break-even = strike price  + net premium

    """
    stg = namedtuple("ShortStraddle", "sell_c sell_p")
    df = df.assign(max_pot_profit=df.call_buy_price + df.put_buy_price)
    df = df.assign(
        lower_break_even=df.strike_price - df.max_pot_profit,
        upper_break_even=df.strike_price + df.max_pot_profit,
    )
    df["current_profit"] = df.apply(
        lambda x: min(
            x["max_pot_profit"],
            -abs(x["ua_final"] - x["strike_price"]) + x["max_pot_profit"],
        ),
        axis=1,
    )
    return df.sort_values(by="current_profit", ascending=False)

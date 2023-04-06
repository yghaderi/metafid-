import pandas as pd
import jdatetime as jdt
import requests
import re


class TSETMC:
    def __init__(self, drop_cols: list = None) -> None:
        """
        :param drop_cols: List of columns you want to drop
        """
        self.drop_cols = drop_cols
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        }

    def mw(self):
        r = requests.get(
            "http://www.tsetmc.com/tsev2/data/MarketWatchPlus.aspx",
            headers=self.headers,
        )
        main_text = r.text
        ob_df = pd.DataFrame((main_text.split("@")[3]).split(";"))
        ob_df = ob_df[0].str.split(",", expand=True)
        ob_df.columns = [
            "web_id",
            "ob_depth",
            "sell_no",
            "buy_no",
            "buy_price",
            "sell_price",
            "buy_vol",
            "sell_vol",
        ]
        ob_df = ob_df[
            [
                "web_id",
                "ob_depth",
                "sell_no",
                "sell_vol",
                "sell_price",
                "buy_price",
                "buy_vol",
                "buy_no",
            ]
        ]
        ob_df.set_index("web_id", inplace=True)

        mw_df = pd.DataFrame((main_text.split("@")[2]).split(";"))
        mw_df = mw_df[0].str.split(",", expand=True)
        mw_df = mw_df.iloc[:, :23]
        mw_df.columns = [
            "web_id",
            "isin",
            "symbol",
            "name",
            "time",
            "open",
            "final",
            "close",
            "no",
            "volume",
            "value",
            "low",
            "high",
            "y_final",
            "eps",
            "base_vol",
            "unknown1",
            "unknown2",
            "sector",
            "day_ul",
            "day_ll",
            "share_no",
            "mkt_id",
        ]
        mw_df.set_index("web_id", inplace=True)
        df = mw_df.join(ob_df)

        def replace_(x):
            return x.replace("ي", "ی").replace("ك", "ک")

        df = df.assign(
            dt=jdt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol=df.symbol.map(replace_),
            name=df.name.map(replace_),
        )
        if self.drop_cols:
            return df.drop(columns=self.drop_cols, axis=1)
        else:
            return df

    def option_mv(self, ua: list = None):
        """

        :param ua:Underlying Asset list.
        :return:
        """
        mw_df = self.mw()

        ua_df = mw_df[
            (mw_df.symbol.isin(ua))
            & (mw_df.value.astype(int) > 0)
            & (mw_df.ob_depth.astype(int) == 1)
            ].add_prefix("ua_")
        ua_df.rename(columns={"ua_symbol": "ua"}, inplace=True)
        ua_df.drop_duplicates(inplace=True)

        mw_df = mw_df[mw_df.name.str.startswith("اختیار")].rename(
            columns={"symbol": "option"}
        )

        def expand_option_info(df_):
            def clean_date(x):
                date_ = re.findall("[0-9]+", x)
                date_ = "".join(date_)
                if len(date_) == 8:
                    return f"{date_[:4]}-{date_[4:6]}-{date_[6:8]}"
                elif len(date_) == 6:
                    return f"14{date_[:2]}-{date_[2:4]}-{date_[4:6]}"
                else:
                    print("bad data!")
                    return None

            def clean_ua(x):
                return x.replace("اختیارخ ", "").replace("اختیارف ", "")

            def day_to_ex(ex_date):
                y, m, d = map(int, (tuple(ex_date.split("-"))))
                return (jdt.date(y, m, d) - jdt.date.today()).days

            df_[["ua", "strike_price", "ex_date"]] = df_.name.str.split("-", expand=True)
            df_ = df_.assign(ua=df_.ua.map(clean_ua))
            df_ = df_.merge(ua_df, on="ua", how="inner")
            df_ = df_[~df_.ex_date.isnull()]
            df_ = df_.assign(ex_date=df_.ex_date.map(clean_date))
            df_ = df_.assign(t=df_.ex_date.map(day_to_ex))
            return df_

        df = expand_option_info(mw_df)
        df = df.apply(pd.to_numeric, errors="ignore")
        return df

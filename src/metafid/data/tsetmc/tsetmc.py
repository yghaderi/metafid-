import pandas as pd
import jdatetime as jdt
import requests
import re

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
}


def get_data(url, timeout=3):
    return requests.get(url, headers=headers, verify=False, timeout=timeout)


class MarketWatch:
    def __init__(self, drop_cols: list = None) -> None:
        """
        :param drop_cols: List of columns you want to drop
        """
        self.drop_cols = drop_cols

    def get_mw(self):

        url = "http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx"
        main_text = get_data(url).text

        ob_df = pd.DataFrame((main_text.split("@")[3]).split(";"))  # order book
        ob_df = ob_df[0].str.split(",", expand=True)
        ob_df.columns = [
            "ins_id",
            "quote",
            "sell_no",
            "buy_no",
            "buy_price",
            "sell_price",
            "buy_vol",
            "sell_vol",
        ]
        ob_df = ob_df[
            [
                "ins_id",
                "quote",
                "sell_no",
                "sell_vol",
                "sell_price",
                "buy_price",
                "buy_vol",
                "buy_no",
            ]
        ]
        ob_df.set_index("ins_id", inplace=True)

        mw_df = pd.DataFrame((main_text.split("@")[2]).split(";"))
        mw_df = mw_df[0].str.split(",", expand=True)
        mw_df = mw_df.iloc[:, :23]
        mw_df.columns = [
            "ins_id",
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
        mw_df.set_index("ins_id", inplace=True)
        df = mw_df.join(ob_df)

        def replace_(x):
            return x.translate(str.maketrans({"ي": "ی", "ك": "ک"}))

        df = df.assign(
            dt=jdt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol=df.symbol.map(replace_),
            name=df.name.map(replace_),
        )
        if self.drop_cols:
            return df.drop(columns=self.drop_cols, axis=1)
        else:
            return df

    def option_mv(self):
        mw_df = self.get_mw()
        mw_df.drop(["time", "open", "no", "eps", "unknown1", "unknown2", "sector", "day_ul", "day_ll", "mkt_id"],
                   axis=1, inplace=True)

        option_df = mw_df[mw_df["isin"].str.startswith(("IRO9","IROF"))].rename(columns={"symbol": "option"})

        ua_df = mw_df[(mw_df["isin"].str.startswith(("IRO1", "IRO3", "IRT1", "IRT3"))) & (
            mw_df["isin"].str.endswith("1"))].add_prefix("ua_")

        ua_df = ua_df[ua_df.ua_quote.astype(int) == 1]
        ua_df.drop_duplicates(inplace=True)
        ua_df.drop(["ua_dt", "ua_name"], axis=1, inplace=True)

        def expand_option_info(df_):
            def clean_date(x):
                date_ = re.findall("[0-9]+", x)
                date_ = "".join(date_)
                if len(date_) == 8:
                    return f"{date_[:4]}-{date_[4:6]}-{date_[6:8]}"
                elif len(date_) == 6:
                    return f"14{date_[:2]}-{date_[2:4]}-{date_[4:6]}"
                else:
                    print("bad ex-date!")
                    return None

            def ua(x):
                return x[4:8]

            def day_to_ex(ex_date):
                y, m, d = map(int, (tuple(ex_date.split("-"))))
                return (jdt.date(y, m, d) - jdt.date.today()).days

            df_[["ua", "strike_price", "ex_date"]] = df_.name.str.split("-", expand=True)
            df_["ua"] = df_["isin"].map(ua)
            ua_df["ua"] = ua_df["ua_isin"].map(ua)
            df_ = df_.merge(ua_df, on="ua", how="inner")
            df_ = df_[~df_.ex_date.isnull()]
            df_ = df_.assign(ex_date=df_.ex_date.map(clean_date))
            df_ = df_.assign(t=df_.ex_date.map(day_to_ex))
            return df_

        df = expand_option_info(option_df)

        df = df.apply(pd.to_numeric, errors="ignore")

        def option_type(x):
            return "call" if x.startswith("ض") else "put"

        df = df.assign(type=df.option.map(option_type))
        return df


class InstrumentInfo:
    def __init__(self):
        self.mw = self.get_instrument_ids()  # market watch

    def get_instrument_ids(self):
        def clean_data(text):
            df = pd.DataFrame((text.split("@")[2]).split(";"))
            df = df[0].str.split(",", expand=True)
            df = df.iloc[:, :23]
            df.columns = [
                "ins_id",
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
            return df

        url = "http://tsetmc.com/tsev2/data/MarketWatchPlus.aspx?"
        return clean_data(get_data(url).text)

    @staticmethod
    def _clean_instrument_info(instrument_info: str):
        instrument = {
            "symbol": instrument_info["cIsin"][4:8],
            "name": instrument_info["lVal18"],
            "symbol_far": instrument_info["lVal18AFC"],
            "name_far": instrument_info["lVal30"],
            "isin": instrument_info["cIsin"],
            "capital": instrument_info["zTitad"],
            "instrument_code": instrument_info["insCode"],
            "instrument_id": instrument_info["instrumentID"],
            "sector_name": instrument_info["sector"]["lSecVal"],
            "sector_code": instrument_info["sector"]["cSecVal"].replace(" ", ""),
            "group_type": instrument_info["cgrValCot"],
            "market_name": instrument_info["flowTitle"],
            "market_code": instrument_info["flow"],
            "market_type": instrument_info["cgrValCotTitle"],
            "base_vol": instrument_info["baseVol"],
            "eps": instrument_info["eps"]["estimatedEPS"],
            "float_shares_pct": instrument_info["kAjCapValCpsIdx"],
        }
        return instrument

    def get_instrument_info(self, ids):
        df = pd.DataFrame()
        for ins_id in ids:
            url = f"http://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{ins_id}"
            ins_info = get_data(url).json()["instrumentInfo"]
            clean_ins_info = self._clean_instrument_info(ins_info)
            df = pd.concat([df, pd.DataFrame.from_records([clean_ins_info])], ignore_index=True)
        return df

    def option_info(self):
        ids = self.mw[self.mw["isin"].str.startswith("IRO9")]
        df = self.get_instrument_info(ids.ins_id.values).rename(columns={"symbol": "ua"})
        return df.iloc[:, :9]

    def stock_info(self):
        ids = self.mw[self.mw["isin"].str.startswith(("IRO1", "IRO3"))]
        return self.get_instrument_info(ids.ins_id.values)

    def etf_info(self):
        ids = self.mw[self.mw["isin"].str.startswith(("IRT1", "IRT3"))]
        return self.get_instrument_info(ids.ins_id.values)


class OrderBook:

    def best_limits(self, ins_ids: list):
        cols = {"number": "quote",
                "zOrdMeDem": "buy_no",
                "qTitMeDem": "buy_vol",
                "pMeDem": "buy_price",
                "pMeOf": "sell_price",
                "zOrdMeOf": "sell_no",
                "qTitMeOf": "sell_vol",
                "insCode": "ins_id"}
        df = pd.DataFrame()
        for ins_id in ins_ids:
            url = f"http://cdn.tsetmc.com/api/BestLimits/{ins_id}"
            best_limits = get_data(url).json()["bestLimits"]
            best_limits_df = pd.DataFrame.from_records(best_limits).rename(columns=cols)
            best_limits_df["ins_id"] = ins_id
            df = pd.concat([df, best_limits_df])
        return df

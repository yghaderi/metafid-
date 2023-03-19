import pandas as pd
import jdatetime
import requests
import re


ua_list = [
    "شپنا",
    "خودرو",
    "فملي",
    "شستا",
    "وبصادر",
    "فولاد",
    "خگستر",
    "سلام",
    "دي",
    "سرو",
    "خاور",
    "وبملت",
    "خساپا",
    "فخوز",
    "فرابورس",
    "اهرم",
    "حسير",
    "خپارس",
    "هاي وب",
    "چكاپا",
    "توسن",
    "وتجارت",
    "ذوب",
    "بساما",
    "شبندر",
    "سمگا",
    "وخاور",
    "شتران",
    "كاريس",
    "ت ثاميد",
    "پالايش",
    "لبخند",
    "تپسي",
    "كارا",
]


def mw():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }
    r = requests.get(
        "http://www.tsetmc.com/tsev2/data/MarketWatchPlus.aspx", headers=headers
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
        "ticker",
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
    df = df.assign(dt=jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return df


def option_data():
    mw_df = mw()

    ua_df = mw_df[(mw_df.ticker.isin(ua_list)) & (mw_df.value.astype(int) > 0)][
        ["ticker", "final"]
    ]
    ua_df.rename(columns={"ticker": "ua", "final": "ua_final"}, inplace=True)
    ua_df.drop_duplicates(inplace=True)

    mw_df = mw_df[mw_df.name.str.startswith("اختيار")]

    def expand_option_info(df):
        def clean_date(x):
            date_ = re.findall("[0-9]+", x)
            date_ = "".join(date_)
            if len(date_) == 8:
                return f"{date_[:4]}-{date_[4:6]}-{date_[6:8]}"
            elif len(date_) == 6:
                return f"00{date_[:2]}-{date_[2:4]}-{date_[4:6]}"
            else:
                print("bad data!")
                return None

        def clean_ua(x):
            return x.replace("اختيارخ ", "").replace("اختيارف ", "")

        df[["ua", "strike_price", "ex_date"]] = df.name.str.split("-", expand=True)
        df = df.assign(ua=df.ua.map(clean_ua))
        df = df.merge(ua_df, on="ua", how="inner")
        df = df[~df.ex_date.isnull()]
        df = df.assign(ex_date=df.ex_date.map(clean_date))
        return df

    df = expand_option_info(mw_df)
    df = df.apply(pd.to_numeric, errors="ignore")
    return df

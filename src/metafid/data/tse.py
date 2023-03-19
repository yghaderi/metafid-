import pandas as pd
import jdatetime
import requests


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

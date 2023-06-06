from datetime import datetime

from ...ta.ripower import RetailInstitutionalPower
from ..db_psycopg import DB


class RetailInstitutionalPowerMFW:
    def __init__(self, dbname: str, user: str, pass_: str, ri_power_table: str, top: int) -> None:
        self.db = DB(dbname=dbname, user=user, pass_=pass_)
        self.ri_df = self.db.query_all(table="tsedata_section", cols="*")
        self.ri_power = RetailInstitutionalPower(ri_data=self.ri_df).ri_per_capita()
        self.df_hist = self.db.query_all(table="tsedata_histprice", cols="date_id,symbol_id,final,volume")
        self.tickers = self.db.query_all(table="tsedata_ticker", cols="symbol,symbol_far")
        self.ri_power_table = ri_power_table
        self.top = top
        self.col = ["date_id", "symbol_far", "volume", "final", "r_buyer_power", "r_seller_power", "i_buyer_power",
                    "i_seller_power"]

    def filter_ri_power(self):
        ri_df_last_date = self.ri_power[self.ri_power.date_id == self.ri_power.date_id.max()].copy()
        ri_df_last_date = ri_df_last_date.sort_values(by="rbp_to_rsp", ascending=False).head(self.top)
        return self.ri_power[self.ri_power.symbol_id.isin(ri_df_last_date.symbol_id)]

    def clean_and_add_final_data(self):
        df = self.df_hist.merge(self.filter_ri_power(), on=["date_id", "symbol_id"], how="inner")
        df = df.merge(self.tickers, left_on="symbol_id", right_on="symbol", how="inner")
        df = df[self.col].rename(columns={"date_id": "date", "symbol_far": "ticker"})
        return df.fillna(0)

    def do_job(self):
        self.db.drop_all(table=self.ri_power_table)
        self.db.insert_data(table=self.ri_power_table, df=self.clean_and_add_final_data())
        print(f'Drop all {self.ri_power_table} records and insert new data! The time is: {datetime.now()}', end="\r")

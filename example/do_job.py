
from CONFIG import DB_PASS as PASS
from metafid.mfw.deriv import OptionStrategyMFW
from metafid.mfw.ta import RetailInstitutionalPowerMFW

import warnings
warnings.filterwarnings('ignore')

DBNAME = "metafid"
USER = "postgres"

ostg_mfw = OptionStrategyMFW(dbname=DBNAME, user=USER, pass_=PASS, ua_table="sigma", ostg_table="derivs_optionstrategy", pct_daily_cp=0.2, interval=10)
ri = RetailInstitutionalPowerMFW(dbname=DBNAME, user=USER, pass_=PASS, ri_power_table="ta_ripower", top=20)


if __name__ == "__main__":
    ri.do_job()
    ostg_mfw.do_job()
    
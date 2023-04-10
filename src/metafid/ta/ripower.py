class RetailInstitutionalPower:

    def __init__(self, ri_data):
        self.ri_data = ri_data


    def ri_per_capita(self):
        df = self.ri_data.copy()
        df = df.assign(r_buyer_power = df.val_buy_r/df.no_buy_r,
                       r_seller_power= df.val_sell_r / df.no_sell_r,
                       i_buyer_power= df.val_buy_i / df.no_buy_i,
                       i_seller_power = df.val_sell_i / df.no_sell_i)
        df = df.assign(rbp_to_rsp = df.r_buyer_power/df.r_seller_power)        
        return df


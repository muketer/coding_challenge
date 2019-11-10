import pandas as pd
import numpy as np
import datetime as dt


class Matcher:

    def __init__(self):
        file_folder = './data/'
        filename_imp_click = file_folder + 'impclk.csv'
        filename_adname = file_folder + 'lineitems.csv'
        filename_ip_region = file_folder + 'states.csv'

        self.result_filename = file_folder + 'final_result.csv'
        self.result_total_sum_filename = file_folder + 'total_sum.csv'

        self.join_idx_name = 'idx'

        self.df_imp_click = pd.read_csv(filename_imp_click)
        self.df_adname = pd.read_csv(filename_adname)
        self.df_ip_region = pd.read_csv(filename_ip_region)

        # self.df_ip_region_arr = None
        # self.distinct_ips = None

    def df_treat(self):
        self.df_imp_click = pd.merge(self.df_imp_click, self.df_adname, how='left', on='lineitem_id')
        self.df_imp_click = self.df_imp_click[self.df_imp_click.columns[:-1]]

        self.df_ip_region.fillna(0, inplace=True)

        # self.df_ip_region_arr = self.df_ip_region.values

        # self.distinct_ips = np.array(sorted(self.df_imp_click.ip.unique()))

    def ip_compare_join(self):
        ip_idx = pd.Index(self.df_imp_click.ip)
        start_ip_idx = pd.Index(self.df_ip_region.ip_from)
        end_ip_idx = pd.Index(self.df_ip_region.ip_to) + 1

        start_idx = start_ip_idx.searchsorted(ip_idx, side='right') - 1
        end_idx = end_ip_idx.searchsorted(ip_idx, side='right')

        self.df_imp_click[self.join_idx_name] = np.where(start_idx==end_idx, end_idx, np.nan)

        self.df_imp_click = pd.merge(
            self.df_imp_click, self.df_ip_region, how='left', left_on=[self.join_idx_name], right_index=True
        )
        self.df_imp_click.fillna('0', inplace=True)
        self.df_imp_click = self.df_imp_click[
            ['ip', 'lineitem_id', 'impression', 'clk', 'item_name', 'region']
        ]

    def export_result(self):
        self.df_imp_click.to_csv(self.result_filename)
        self.df_imp_click.groupby(['item_name', 'region'])['impression', 'clk'].sum().to_csv(
            self.result_total_sum_filename
        )


if __name__ == '__main__':

    start_time = dt.datetime.now()

    matcher = Matcher()
    matcher.df_treat()

    matcher.ip_compare_join()

    matcher.export_result()

    end_time = dt.datetime.now()
    print(end_time - start_time)
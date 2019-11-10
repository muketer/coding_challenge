import pandas as pd
import numpy as np

import datetime as dt

import multiprocessing as mp


class PreTest:

    def __init__(self):
        file_folder = './data/'
        filename_imp_click = file_folder + 'impclk.csv'
        filename_adname = file_folder + 'lineitems.csv'
        filename_ip_region = file_folder + 'states.csv'

        self.result_filename = file_folder + 'final_result.csv'
        self.result_total_sum_filename = file_folder + 'total_sum.csv'

        self.df_imp_click = pd.read_csv(filename_imp_click)
        self.df_adname = pd.read_csv(filename_adname)
        self.df_ip_region = pd.read_csv(filename_ip_region)

        self.df_ip_region_arr = None
        self.distinct_ips = None

    def df_treat(self):
        self.df_imp_click = pd.merge(self.df_imp_click, self.df_adname, how='left', on='lineitem_id')
        self.df_imp_click = self.df_imp_click[self.df_imp_click.columns[:-1]]

        self.df_ip_region.fillna(0, inplace=True)

        self.df_ip_region_arr = self.df_ip_region.values

        self.distinct_ips = np.array(sorted(self.df_imp_click.ip.unique()))

    def one_ip_compare(self, addr_):
        df_one_ip_compare = np.hstack([self.df_ip_region_arr, np.full((len(self.df_ip_region_arr), 1), addr_)])

        df_one_ip_compare_is_include = (df_one_ip_compare[:, 3] >= df_one_ip_compare[:, 0]) &\
                                       (df_one_ip_compare[:, 3] <= df_one_ip_compare[:, 1])

        # try:
        #     return_value = df_one_ip_compare[np.where(df_one_ip_compare_is_include == True)[0][0], 2]
        #     print(return_value)
        #     # end_time = dt.datetime.now()
        #     # print(end_time - start_time)
        #     return return_value
        # except:
        #     end_time = dt.datetime.now()
        #     print(end_time - start_time)
        #     return '0'

        if True in df_one_ip_compare_is_include:
            return df_one_ip_compare[np.where(df_one_ip_compare_is_include)[0][0], 2]
        else:
            return '0'

    def final_merge(self, result_list_):
        df_ip_region_matcher = pd.DataFrame({
            'ip': sorted(self.df_imp_click.ip.unique()),
            'region': result_list_
        })
        self.df_imp_click = pd.merge(self.df_imp_click, df_ip_region_matcher, how='left', on='ip')

    def export_result(self):
        self.df_imp_click.to_csv(self.result_filename)
        self.df_imp_click.groupby(['item_name', 'region'])['impression', 'clk'].sum().to_csv(
            self.result_total_sum_filename)


if __name__ == '__main__':

    start_time = dt.datetime.now()

    pre_test = PreTest()
    pre_test.df_treat()

    pool = mp.Pool(mp.cpu_count())

    result_list = pool.map(pre_test.one_ip_compare, pre_test.distinct_ips)

    pool.close()
    pool.join()

    pre_test.final_merge(result_list)

    pre_test.export_result()

    end_time = dt.datetime.now()
    print(end_time - start_time)

    # 3: 06:35.833723

    # 2019 - 08 - 11 15: 49:49.003003
    # 2: 36:39.233091

    # 2019 - 08 - 11 13: 11:30.354041
    # 2: 23:13.368116
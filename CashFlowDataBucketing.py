import pandas as pd
# from flask import current_app as app
from nltk.stem import PorterStemmer
from main_page_config import keyword_mapping_settings
# from src.modules.data_processing.DataBucketingGeneric import DataBucketingGeneric
import os
from TechMagicFuzzy import TechMagicFuzzy
from DataBucketingUtils import *

"""
@author: jayesh.thukarul
"""


class CashFlowDataBucketing():
    def __init__(self, df_datasheet, df_nlp_bucket_master, record_dtls=None):
        # DataBucketingGeneric.__init__(self, statement_type='ccf', record_dtls_=record_dtls)

        self.df_datasheet = df_datasheet
        self.df_nlp_bucket_master = df_nlp_bucket_master
        # print(df_nlp_bucket_master)
        self.ps = PorterStemmer()

        self.conf_score_thresh = 80
        self.df_response = None
        self.df_drilldown_gen = None
        self.years_list = []
        self.record_dtls = record_dtls
        self.section_subtotal = {}
        self.list_drilldown_flags = {}
        self.obj_techfuzzy = TechMagicFuzzy()

        # bucketing logic method
        # self.fetch_report()

    def get_keywords_library(self,filepath):
        res_dict = {}

        if not os.path.exists(filepath):
            return res_dict

        df_book = pd.read_csv(filepath, sep='\t')
        for key in df_book.key.unique():
            res_dict[key] = []

        for df_index, df_row in df_book.iterrows():
            str_keyword = str(df_row['keyword']).strip()
            res_dict[df_row['key']].append(str_keyword)
            continue
        return res_dict

    # updated method for dynamic handling
    def fetch_report(self):
        # cleanup record values (check for special characters and fiels types for calculations)
        print("report_data_tuning")
        self.report_data_tuning()

        self.df_response = pd.DataFrame(columns=self.df_nlp_bucket_master.columns.values)

        # app.logger.info(f'DATA PDF REPORT:\n {self.df_datasheet}')
        # print(f'BUCKET DATAFRAME:\n {self.df_nlp_bucket_master}')
        # print('============================================================================================')
        print("get_section_subtotal")
        self.get_section_subtotal()
        print("process_operating_bucket")
        self.process_operating_bucket()
        print("process_investing_bucket")
        self.process_investing_bucket()
        print("process_financing_bucket")
        self.process_financing_bucket()

        # print('============================================================================================')
        # app.logger.info(f'Bukceting Response | CCF |{self.df_response}')

        # RECORD INSERT IN DB
        # self.report_bucketing_db_save(self.record_dtls['id'], self.df_response, self.years_list)

        # LINE-ITEMS RECORD INSERT IN DB
        # self.lineitems_db_save(self.record_dtls['id'], self.years_list)

        # app.logger.info(f'Bukceting Response | CCF |{self.df_response}')

        # print(f'DRILL-DOWN DATAFRAME \n{self.df_drilldown_gen}')

        # TODO remove temporary code
        rem_cols = ['id', 'statement_section', 'statement_sub_section', 'statement_type', 'target_keyword',
                    'fetch_type', 'sequence_view', 'match_type',
                    'note_keyword', 'flag_model_training', 'field_tage', 'remarks', 'custom_formula', 'ref_notes_key']

        # get years list and sort in asc order
        col_list = ([str(i) for i in self.df_response.columns.values if i not in rem_cols])

        self.df_response = self.df_response[col_list]

        self.df_response = self.df_response.rename(columns={'primary_keywords': 'Particulars'})
        self.df_response.fillna('', inplace=True)
        # --- till here

        # return

    # data cleanup and add necessary columns for processing the bucketing logic
    def report_data_tuning(self):
        # get year list in integer form
        self.df_datasheet = remove_total_lines_main_pages(df_datasheet=self.df_datasheet,filepath=keyword_mapping_settings.mastersheet_filter_particulars,statement_type='ccf',obj_techfuzzy=self.obj_techfuzzy)
        data_column_names = self.df_datasheet.columns.values

        # ignore these columns to fetch years list
        filter_headers = ['Notes', 'Particulars', 'statement_section', 'statement_sub_section']

        # get years list and sort in asc order
        years_list = ([int(i) for i in data_column_names if i not in filter_headers])
        years_list.sort()

        # convert into str format for processing
        self.years_list = [str(i) for i in years_list]

        # add years columns in bucket dataframe
        for curr_year in self.years_list:
            self.df_nlp_bucket_master[str(curr_year)] = float(0)

        # handle numeric data
        # self.df_datasheet = self.numeric_data_tuning(self.df_datasheet, self.years_list, drop_nan_years=True)

        # match score flag in bucketing
        self.df_nlp_bucket_master['score'] = 0

        # check in report dataframe record if particular value calculated - avoid repeated value assignment
        self.df_datasheet['flg_processed'] = False

        # create drill down dataframe
        self.get_drilldown_dataframe(self.years_list)

        # return

    def get_drilldown_dataframe(self, list_years):
        list_cols = ['meta_keyword', 'particulars']
        list_cols = list_cols + list_years
        self.df_drilldown_gen = pd.DataFrame(columns=list_cols)
        return self.df_drilldown_gen

    def get_section_subtotal(self):

        subtotal_keywords = self.get_keywords_library(keyword_mapping_settings.ccf_section_subtotal_keywords)

        # SEARCH FOR NET SUBTOTAL SECTION-WISE
        for section_key, list_keywords in subtotal_keywords.items():
            self.section_subtotal[section_key] = {}

            # ERROR HANDLING : If Section is not present in dataframe then continue
            if section_key not in self.df_datasheet['statement_section'].values:
                continue

            # GET ONLY SECTION Dataframe
            df_section_datasheet = self.df_datasheet[self.df_datasheet['statement_section'] == section_key]
            # print(f'{section_key} | {list_keywords}')

            # ITERATE IN REVERSE ORDER
            for idx in reversed(df_section_datasheet.index):
                res_match = self.obj_techfuzzy.token_sort_pro(self.df_datasheet.loc[idx, 'Particulars'],
                                                              list_keywords)
                # print(f'{self.df_datasheet.loc[idx, "Particulars"]} | {res_match}')
                if res_match[0][1] >= 85:
                    # print(f'{section_key} subtotoal | {self.df_datasheet.loc[idx, "Particulars"]}')
                    for curr_year in self.years_list:
                        self.section_subtotal[section_key][curr_year] = self.df_datasheet.loc[idx, curr_year]
                    self.df_datasheet.drop(idx, axis=0, inplace=True)
                    break
            continue

        # print(f'CCF SECTION SUBTOTAL DETAILS | {self.section_subtotal}')

        # return

    def process_operating_bucket(self, section_key='operating_activities'):
        # pdf report sheet dataframe - only operating activities
        df_data = self.df_datasheet[(self.df_datasheet['statement_section'].str.lower() == section_key)]

        self.list_drilldown_flags[section_key] = False

        # db bucket dataframe - only operating activities
        df_bucket = self.df_nlp_bucket_master[
            self.df_nlp_bucket_master['statement_section'].str.lower() == section_key]
                                        
        # print(f'OPERATING ACTIVITY DATA:\n {df_data}')
        # print(f'OPERATING ACTIVITY BUCKET:\n {df_bucket}')

        self.dynamic_bucketing(df_data, df_bucket, section_key)

        net_working_capital = 'net_working_capital'
        adj_sum_yearly = {}
        if net_working_capital in df_data['statement_sub_section'].unique():
            self.list_drilldown_flags[net_working_capital] = False
            df_adjustments = df_data[df_data['statement_sub_section'] == net_working_capital]
            for curr_year in self.years_list:
                adj_sum_yearly[curr_year] = df_adjustments[curr_year].sum(skipna=True)

        # print(f'NET WORKING CAPITAL Operating Activities: {adj_sum_yearly}')

        for bucket_index, bucket_row in self.df_response.iterrows():
            if bucket_row['target_keyword']:
                res_match = self.obj_techfuzzy.token_sort_pro(net_working_capital, [bucket_row['target_keyword']])

                if res_match[0][1] >= 90:
                    for curr_year in self.years_list:
                        # print(f'{type(curr_year)} | ')
                        if curr_year in adj_sum_yearly.keys():
                            self.df_response.at[bucket_index, curr_year] = adj_sum_yearly[curr_year]

                    # LOG NET WORKING CAPITAL IN DRILL-DOWN
                    for idx_nwc, row_nwc in df_data[df_data['statement_sub_section'] == net_working_capital].iterrows():
                        self.ccf_drilldown_items(bucket_row, row_nwc, net_working_capital)
                    break
            continue
        # print('++++++++++++++++++++++++++++++++++++++++++++++++++')
        # return

    def process_investing_bucket(self, section_key='investing_activities'):
        # pdf report sheet dataframe - only investing activities
        df_data = self.df_datasheet[(self.df_datasheet['statement_section'].str.lower() == section_key)]

        self.list_drilldown_flags[section_key] = False

        # db bucket dataframe - only investing activities
        df_bucket = self.df_nlp_bucket_master[
            self.df_nlp_bucket_master['statement_section'].str.lower() == section_key]

        # print(f'INVESTING ACTIVITY DATA:\n {df_data}')
        # print(f'INVESTING ACTIVITY BUCKET:\n {df_bucket}')

        self.dynamic_bucketing(df_data, df_bucket, section_key)

        # print('++++++++++++++++++++++++++++++++++++++++++++++++++')
        # return

    def process_financing_bucket(self, section_key='financing_activities'):
        # pdf report sheet dataframe - only financing activities
        df_data = self.df_datasheet[(self.df_datasheet['statement_section'].str.lower() == section_key)]

        self.list_drilldown_flags[section_key] = False

        # db bucket dataframe - only financing activities
        df_bucket = self.df_nlp_bucket_master[
            self.df_nlp_bucket_master['statement_section'].str.lower() == section_key]

        # print(f'FINANCING ACTIVITY DATA:\n {df_data}')
        # print(f'FINANCING ACTIVITY BUCKET:\n {df_bucket}')

        self.dynamic_bucketing(df_data, df_bucket, section_key)

        # print('++++++++++++++++++++++++++++++++++++++++++++++++++')
        # return

    def dynamic_bucketing(self, df_data, df_bucket, section_key):

        net_bucket_total = {}

        for curr_year in self.years_list:
            print(f'FINANCIAL_YEAR : {curr_year}')

            # GET CURRENT YEAR SUM
            if curr_year in self.section_subtotal[section_key]:
                curr_year_sum = self.section_subtotal[section_key][curr_year]
            else:
                curr_year_sum = df_data[curr_year].sum(skipna=True)

            value_adjusted = float(0)

            # Reset Processing Flag for each year
            df_data['flg_processed'] = False

            # get balancing column index in order to store calculated balance figure in the end
            index_balancing_column = None
            index_subtotal_column = None

            # OUTER LOOP : BUCKET DATAFRAME
            for bucket_index, bucket_row in df_bucket.iterrows():
                print(f'Bucket Keyword : {bucket_row[str("primary_keywords")]}')

                if bucket_row['field_tage'] == 'balancing_value':
                    index_balancing_column = bucket_index
                    continue
                elif bucket_row['field_tage'] == 'subtotal':
                    index_subtotal_column = bucket_index
                    continue

                if bucket_row['fetch_type'] == 'Direct':
                    best_match = self.direct_datafetch(curr_year, bucket_row, df_data)
                else:
                    continue

                print(f'BEST MATCH: {best_match}')
                if len(best_match['data_index']) > 0:
                    df_bucket.at[bucket_index, str(curr_year)] = float(best_match['value'])
                    df_bucket.at[bucket_index, 'score'] = best_match['score']

                    # df_data.at[best_match['data_index'], 'flg_processed'] = True
                    value_adjusted = value_adjusted + best_match['value']
                    if bucket_row['fetch_type'] == 'Direct':
                        for df_idx in best_match['data_index']:
                            df_data.at[df_idx, 'flg_processed'] = True
                    print(f'MATCH FOUND {best_match}')
                else:
                    pass
                    # print(f'NO MATCH FOUND')

                # print('-----------')

            # CALCULATE BALANCING FIGURE MANUALLY - FOR THE YEAR
            print(f'BALANCING INDEX FOUND AT: {index_balancing_column}')
            if index_balancing_column:
                df_bucket.at[index_balancing_column, str(curr_year)] = curr_year_sum - value_adjusted
                df_bucket.at[index_balancing_column, 'score'] = 95

            # SET TOTAL FIGURE MANUALLY - FOR THE YEAR
            print(f'SUBTOTAL INDEX FOUND AT: {index_subtotal_column}')
            if index_subtotal_column:
                df_bucket.at[index_subtotal_column, str(curr_year)] = curr_year_sum
                df_bucket.at[index_subtotal_column, 'score'] = 95
                net_bucket_total[curr_year] = curr_year_sum

            print(
                f'TOTAL SUM: {curr_year_sum} | VALUE ADJUSTED: {value_adjusted} | BALANCING FIGURE : {curr_year_sum - value_adjusted}')
            print('-------------------------')

            self.list_drilldown_flags[section_key] = True

        print('=========================================================')
        print(f'NET SUBTOTAL : {net_bucket_total}')
        print(f'----- AFTER PROCESSING BUCKET DATA ----- \n{df_bucket}')

        df_bucket = self.formula_datafetch(df_bucket)

        self.df_response = self.df_response.append(df_bucket, ignore_index=True)
        # return

    def direct_datafetch(self, curr_year, bucket_row, df_data, fetch_type='direct'):
        # BEST MATCH INDEX AND SCORE
        best_match = {'data_index': [], 'score': 0, 'value': 0, 'label': ''}

        # INNER LOOP : REPORT DATAFRAME
        for data_index, data_row in df_data.iterrows():
            # skip if data value is already found for bucketing
            # print(data_row["Particulars"])
            if data_row['flg_processed']:
                continue

            list_target_keywords = bucket_row['target_keyword'].split('|')
            res_fuzz_match = self.obj_techfuzzy.partial_ratio_pro(data_row["Particulars"], list_target_keywords)

            # print(f'\t\t{res_fuzz_match}')

            if res_fuzz_match[0][1] >= self.conf_score_thresh:
                if bucket_row['field_tage'] == 'positive' and float(data_row[str(curr_year)]) < 0:
                    continue
                elif bucket_row['field_tage'] == 'negative' and float(data_row[str(curr_year)]) > 0:
                    continue
                best_match['value'] += float(data_row[str(curr_year)])
                best_match['score'] = res_fuzz_match[0][1]
                (best_match['data_index']).append(data_index)
                best_match['label'] = data_row[str("Particulars")]
                self.ccf_drilldown_items(bucket_row, data_row)

            continue

        return best_match

    def formula_datafetch(self, df_bucket):
        # print('FORMULA CALCULATION STARTED HERE----------------')
        for curr_year in self.years_list:
            for bucket_index, bucket_row in df_bucket[df_bucket['fetch_type'] == 'Formula'].iterrows():
                if bucket_row['custom_formula'] and bucket_row['custom_formula']!="NULL":
                    custom_formula = bucket_row['custom_formula']
                    # print(custom_formula)
                    for in_index, in_row in df_bucket.iterrows():
                        custom_formula = custom_formula.replace(str(in_row['meta_keyword']),
                                                                str(in_row[str(curr_year)]))
                        continue
                    # print(custom_formula)
                    df_bucket.at[bucket_index, curr_year] = eval(custom_formula)
                continue
        return df_bucket

    def ccf_drilldown_items(self, bucket_row, data_row, statement_section=None):

        if statement_section is None:
            statement_section = bucket_row['statement_section']

        # IF SECTION SUBSECTION RECORDS UPDATED THEN SKIP
        if self.list_drilldown_flags[statement_section]:
            return

        dict_yrs_value = {}

        for curr_year in self.years_list:
            dict_yrs_value[curr_year] = float(data_row[curr_year])

        self.update_drilldown_data(bucket_row['meta_keyword'], data_row["Particulars"], dict_yrs_value)

        return

    def update_drilldown_data(self, meta_keyword, particular, dict_year_vals):

        dict_row = {'meta_keyword': meta_keyword, 'particulars': particular}
        dict_row.update(dict_year_vals)

        self.df_drilldown_gen = self.df_drilldown_gen.append(dict_row, ignore_index=True)
        # return
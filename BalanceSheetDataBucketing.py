import pandas as pd
# from flask import current_app as app
from nltk.stem import PorterStemmer
from main_page_config import keyword_mapping_settings
# from src.modules.data_processing.DataBucketingGeneric import DataBucketingGeneric
import os
from TechMagicFuzzy import TechMagicFuzzy
from DataBucketingUtils import *



class BalanceSheetDataBucketing():
    def __init__(self, df_datasheet, df_nlp_bucket_master,notes_ref_dict,notes_region_meta_data,standardised_cropped_dict,standard_note_meta_dict,transformed_standardised_cropped_dict):
        self.df_datasheet = df_datasheet
        self.df_nlp_bucket_master = df_nlp_bucket_master
        self.df_datasheet_cp = df_datasheet.copy()
        self.notes_ref_dict = notes_ref_dict
        self.notes_region_meta_data = notes_region_meta_data    
        self.standardised_cropped_dict = standardised_cropped_dict
        self.standard_note_meta_dict = standard_note_meta_dict
        self.transformed_standardised_cropped_dict = transformed_standardised_cropped_dict

        self.ps = PorterStemmer()
        self.conf_score_thresh = 80
        self.years_list = []
        self.dict_notes_df = {}
        self.df_response = None
        # self.dict_notes_files = dict_notes_files
        # self.dict_notes_data_pages = {}
        # self.record_dtls = record_dtls
        self.obj_techfuzzy = TechMagicFuzzy()
        self.list_drilldown_flags = {}
        self.bs_bucketing_dict = {}


    def fetch_report(self):
        self.report_data_tuning()
        self.get_CASH_AND_CASH_EQUIVALENTS()
        self.get_INVENTORIES()
        self.get_PREPAID_EXPNS()
        self.get_OTHER_CURR_AST()
        self.get_CURR_AST()
        self.get_ACCUMLATED_DEPRE()
        self.get_NET_PLANT_PRPTY_AND_EQPMNT()
        self.get_OTHER_TANGIBLE_AST()
        self.get_TANGIBLE_AST()
        self.get_GOODWILL()
        self.get_OTHER_INTANGIBLE_AST()
        self.get_INTANGIBLE_AST()
        self.get_INVSTMENT()
        self.get_DEFFERED_CHARGES()
        self.get_OTHER_AST()
        self.get_NON_CURR_AST_TOTAL()
        self.get_SHORT_TERM_DEBT()
        self.get_LONG_TERM_DEBT_DUE_IN_ONE_Y()
        self.get_NOTE_PAYABLE()
        self.get_ACCOUNTS_PAYABLE()
        self.get_ACCURED_EXPNS()
        self.get_TAX_PAYABLE()
        self.get_OTHER_CURR_LIAB()
        self.get_CURR_LIAB()
        self.get_LONG_TERM_DEBT()
        self.get_LONG_TERM_BORROWING()
        self.get_BOND()
        self.get_SUBORDINATE_DEBT()
        self.get_DEFFERED_TAXES()
        self.get_OTHER_LONG_TERM_LIAB()
        self.get_MINORITY_INT()
        self.get_LONG_TERM_LIAB()
        self.get_COMMON_STOCK()
        self.get_ADDITIONAL_PAID_IN_CAPITAL()
        self.get_OTHER_RSRV()
        self.get_RETAINED_EARNINGS()
        self.get_OTHERS()
        self.get_SHAREHOLDERS_EQUITY()
        self.get_TOTAL_LIAB_AND_EQUITY()
        self.get_LIAB_TOTAL()
        self.get_TOTAL_AST()
        self.get_GROS_PLANT_PRPTY_AND_EQPMNT()
        self.get_ACCOUNTS_RECEIVABLES()


        

    def report_data_tuning(self):
        data_column_names = self.df_datasheet.columns.values

        # ignore these columns to fetch years list
        filter_headers = ['Notes', 'Particulars', 'statement_section', 'statement_sub_section']

        # get years list
        years_list = ([int(i) for i in data_column_names if i not in filter_headers])
        years_list.sort()
        self.years_list = [str(i) for i in years_list]


  

    def get_CASH_AND_CASH_EQUIVALENTS(self):
        meta_keywrods = "ca_cash_and_cash_equivalents"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        notes_table_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        main_page_data_indices = []
        main_page_year_total_lst = []
        main_page_raw_note_list = []
        for year in self.years_list:
            print(year)
            main_page_best_match= get_main_page_line_items(df_datasheet=self.df_datasheet,keywords=main_page_targat_keywords,curr_year=year,obj_techfuzzy=self.obj_techfuzzy,conf_score_thresh=self.conf_score_thresh)
            print(f"main_page_best_match:= {main_page_best_match}")
            # main_page_data_indices.append(main_page_best_match.get("data_index"))
            main_page_data_indices = main_page_best_match.get("data_index")
            main_page_year_total_lst.append(main_page_best_match.get("value"))
            # print(list(main_page_best_match.get("label")))
        filtered_standardised_tables_dict,filtered_transformed_standardised_tables_dict,raw_note_list,note_number_list,subnote_number_list,tableid_list = get_notes_tables_from_meta_dict_and_standardized_notes_dict(main_page_best_match=main_page_best_match,notes_reference_dict=self.notes_ref_dict,notes_region_meta_data=self.notes_region_meta_data,standardised_cropped_dict=self.standardised_cropped_dict,trasnformed_standardised_cropped_dict=self.transformed_standardised_cropped_dict,statement_type="cbs")
        print(f"1.raw_note_list: {raw_note_list},note_number_list: {note_number_list},sbnoue: {subnote_number_list},tableid:{tableid_list}")
        temp_df = prepare_df_for_dumping(raw_note_list,note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict)
        notes_table_df = pd.concat([notes_table_df,temp_df],ignore_index=True)
        main_page_raw_note_list = raw_note_list
            # get_notes_pages_line_items()
        temp_dict ={}
        temp_dict["main_page_row_indices"] = main_page_data_indices
        temp_dict["main_page_year_total"] =main_page_year_total_lst
        temp_dict["main_page_raw_note"] =main_page_raw_note_list
        temp_dict["notes_table_df"] = notes_table_df
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_INVENTORIES(self):
        pass

    def get_PREPAID_EXPNS(self):
        pass
    def get_OTHER_CURR_AST(self):
        pass

    def get_CURR_AST(self):
        pass
    def get_ACCUMLATED_DEPRE(self):
        pass
    def get_NET_PLANT_PRPTY_AND_EQPMNT(self):
        pass
    def get_OTHER_TANGIBLE_AST(self):
        pass
    def get_TANGIBLE_AST(self):
        pass
    def get_GOODWILL(self):
        pass
    def get_OTHER_INTANGIBLE_AST(self):
        pass
    def get_INTANGIBLE_AST(self):
        pass
    def get_INVSTMENT(self):
        pass
    def get_DEFFERED_CHARGES(self):
        pass
    def get_OTHER_AST(self):
        pass
    def get_NON_CURR_AST_TOTAL(self):
        pass
    def get_SHORT_TERM_DEBT(self):
        pass
    def get_LONG_TERM_DEBT_DUE_IN_ONE_Y(self):
        pass
    def get_NOTE_PAYABLE(self):
        pass
    def get_ACCOUNTS_PAYABLE(self):
        pass
    def get_ACCURED_EXPNS(self):
        pass
    def get_TAX_PAYABLE(self):
        pass
    def get_OTHER_CURR_LIAB(self):
        pass
    def get_CURR_LIAB(self):
        pass
    def get_LONG_TERM_DEBT(self):
        pass
    def get_LONG_TERM_BORROWING(self):
        pass
    def get_BOND(self):
        pass
    def get_SUBORDINATE_DEBT(self):
        pass
    def get_DEFFERED_TAXES(self):
        pass
    def get_OTHER_LONG_TERM_LIAB(self):
        pass
    def get_MINORITY_INT(self):
        pass
    def get_LONG_TERM_LIAB(self):
        pass
    def get_COMMON_STOCK(self):
        pass
    def get_ADDITIONAL_PAID_IN_CAPITAL(self):
        pass
    def get_OTHER_RSRV(self):
        pass
    def get_RETAINED_EARNINGS(self):
        pass
    def get_OTHERS(self):
        pass
    def get_SHAREHOLDERS_EQUITY(self):
        pass
    def get_TOTAL_LIAB_AND_EQUITY(self):
        pass
    def get_LIAB_TOTAL(self):
        pass
    def get_TOTAL_AST(self):
        pass
    def get_GROS_PLANT_PRPTY_AND_EQPMNT(self):
        pass
    def get_ACCOUNTS_RECEIVABLES(self):
        pass
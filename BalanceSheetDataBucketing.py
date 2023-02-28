import pandas as pd
# from flask import current_app as app
from nltk.stem import PorterStemmer
from main_page_config import keyword_mapping_settings
# from src.modules.data_processing.DataBucketingGeneric import DataBucketingGeneric
import os
from TechMagicFuzzy import TechMagicFuzzy
from DataBucketingUtils import *



class BalanceSheetDataBucketing():
    def __init__(self, df_datasheet, df_nlp_bucket_master,notes_ref_dict,notes_region_meta_data):
        self.df_datasheet = df_datasheet
        self.df_nlp_bucket_master = df_nlp_bucket_master
        self.df_datasheet_cp = df_datasheet.copy()
        self.notes_ref_dict = notes_ref_dict
        self.notes_region_meta_data = notes_region_meta_data    

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
        self.years_list = [i for i in data_column_names if i not in filter_headers]



    def get_CASH_AND_CASH_EQUIVALENTS():
        pass
    def get_INVENTORIES():
        pass

    def get_PREPAID_EXPNS():
        pass
    def get_OTHER_CURR_AST():
        pass

    def get_CURR_AST():
        pass
    def get_ACCUMLATED_DEPRE():
        pass
    def get_NET_PLANT_PRPTY_AND_EQPMNT():
        pass
    def get_OTHER_TANGIBLE_AST():
        pass
    def get_TANGIBLE_AST():
        pass
    def get_GOODWILL():
        pass
    def get_OTHER_INTANGIBLE_AST():
        pass
    def get_INTANGIBLE_AST():
        pass
    def get_INVSTMENT():
        pass
    def get_DEFFERED_CHARGES():
        pass
    def get_OTHER_AST():
        pass
    def get_NON_CURR_AST_TOTAL():
        pass
    def get_SHORT_TERM_DEBT():
        pass
    def get_LONG_TERM_DEBT_DUE_IN_ONE_Y():
        pass
    def get_NOTE_PAYABLE():
        pass
    def get_ACCOUNTS_PAYABLE():
        pass
    def get_ACCURED_EXPNS():
        pass
    def get_TAX_PAYABLE():
        pass
    def get_OTHER_CURR_LIAB():
        pass
    def get_CURR_LIAB():
        pass
    def get_LONG_TERM_DEBT():
        pass
    def get_LONG_TERM_BORROWING():
        pass
    def get_BOND():
        pass
    def get_SUBORDINATE_DEBT():
        pass
    def get_DEFFERED_TAXES():
        pass
    def get_OTHER_LONG_TERM_LIAB():
        pass
    def get_MINORITY_INT():
        pass
    def get_LONG_TERM_LIAB():
        pass
    def get_COMMON_STOCK():
        pass
    def get_ADDITIONAL_PAID_IN_CAPITAL():
        pass
    def get_OTHER_RSRV():
        pass
    def get_RETAINED_EARNINGS():
        pass
    def get_OTHERS():
        pass
    def get_SHAREHOLDERS_EQUITY():
        pass
    def get_TOTAL_LIAB_AND_EQUITY():
        pass
    def get_LIAB_TOTAL():
        pass
    def get_TOTAL_AST():
        pass
    def get_GROS_PLANT_PRPTY_AND_EQPMNT():
        pass
    def get_ACCOUNTS_RECEIVABLES():
        pass
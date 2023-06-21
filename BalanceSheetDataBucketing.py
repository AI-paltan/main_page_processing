import pandas as pd
# from flask import current_app as app
from nltk.stem import PorterStemmer
from main_page_config import keyword_mapping_settings
# from src.modules.data_processing.DataBucketingGeneric import DataBucketingGeneric
import os
from TechMagicFuzzy import TechMagicFuzzy
from DataBucketingUtils import *



class BalanceSheetDataBucketing():
    def __init__(self, df_datasheet, df_nlp_bucket_master,notes_ref_dict,notes_region_meta_data,standardised_cropped_dict,standard_note_meta_dict,transformed_standardised_cropped_dict,month):
        self.df_datasheet = df_datasheet
        self.df_nlp_bucket_master = df_nlp_bucket_master
        self.df_datasheet_cp = df_datasheet.copy()
        self.notes_ref_dict = notes_ref_dict
        self.notes_region_meta_data = notes_region_meta_data    
        self.standardised_cropped_dict = standardised_cropped_dict
        self.standard_note_meta_dict = standard_note_meta_dict
        self.transformed_standardised_cropped_dict = transformed_standardised_cropped_dict
        self.month = month
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

    def get_cdm_item_data_buckets(self,main_page_targat_keywords,df_datasheet,match_type,note_page_include_keywords=[],notes_page_exclude_keywords=[]):
        notes_table_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        main_page_data_indices = []
        main_page_year_total_lst = []
        main_page_raw_note_list = []
        main_page_particular_text_list = []
        main_page_value_list = []
        matched_main_page_df = []
        notes_table_df = []
        temp_horizontal_df = []
        ## clear total keyowrds line items from main pages
        try:
            df_datasheet = remove_total_lines_main_pages(df_datasheet=df_datasheet,filepath=keyword_mapping_settings.mastersheet_filter_particulars,statement_type='cbs',obj_techfuzzy=self.obj_techfuzzy)
            for year in self.years_list:
                # print(year)
                main_page_best_match= get_main_page_line_items(df_datasheet=df_datasheet,keywords=main_page_targat_keywords,curr_year=year,obj_techfuzzy=self.obj_techfuzzy,conf_score_thresh=self.conf_score_thresh,match_type=match_type)
                # print(f"main_page_best_match:= {main_page_best_match}")
                # main_page_data_indices.append(main_page_best_match.get("data_index"))
                main_page_data_indices = main_page_best_match.get("data_index")
                main_page_year_total_lst.append(main_page_best_match.get("value"))
                main_page_particular_text_list = main_page_best_match.get("line_item_label")
                main_page_value_list.append(main_page_best_match.get("line_item_value"))
                # print(list(main_page_best_match.get("label")))
            # print(f"main_page_best_match:= {main_page_best_match}")
            filtered_standardised_tables_dict,filtered_transformed_standardised_tables_dict,raw_note_list,note_number_list,subnote_number_list,tableid_list = get_notes_tables_from_meta_dict_and_standardized_notes_dict(main_page_best_match=main_page_best_match,notes_reference_dict=self.notes_ref_dict,notes_region_meta_data=self.notes_region_meta_data,standardised_cropped_dict=self.standardised_cropped_dict,trasnformed_standardised_cropped_dict=self.transformed_standardised_cropped_dict,statement_type="cbs")
            # print(f"1.raw_note_list: {raw_note_list},note_number_list: {note_number_list},sbnoue: {subnote_number_list},tableid:{tableid_list}")
            # print(f"len of std dict {len(filtered_standardised_tables_dict)} and len of trasnformed std dict: {len(filtered_transformed_standardised_tables_dict)}")
            noted_dict_respnse_after_filtering_keywrods = get_notes_dfDict_after_filtering_keywords(note_number_list=note_number_list,subnote_number_list=subnote_number_list,tableid_list=tableid_list,filtered_transformed_standardised_tables_dict=filtered_transformed_standardised_tables_dict,obj_techfuzzy=self.obj_techfuzzy,conf_score=self.conf_score_thresh,match_type='partial',notes_include_keywords=note_page_include_keywords,notes_exclude_keywords=notes_page_exclude_keywords)
            # temp_df = prepare_df_for_dumping(raw_note_list,note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict)
            # print("new meta dict")
            # print(noted_dict_respnse_after_filtering_keywrods)
            temp_df,temp_horizontal_df = prepare_df_for_dumping2(raw_note_list,note_number_list,subnote_number_list,tableid_list,noted_dict_respnse_after_filtering_keywrods)
            notes_table_df = pd.concat([notes_table_df,temp_df],ignore_index=True)
            main_page_raw_note_list = raw_note_list
            # print(main_page_data_indices)
            matched_main_page_df = get_matched_main_page_df(main_page_data_indices=main_page_data_indices,df=self.df_datasheet)
            ## psoprocess horizontal standardised notes df
            temp_horizontal_df = postprocessing_note_df(std_hrzntl_nte_df=temp_horizontal_df)
            # get_notes_pages_line_items()
        except:
            pass
        temp_dict ={}
        temp_dict["main_page_row_indices"] = main_page_data_indices
        temp_dict["main_page_year_total"] =main_page_year_total_lst
        temp_dict["main_page_year_list"] = self.years_list
        temp_dict["main_page_raw_note"] =main_page_raw_note_list
        temp_dict["main_page_particular_text_list"] = main_page_particular_text_list
        temp_dict["main_page_value_list"] = main_page_value_list
        temp_dict["main_page_cropped_df"] = matched_main_page_df
        temp_dict["notes_table_df"] = notes_table_df
        temp_dict["notes_horizontal_table_df"] = temp_horizontal_df
        return temp_dict
  

    def get_CASH_AND_CASH_EQUIVALENTS(self):
        meta_keywrods = "ca_cash_and_cash_equivalents"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        notes_page_exlude_keywords = get_notes_pages_exclude_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        # notes_table_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        # main_page_data_indices = []
        # main_page_year_total_lst = []
        # main_page_raw_note_list = []
        # for year in self.years_list:
        #     print(year)
        #     main_page_best_match= get_main_page_line_items(df_datasheet=self.df_datasheet,keywords=main_page_targat_keywords,curr_year=year,obj_techfuzzy=self.obj_techfuzzy,conf_score_thresh=self.conf_score_thresh)
        #     print(f"main_page_best_match:= {main_page_best_match}")
        #     # main_page_data_indices.append(main_page_best_match.get("data_index"))
        #     main_page_data_indices = main_page_best_match.get("data_index")
        #     main_page_year_total_lst.append(main_page_best_match.get("value"))
        #     # print(list(main_page_best_match.get("label")))
        # filtered_standardised_tables_dict,filtered_transformed_standardised_tables_dict,raw_note_list,note_number_list,subnote_number_list,tableid_list = get_notes_tables_from_meta_dict_and_standardized_notes_dict(main_page_best_match=main_page_best_match,notes_reference_dict=self.notes_ref_dict,notes_region_meta_data=self.notes_region_meta_data,standardised_cropped_dict=self.standardised_cropped_dict,trasnformed_standardised_cropped_dict=self.transformed_standardised_cropped_dict,statement_type="cbs")
        # print(f"1.raw_note_list: {raw_note_list},note_number_list: {note_number_list},sbnoue: {subnote_number_list},tableid:{tableid_list}")
        # temp_df = prepare_df_for_dumping(raw_note_list,note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict)
        # notes_table_df = pd.concat([notes_table_df,temp_df],ignore_index=True)
        # main_page_raw_note_list = raw_note_list
        #     # get_notes_pages_line_items()
        # temp_dict ={}
        # temp_dict["main_page_row_indices"] = main_page_data_indices
        # temp_dict["main_page_year_total"] =main_page_year_total_lst
        # temp_dict["main_page_raw_note"] =main_page_raw_note_list
        # temp_dict["notes_table_df"] = notes_table_df
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        # print(section,subsection,match_type)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"].str.lower()==section)&(self.df_datasheet["statement_sub_section"].str.lower()==subsection)]
        # print(df_data)
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_INVENTORIES(self):
        meta_keywrods = "ca_inventories"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"].str.lower()==section)&(self.df_datasheet["statement_sub_section"].str.lower()==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_PREPAID_EXPNS(self):
        meta_keywrods = "ca_prepaid_expenses"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_CURR_AST(self):
        meta_keywrods = "ca_other_current_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_CURR_AST(self):
        meta_keywrods = "ca_total_current_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type,note_page_include_keywords=note_page_notes_keywords)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_ACCUMLATED_DEPRE(self):
        meta_keywrods = "nca_accumulated_depreciation"
        # print(meta_keywrods)
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_exclude_note_keywords = get_notes_pages_exclude_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        # print(f"nca_accumulated_depreciation: ", note_page_notes_keywords)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type,note_page_include_keywords=note_page_notes_keywords,notes_page_exclude_keywords=note_page_exclude_note_keywords)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        ### secodn level filtering based on month of annual report
        hrzntl_df = temp_dict["notes_horizontal_table_df"]
        temp_dict["notes_horizontal_table_df"] = second_filter_PPE(std_hrzntl_note_df=hrzntl_df,month=self.month)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_NET_PLANT_PRPTY_AND_EQPMNT(self):
        meta_keywrods = "nca_net_ppe"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_TANGIBLE_AST(self):
        meta_keywrods = "nca_other_tangible_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_TANGIBLE_AST(self):
        meta_keywrods = "nca_tangible_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_GOODWILL(self):
        meta_keywrods = "nca_goodwill"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_INTANGIBLE_AST(self):
        meta_keywrods = "nca_other_intangible_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_INTANGIBLE_AST(self):
        meta_keywrods = "nca_intangible_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_INVSTMENT(self):
        meta_keywrods = "nca_investments"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_DEFFERED_CHARGES(self):
        meta_keywrods = "nca_deffered_charges"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_AST(self):
        meta_keywrods = "nca_other_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_NON_CURR_AST_TOTAL(self):
        meta_keywrods = "nca_total_non_current_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_SHORT_TERM_DEBT(self):
        meta_keywrods = "cl_short_term_debt"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_LONG_TERM_DEBT_DUE_IN_ONE_Y(self):
        meta_keywrods = "cl_long_term_debt_due_in_year"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_NOTE_PAYABLE(self):
        meta_keywrods = "cl_note_payable_debt"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_ACCOUNTS_PAYABLE(self):
        meta_keywrods = "cl_accounts_payable"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_ACCURED_EXPNS(self):
        meta_keywrods = "cl_accrued_expenses"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_TAX_PAYABLE(self):
        meta_keywrods = "cl_tax_payable"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_CURR_LIAB(self):
        meta_keywrods = "cl_other_current_liabilities"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_CURR_LIAB(self):
        meta_keywrods = "cl_total_current_liabilities"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_LONG_TERM_DEBT(self):
        meta_keywrods = "ncl_long_term_debt"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_LONG_TERM_BORROWING(self):
        meta_keywrods = "ncl_long_term_borrowing"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_BOND(self):
        meta_keywrods = "ncl_bond"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_SUBORDINATE_DEBT(self):
        meta_keywrods = "ncl_suboardinate_debt"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_DEFFERED_TAXES(self):
        meta_keywrods = "ncl_deferred_taxes"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_LONG_TERM_LIAB(self):
        meta_keywrods = "ncl_other_long_term_liabilities"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_MINORITY_INT(self):
        meta_keywrods = "ncl_minority_interest"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_LONG_TERM_LIAB(self):
        meta_keywrods = "ncl_long_term_liabilities"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_COMMON_STOCK(self):
        meta_keywrods = "eqt_common_stock"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_ADDITIONAL_PAID_IN_CAPITAL(self):
        meta_keywrods = "eqt_additional_paid_in_capital"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHER_RSRV(self):
        meta_keywrods = "eqt_other_reserves"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_RETAINED_EARNINGS(self):
        meta_keywrods = "eqt_retained_earnings"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_OTHERS(self):
        meta_keywrods = "eqt_others"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_SHAREHOLDERS_EQUITY(self):
        meta_keywrods = "eqt_shareholder_equity"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_TOTAL_LIAB_AND_EQUITY(self):
        meta_keywrods = "total_liability_equity"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_LIAB_TOTAL(self):
        meta_keywrods = "lbt_total_liability"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_TOTAL_AST(self):
        meta_keywrods = "ast_total_assets"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_GROS_PLANT_PRPTY_AND_EQPMNT(self):
        meta_keywrods = "nca_gross_ppe"
        # print(meta_keywrods)
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_exclude_note_keywords = get_notes_pages_exclude_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        # print(f"nca_gross_ppe: ", note_page_notes_keywords)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type,note_page_include_keywords=note_page_notes_keywords,notes_page_exclude_keywords=note_page_exclude_note_keywords)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        ### secodn level filtering based on month of annual report
        hrzntl_df = temp_dict["notes_horizontal_table_df"]
        temp_dict["notes_horizontal_table_df"] = second_filter_PPE(std_hrzntl_note_df=hrzntl_df,month=self.month)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict

    def get_ACCOUNTS_RECEIVABLES(self):
        meta_keywrods = "ca_account_receivables"
        main_page_targat_keywords = get_main_page_keywords(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        note_page_notes_keywords = get_notes_pages_keyowrds(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        section,subsection,match_type = get_section_subsection_matchType(df_nlp_bucket_master=self.df_nlp_bucket_master,df_meta_keyword=meta_keywrods)
        df_data = self.df_datasheet[(self.df_datasheet["statement_section"]==section)&(self.df_datasheet["statement_sub_section"]==subsection)]
        temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords=main_page_targat_keywords,df_datasheet=df_data,match_type=match_type)
        # temp_dict = self.get_cdm_item_data_buckets(main_page_targat_keywords)
        self.bs_bucketing_dict[meta_keywrods] = temp_dict
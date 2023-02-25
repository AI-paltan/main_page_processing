import pandas as pd
from TechMagicFuzzy import TechMagicFuzzy
import re
from fuzzywuzzy import fuzz
from main_page_config import main_page_core_settings
import os
import string


class CCFsections:
    def __init__(self,df) -> None:
        self.cbs_dataframe = df
        self.section_master = {'section': 'statement_section', 'sub_section': 'statement_sub_section'}

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
    
    def string_cleaning(self, str_line):
        remove = string.punctuation
        remove = remove + '\n'
        pattern = r"[{}]".format(remove)  # create the pattern

        # Regular expression to replace "Non - <TEXT>" to "Non-<TEXT>"
        particular_text = re.sub(r'(non)(\s+)(-)(\s+)', r'\1\3', str(str_line), flags=re.IGNORECASE)

        return re.sub(pattern, "", particular_text.strip())
    
    def set_section_details(self, fuzz_thresh=90):
        obj_techfuzzy = TechMagicFuzzy()
        self.clean_before_section_setup()

        dict_main_sections = self.get_keywords_library(main_page_core_settings.ccf_refactor_sections)

        dict_sub_sections = self.get_keywords_library(main_page_core_settings.ccf_refactor_subsections)

        for key, value in self.section_master.items():
            self.cbs_dataframe[value] = None

        curr_section = None
        curr_subsection = None
        self.cbs_dataframe['Particulars'] = self.cbs_dataframe['Particulars'].astype('str')
        for df_index, df_row in self.cbs_dataframe.iterrows():

            # CHOOSE MAIN SECTION
            for key, main_sec_list in dict_main_sections.items():
                res_match = obj_techfuzzy.token_sort_pro(df_row['Particulars'], main_sec_list)
                t_score = res_match[0][1]
                if t_score >= 90:
                    curr_section = key

            # NET WORKING CAPITAL IS SELECTED ONLY FOR OPERATING ACTIVITIES
            if curr_section != 'operating_activities':
                curr_subsection = None

            # CHOOSE SUB SECTION : ADJUSTMENTS
            if curr_subsection is None:
                res_match = obj_techfuzzy.token_sort_pro(df_row['Particulars'],
                                                              dict_sub_sections['adjustments_begin'])
                # app.logger.debug(f'SUB SECTION : ADJUSTMENTS --- {df_row["Particulars"]} | {res_match[0][0]}')
                if res_match[0][1] >= 90:
                    curr_subsection = 'net_working_capital'
            else:
                res_match = obj_techfuzzy.partial_ratio_pro(df_row['Particulars'],
                                                                 dict_sub_sections['adjustments_end'])
                if res_match[0][1] >= 90:
                    curr_subsection = None

            self.cbs_dataframe.at[df_index, self.section_master['section']] = curr_section
            self.cbs_dataframe.at[df_index, self.section_master['sub_section']] = curr_subsection

        return

    def clean_before_section_setup(self):
        obj_techfuzzy = TechMagicFuzzy()
        remove_index = []
        for df_index, df_row in self.cbs_dataframe.iterrows():
            particular_text = self.string_cleaning(df_row['Particulars'])
            res_match = obj_techfuzzy.token_sort_pro(particular_text, ['adjustments for'])

            if res_match[0][1] >= 90:
                # app.logger.debug(f'{particular_text} | {res_match}')
                remove_index.append(df_index)
        # app.logger.debug(f'FILTER DATAFRAME INDICES | {self.statement_type} | {remove_index}')
        self.cbs_dataframe = self.cbs_dataframe.drop(remove_index)
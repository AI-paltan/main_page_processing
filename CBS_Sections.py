import pandas as pd
from TechMagicFuzzy import TechMagicFuzzy
import re
from fuzzywuzzy import fuzz
from main_page_config import main_page_core_settings
import os
import string


class CBSsections:
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

            # if self.statement_type != 'cbs':
            #     return

            dict_statement_sections = self.get_keywords_library(main_page_core_settings.cbs_statement_sections)

            dict_main_sections = self.get_keywords_library(main_page_core_settings.cbs_refactor_sections)
            list_main_sections = []
            for key, value in dict_main_sections.items():
                list_main_sections = list_main_sections + value

            dict_sub_sections = self.get_keywords_library(main_page_core_settings.cbs_refactor_subsections)

            for key, value in self.section_master.items():
                self.cbs_dataframe[value] = None

            curr_section = None
            curr_subsection = None
            for df_index, df_row in self.cbs_dataframe.iterrows():
                # app.logger.debug(f'{df_row["Particulars"]}')

                particular_text = self.string_cleaning(df_row['Particulars'])

                fuzz_res = obj_techfuzzy.token_sort_pro(particular_text, list_main_sections)
                # print(f'{particular_text} | {fuzz_res}')
                if fuzz_res[0][1] >= fuzz_thresh:
                    for key, value in dict_main_sections.items():
                        if fuzz_res[0][0] in value:
                            curr_section = key
                            break

                # section_score = process.extract(particular_text, dict_main_sections, limit=1)
                # if section_score[0][1] >= 90:
                #     # app.logger.debug(f'{df_row["Particulars"]} | {section_score}')
                #     # curr_section = section_score[0][2]
                #     pass

                # app.logger.debug(particular_text)

                # subsection_score = process.extract(particular_text.lower(), self.dict_sub_sections)
                if curr_section is None:
                    continue

                subsection_score = 0
                for key, sub_sec_list in dict_sub_sections.items():
                    # app.logger.debug(f'MATCH KEY {key}')
                    for ss_word in sub_sec_list:
                        t = fuzz.WRatio(ss_word, particular_text)
                        # app.logger.debug(f'particular_text {particular_text} | MATCH WORD {ss_word} | score {t}')
                        if t < 90:
                            continue
                        if t > subsection_score:
                            subsection_score = t
                            # check if sub section comes under the respective section (e.g. equity_liability -> equity)
                            if key in dict_statement_sections[curr_section]:
                                curr_subsection = key

                # if subsection_score[0][1] >= 90:
                #     curr_subsection = subsection_score[0][2]
                #     app.logger.debug(f'{particular_text} | {subsection_score}')

                self.cbs_dataframe.at[df_index, self.section_master['section']] = curr_section
                self.cbs_dataframe.at[df_index, self.section_master['sub_section']] = curr_subsection

            # IF THRESH ATTEMPT 80
            if fuzz_thresh == 80:
                return

            # IF SECTIONING NOT WORKING WITH THRESH 90 THEN ATTEMPT SCORE 80
            if all(x is None for x in self.cbs_dataframe.statement_section.unique()):
                # app.logger.debug('SECTION ATTEMPT 2')
                self.set_section_details(fuzz_thresh=80)
            return

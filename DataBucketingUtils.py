import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import re
import nltk
from typing import Dict


def get_main_page_keywords(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    list_target_keywords = bucket_row['target_keyword'].split('|')
    return list_target_keywords

def get_notes_pages_keyowrds(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    list_target_keywords = bucket_row['note_keyword'].split('|')
    return list_target_keywords

def strip_string_bullets(str_txt,obj_techfuzzy):
        strip_string_bullets_str = obj_techfuzzy.strip_string_bullets(str_txt)
        # remove multi space between words              
        strip_string_bullets_str = re.sub(r'\s\s+', " ", strip_string_bullets_str)
        return strip_string_bullets_str

def get_main_page_line_items(df_datasheet,keywords,curr_year,obj_techfuzzy,match_type):
    best_match = {'data_index': [], 'score': 0, 'value': 0, 'label': [],'note_numbers':[]}
    for data_index, data_row in df_datasheet.iterrows():
            # skip if data value is already found for bucketing
            # app.logger.debug(data_row["Particulars"])
            # if data_row['flg_processed']:
            #     continue
            list_target_keywords = keywords
            txt_particular = strip_string_bullets(data_row["Particulars"],obj_techfuzzy)

            if match_type == 'partial':
                res_fuzz_match = obj_techfuzzy.partial_ratio_pro(txt_particular, list_target_keywords)
            else:
                res_fuzz_match = obj_techfuzzy.token_sort_pro(txt_particular, list_target_keywords)
            # app.logger.debug(f'\t\t{res_fuzz_match}')

            if res_fuzz_match[0][1] >= self.conf_score_thresh:
                # if bucket_row['field_tag'] == 'multisum':
                best_match['value'] += float(data_row[curr_year])
                best_match['score'] = res_fuzz_match[0][1]
                (best_match['data_index']).append(data_index)
                # if len(data_row['Notes']) > 1:
                (best_match['note_numbers']).append(data_row['Notes'])
                # self.cbs_drilldown_items(bucket_row, data_row)
                (best_match['label']).append(data_row[str("Particulars")])

    return best_match


def get_notes_tables_from_meta_dict_and_standardized_notes_dict(main_page_best_match,notes_reference_dict,notes_region_meta_data,standardised_cropped_dict,statement_type):
    # main_page_account_note_numbers = main_page_best_match.get('note_numbers')
    filtered_standardised_tables_dict : Dict = {}
    for account,note in zip(main_page_best_match.get('label'),main_page_best_match.get('note_numbers')):
        if len(note) > 1:
                reference_notes_dict = notes_reference_dict.get(statement_type)
                for refe_notes in reference_notes_dict:
                    if str(account) == str(refe_notes.get('particular')):
                        note = refe_notes.get('main_note_number')[0]
                        subnote = refe_notes.get('subnote_number')[0] 
                        tableid_list = notes_region_meta_data[(notes_region_meta_data['note']==str(note)) & (notes_region_meta_data['subnote']==str(subnote))]['tableid']
                        if len(tableid_list)>1:
                            for tableid in tableid_list:
                                filtered_standardised_tables_dict[tableid] = standardised_cropped_dict.get(tableid)

    return filtered_standardised_tables_dict


def clean_standardised_notes_tables(note_df):
     pass

def get_notes_pages_line_items(df_notes,keywords):
    pass



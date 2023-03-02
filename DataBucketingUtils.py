import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import re
import nltk
from typing import Dict


def get_main_page_keywords(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    # list_target_keywords = []
    # for idx,row in bucket_row.iterrows():
    list_target_keywords = bucket_row['target_keyword'].values[0].split('|')
    return list_target_keywords

def get_notes_pages_keyowrds(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    list_target_keywords = bucket_row['note_keyword'].values[0].split('|')
    return list_target_keywords

def strip_string_bullets(str_txt,obj_techfuzzy):
        strip_string_bullets_str = obj_techfuzzy.strip_string_bullets(str_txt)
        # remove multi space between words              
        strip_string_bullets_str = re.sub(r'\s\s+', " ", strip_string_bullets_str)
        return strip_string_bullets_str

def get_main_page_line_items(df_datasheet,keywords,curr_year,obj_techfuzzy,conf_score_thresh,match_type='partial'):
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

            if res_fuzz_match[0][1] >= conf_score_thresh:
                # if bucket_row['field_tag'] == 'multisum':
                best_match['value'] += float(data_row[curr_year])
                best_match['score'] = res_fuzz_match[0][1]
                (best_match['data_index']).append(data_index)
                # if len(data_row['Notes']) > 1:
                (best_match['note_numbers']).append(data_row['Notes'])
                # self.cbs_drilldown_items(bucket_row, data_row)
                (best_match['label']).append(data_row[str("Particulars")])

    return best_match



def get_notes_tables_from_meta_dict_and_standardized_notes_dict(main_page_best_match,notes_reference_dict,notes_region_meta_data,standardised_cropped_dict,trasnformed_standardised_cropped_dict,statement_type):
    # main_page_account_note_numbers = main_page_best_match.get('note_numbers')
    filtered_standardised_tables_dict : Dict = {}
    filtered_transformed_standardised_tables_dict : Dict = {}
    raw_note_list = []
    note_number_list= []
    subnote_number_list =[]
    tableid_list_main = []
    prcoessed_tabelids = []
    # print(notes_region_meta_data)
    for account,note_nums in zip(main_page_best_match.get('label'),main_page_best_match.get('note_numbers')):
        # print(f"account: {account} and note= {note_nums}")
        # if len(note_nums) >= 1:
        reference_notes_dict = notes_reference_dict.get(statement_type)
        # print(f"reference_notes_dict = {reference_notes_dict}")
        # for account in account_list:
            # print(f"account = {account}")
        
        for refe_notes in reference_notes_dict:
            # print(f"refe_notes = {refe_notes}")
            if str(account) == str(refe_notes.get('particular')):
                # print("yes matched")
                
                for note,subnote in zip(refe_notes.get('main_note_number'),refe_notes.get('subnote_number')):
                    # note = refe_notes.get('main_note_number')
                    # subnote = refe_notes.get('subnote_number')
                    # print(notes_region_meta_data[(notes_region_meta_data['note']==str(note)) & (notes_region_meta_data['subnote']==str(subnote))]['tableid'].values)
                    lst = notes_region_meta_data[(notes_region_meta_data['note']==str(note)) & (notes_region_meta_data['subnote']==str(subnote))]['tableid'].values
                    if len(lst)>0:
                    # tableid_list = notes_region_meta_data[(notes_region_meta_data['note']==str(note)) & (notes_region_meta_data['subnote']==str(subnote))]['tableid'].values[0]
                        tableid_list = list(set(lst[0]))
                        # for idx,tableid_row in tableid_list.iterrows():
                        # print(f"note : {note} nad subnote: {subnote}")
                        # print(f"tableid list : {tableid_list}")
                        # print(len(tableid_list))
                        if len(tableid_list)>=1:
                            for i in range(len(tableid_list)):
                                # print(f"i{i}")
                                
                                tableid = tableid_list[i]
                                if tableid not in prcoessed_tabelids:
                                    # print(f"note : {note} nad subnote: {subnote}")
                                    # print(f"tableid list : {tableid_list}")
                                    # print(f"tableid={tableid}")
                                    combo_key = str(note)+"_"+str(subnote)+"_"+str(tableid)
                                    # print(f"combo key = {combo_key}")
                                    # filtered_standardised_tables_dict[tableid] = standardised_cropped_dict.get(tableid)
                                    # filtered_transformed_standardised_tables_dict[tableid] = trasnformed_standardised_cropped_dict.get(tableid)
                                    filtered_standardised_tables_dict[combo_key] = standardised_cropped_dict.get(combo_key)
                                    # print(filtered_standardised_tables_dict[combo_key])
                                    filtered_transformed_standardised_tables_dict[combo_key] = trasnformed_standardised_cropped_dict.get(combo_key)
                                    # print(filtered_transformed_standardised_tables_dict[combo_key])
                                    raw_note_list.append(refe_notes.get('raw_note_no'))
                                    note_number_list.append(note)
                                    subnote_number_list.append(subnote)
                                    tableid_list_main.append(tableid)
                                    prcoessed_tabelids.append(tableid)
                        

    return filtered_standardised_tables_dict,filtered_transformed_standardised_tables_dict,raw_note_list,note_number_list,subnote_number_list,tableid_list_main


# def convert_standaradised_notes_to_column_row_year(note_df,year_list,standard_note_meta_dict_item):
#     ## this function converts standradised note df into 4 columns. rows will be combination of row header + line item 1 + line item 0
#     ## cols will be combination of col_header_1 + col_header_0 etc. column which contains year value will be dropped based on standard_note_meta_dict_item
#     converted_standardised_df = pd.DataFrame(columns=["rows","columns","year","value"])
#     note_df["year"] = note_df["year"].fillna(note_df["year"].max())
#     # for curr_year in year_list:
#     #         converted_standardised_df[str(curr_year)] = float(0)

#     year_column_header_name = standard_note_meta_dict_item.get('year_column_header_name')
#     if year_column_header_name:
#          note_df = note_df.drop(year_column_header_name,axis=1)
#     line_item_colnames = note_df.filter(like="line_item", axis=1).columns.to_list()
#     col_header_colnames = note_df.filter(like="header_col", axis=1).sort_index(axis=1, ascending=False).columns.to_list()
#     row_header_available = False
#     if "row_header" in note_df.columns.to_list():
#          row_header_available = True
#          row_colmns = ["row_header"]
#          row_colmns.extend(line_item_colnames)
#          converted_standardised_df["rows"] = note_df[row_colmns].fillna('').apply(" ".join, axis=1)
#     else:
#          converted_standardised_df["rows"] = note_df[line_item_colnames].fillna('').apply(" ".join, axis=1)
#     converted_standardised_df["columns"] = note_df[col_header_colnames].fillna('').apply(" ".join, axis=1)
#     converted_standardised_df["year"] = note_df["year"]
#     converted_standardised_df["value"] = note_df["value"]
    
    # converted_standardised_df["rows"] = 

def get_notes_pages_line_items(transformed_standardised_note_df,standardised_note_df,keywords,obj_techfuzzy,conf_score_thresh,match_type="partial"):
    ## tis function will match the given keyword (both included and excluded keyword) and return the indices,value and year and label
    best_match = {'data_index': [], 'score': [], 'value': [], 'label': [],'year':[],'colname_found':[]}
    for data_index, data_row in transformed_standardised_note_df.iterrows():
            # skip if data value is already found for bucketing
            # app.logger.debug(data_row["Particulars"])
            # if data_row['flg_processed']:
            #     continue
            list_target_keywords = keywords
            for col in ["rows","columns"]:
                txt_rows = strip_string_bullets(data_row[col],obj_techfuzzy)

                if match_type == 'partial':
                    res_fuzz_match = obj_techfuzzy.partial_ratio_pro(txt_rows, list_target_keywords)
                else:
                    res_fuzz_match = obj_techfuzzy.token_sort_pro(txt_rows, list_target_keywords)
                # app.logger.debug(f'\t\t{res_fuzz_match}')

                if res_fuzz_match[0][1] >= conf_score_thresh:
                    # if bucket_row['field_tag'] == 'multisum':
                    (best_match['value']).append(float(data_row["value"]))
                    (best_match['score']).append(res_fuzz_match[0][1])
                    (best_match['data_index']).append(data_index)
                    # if len(data_row['Notes']) > 1:
                    # (best_match['note_numbers']).append(data_row['Notes'])
                    # self.cbs_drilldown_items(bucket_row, data_row)
                    (best_match['label']).append(data_row[str("Particulars")])
                    (best_match['colname_found']).append(col)

    return best_match

def filter_notes_row_indices(included_keyword_best_match,excluded_keyword_best_match):
    included_data_indices = set(included_keyword_best_match.get('data_index'))
    excluded_keyowrd_data_indices = set(excluded_keyword_best_match.get('data_index'))
    remaining_data_indices = list(included_data_indices - excluded_keyowrd_data_indices)
    return remaining_data_indices

def prepare_df_for_dumping(raw_note_list,note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict):
    temp_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
    for raw_note,note,subnote,tableid in zip(raw_note_list,note_number_list,subnote_number_list,tableid_list):
        std_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        combo_key = str(note)+"_"+str(subnote)+"_"+str(tableid)
        for key,value in filtered_transformed_standardised_tables_dict.items():
            # _df = filtered_transformed_standardised_tables_dict.get(tableid)
            if key == combo_key:
                _df = value
                std_df["line_item"] =  _df[["columns","rows"]].fillna('').apply(" ".join, axis=1)
                std_df["year"] = _df["year"]
                std_df["value"] = _df["value"]
        std_df["raw_note_no"]=raw_note
        std_df["note_no"]=note
        std_df["subnote_no"]=subnote
        temp_df = pd.concat([temp_df,std_df],ignore_index=True)
    return temp_df
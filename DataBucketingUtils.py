import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import re
import nltk
from typing import Dict
import os
import string


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

def get_main_page_exclude_keywords(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    # list_target_keywords = []
    # for idx,row in bucket_row.iterrows():
    list_target_keywords = bucket_row['exclude_target_keyword'].values[0].split('|')
    return list_target_keywords

def get_notes_pages_exclude_keyowrds(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    list_target_keywords = bucket_row['exclude_note_keyword'].values[0].split('|')
    return list_target_keywords

def get_section_subsection_matchType(df_nlp_bucket_master,df_meta_keyword):
    bucket_row  = df_nlp_bucket_master[df_nlp_bucket_master['meta_keyword']==df_meta_keyword]
    section = bucket_row['statement_section'].values[0]
    subsection = bucket_row['statement_sub_section'].values[0]
    match_type = bucket_row['match_type'].values[0]
    return section,subsection,match_type

def strip_string_bullets(str_txt,obj_techfuzzy):
        strip_string_bullets_str = obj_techfuzzy.strip_string_bullets(str_txt)
        # remove multi space between words              
        strip_string_bullets_str = re.sub(r'\s\s+', " ", strip_string_bullets_str)
        return strip_string_bullets_str

def get_main_page_line_items(df_datasheet,keywords,curr_year,obj_techfuzzy,conf_score_thresh,match_type='partial'):
    best_match = {'data_index': [], 'score': 0, 'value': 0, 'line_item_label': [],'note_numbers':[],'line_item_value':[]}
    datasheet_col_list = df_datasheet.columns.to_list()
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
                if 'Notes' in datasheet_col_list:
                    (best_match['note_numbers']).append(data_row['Notes'])
                # self.cbs_drilldown_items(bucket_row, data_row)
                (best_match['line_item_label']).append(data_row[str("Particulars")])
                (best_match['line_item_value']).append(float(data_row[curr_year]))

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
    for account,note_nums in zip(main_page_best_match.get('line_item_label'),main_page_best_match.get('note_numbers')):
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

def get_notes_pages_line_items(transformed_standardised_note_df,keywords,obj_techfuzzy,conf_score_thresh,match_type="partial"):
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
                # print(f"txt_rows : {txt_rows}")
                if match_type == 'partial':
                    res_fuzz_match = obj_techfuzzy.partial_ratio_pro(txt_rows, list_target_keywords)
                else:
                    res_fuzz_match = obj_techfuzzy.token_sort_pro(txt_rows, list_target_keywords)
                # app.logger.debug(f'\t\t{res_fuzz_match}')
                # print(f"res_fuzz_match : {res_fuzz_match}")
                if res_fuzz_match[0][1] >= conf_score_thresh:
                    # if bucket_row['field_tag'] == 'multisum':
                    (best_match['value']).append(float(data_row["value"]))
                    (best_match['score']).append(res_fuzz_match[0][1])
                    (best_match['data_index']).append(data_index)
                    # if len(data_row['Notes']) > 1:
                    # (best_match['note_numbers']).append(data_row['Notes'])
                    # self.cbs_drilldown_items(bucket_row, data_row)
                    (best_match['label']).append(data_row[col])
                    (best_match['colname_found']).append(col)

    return best_match

def filter_notes_row_indices(included_keyword_best_match,excluded_keyword_best_match):
    included_data_indices = set(included_keyword_best_match.get('data_index'))
    excluded_keyowrd_data_indices = set(excluded_keyword_best_match.get('data_index'))
    remaining_data_indices = list(included_data_indices - excluded_keyowrd_data_indices)
    return remaining_data_indices


def get_notes_dfDict_after_filtering_keywords(note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict,obj_techfuzzy,conf_score,match_type='partial',notes_include_keywords=[],notes_exclude_keywords=[]):
    repsonse_notes_dict = {}
    # print(f"notes_include_keywords : {notes_include_keywords}")
    # print(notes_include_keywords,notes_exclude_keywords)
    if "NULL" in notes_include_keywords:
        notes_include_keywords.remove("NULL")
    if "NULL" in notes_exclude_keywords:
        notes_exclude_keywords.remove("NULL")
    # print(notes_include_keywords,notes_exclude_keywords)
    for note,subnote,tableid in zip(note_number_list,subnote_number_list,tableid_list):
        combo_key = str(note)+"_"+str(subnote)+"_"+str(tableid)
        for key,value in filtered_transformed_standardised_tables_dict.items():
            # _df = filtered_transformed_standardised_tables_dict.get(tableid)
            if key == combo_key:
                _df = value
                # print(f"_df : {_df}")
                
                if len(notes_include_keywords)>0:
                    include_best_match = get_notes_pages_line_items(transformed_standardised_note_df=_df,keywords=notes_include_keywords,obj_techfuzzy=obj_techfuzzy,conf_score_thresh=conf_score,match_type=match_type)
                    if len(notes_exclude_keywords) > 0:
                        exclude_best_match = get_notes_pages_line_items(transformed_standardised_note_df=_df,keywords=notes_exclude_keywords,obj_techfuzzy=obj_techfuzzy,conf_score_thresh=conf_score,match_type=match_type)
                        data_indices = filter_notes_row_indices(included_keyword_best_match=include_best_match,excluded_keyword_best_match=exclude_best_match)
                    else:
                        data_indices = include_best_match.get('data_index')
                    remain_notes_df = _df.iloc[data_indices]
                    repsonse_notes_dict[combo_key] = remain_notes_df
                else:
                    repsonse_notes_dict[combo_key] = _df

    return repsonse_notes_dict

def prepare_df_for_dumping(raw_note_list,note_number_list,subnote_number_list,tableid_list,filtered_transformed_standardised_tables_dict):
    temp_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
    for raw_note,note,subnote,tableid in zip(raw_note_list,note_number_list,subnote_number_list,tableid_list):
        std_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        combo_key = str(note)+"_"+str(subnote)+"_"+str(tableid)
        for key,value in filtered_transformed_standardised_tables_dict.items():
            # _df = filtered_transformed_standardised_tables_dict.get(tableid)
            if key == combo_key:
                _df = value
                if len(_df)>0:
                    std_df["line_item"] =  _df[["columns","rows"]].fillna('').apply(" ".join, axis=1)
                    std_df["year"] = _df["year"]
                    std_df["value"] = _df["value"]
        std_df["raw_note_no"]=raw_note
        std_df["note_no"]=note
        std_df["subnote_no"]=subnote
        temp_df = pd.concat([temp_df,std_df],ignore_index=True)
    return temp_df

def prepare_df_for_dumping2(raw_note_list,note_number_list,subnote_number_list,tableid_list,noted_dict_respnse_after_filtering_keywrods):
    temp_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
    temp_horizontal_df = pd.DataFrame()
    for raw_note,note,subnote,tableid in zip(raw_note_list,note_number_list,subnote_number_list,tableid_list):
        std_df = pd.DataFrame(columns=["raw_note_no","note_no","subnote_no","line_item","year","value"])
        combo_key = str(note)+"_"+str(subnote)+"_"+str(tableid)
        for key,value in noted_dict_respnse_after_filtering_keywrods.items():
            # _df = filtered_transformed_standardised_tables_dict.get(tableid)
            if key == combo_key:
                _df = value
                if len(_df)>0:
                    std_df["line_item"] =  _df[["columns","rows"]].fillna('').apply(" ".join, axis=1)
                    std_df["year"] = _df["year"]
                    std_df["value"] = _df["value"]
                    horizontal_note_df = convert_note_df_to_hotizontal(std_df)
                    temp_horizontal_df = temp_horizontal_df.append(horizontal_note_df)
        std_df["raw_note_no"]=raw_note
        std_df["note_no"]=note
        std_df["subnote_no"]=subnote
        temp_df = pd.concat([temp_df,std_df],ignore_index=True)
    return temp_df,temp_horizontal_df


def convert_note_df_to_hotizontal(note_df):
    years = sorted(list(note_df.year.unique()))
    years = map(int, years)
    col_list = ["line_item"]
    col_list.extend(years)
    new_horizontal_note_df = pd.DataFrame(columns=col_list)
    for idx,row in note_df.iterrows():
        if row["line_item"] in set(new_horizontal_note_df['line_item']):
            new_horizontal_note_df.loc[new_horizontal_note_df.line_item == row["line_item"],row["year"]] = row["value"]
        else:
            tmp_df = dict.fromkeys(col_list)
            tmp_df["line_item"] =  row["line_item"]
            tmp_df[row["year"]] = row["value"]
            new_horizontal_note_df = new_horizontal_note_df.append(tmp_df, ignore_index=True)
    return new_horizontal_note_df


def get_matched_main_page_df(main_page_data_indices,df):
    # if len(main_page_data_indices)>0:
    # print(df)
    matched_main_page_df = df.iloc[main_page_data_indices]
    # else:
    #     matched_main_page_df = df
    return matched_main_page_df


def clean_note_df(std_horzntl_note_df):
    # patterns = ["consolidated","$000","$"]
    patterns = ["consolidated","$000","$00","$"]
    for pattrn in patterns:
        std_horzntl_note_df["line_item"] = std_horzntl_note_df["line_item"].str.replace(re.escape(pattrn),'',flags=re.IGNORECASE)
        # print(f"pattern: {pattrn}")
        # print(std_horzntl_note_df["line_item"])
    return std_horzntl_note_df


def remove_total_line_items(std_horzntl_note_df):
    remove_indices = []
    for idx,row in std_horzntl_note_df.iterrows():
        if "total" in row["line_item"].lower():
            remove_indices.append(idx)
    if len(remove_indices)>0:
        std_horzntl_note_df.drop(remove_indices,inplace=True)
    std_horzntl_note_df.reset_index(drop=True,inplace=True)
    return std_horzntl_note_df

def remove_0_value_line_items(std_horzntl_note_df):
    year_cols = [i for i in std_horzntl_note_df.columns if i not in ["line_item"]]
    std_horzntl_note_df[year_cols] = std_horzntl_note_df[year_cols].fillna(value=0)
    remove_indics = []
    for idx,row in std_horzntl_note_df.iterrows():
        sum = 0
        for year in year_cols:
            sum = sum+row[year]
        if sum == 0:
            remove_indics.append(idx)
    if len(remove_indics)>0:
        std_horzntl_note_df.drop(remove_indics,inplace=True)
    std_horzntl_note_df.reset_index(drop=True,inplace=True)

    return std_horzntl_note_df


def postprocessing_note_df(std_hrzntl_nte_df):
    if len(std_hrzntl_nte_df) > 0:
        std_hrzntl_nte_df = remove_0_value_line_items(std_horzntl_note_df=std_hrzntl_nte_df)
        std_hrzntl_nte_df = remove_total_line_items(std_horzntl_note_df=std_hrzntl_nte_df)
        std_hrzntl_nte_df = clean_note_df(std_horzntl_note_df=std_hrzntl_nte_df)
    return std_hrzntl_nte_df


def get_keywords_library(filepath):
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
    
def string_cleaning(str_line):
        remove = string.punctuation
        remove = remove + '\n'
        pattern = r"[{}]".format(remove)  # create the pattern

        # Regular expression to replace "Non - <TEXT>" to "Non-<TEXT>"
        particular_text = re.sub(r'(non)(\s+)(-)(\s+)', r'\1\3', str(str_line), flags=re.IGNORECASE)

        return re.sub(pattern, "", particular_text.strip())


def remove_total_lines_main_pages(df_datasheet,filepath,statement_type,obj_techfuzzy):
        remove_particulars = get_keywords_library(filepath)
        remove_particulars = remove_particulars[statement_type]

        remove_index = []
        for df_index, df_row in df_datasheet.iterrows():
            particular_text = string_cleaning(df_row['Particulars'])

            if statement_type == 'ccf':
                res_match = obj_techfuzzy.token_set_pro(particular_text, remove_particulars)
            else:
                res_match = obj_techfuzzy.token_sort_pro(particular_text, remove_particulars)

            if res_match[0][1] >= 90:
                remove_index.append(df_index)


        df_datasheet = df_datasheet.drop(remove_index)

        # app.logger.debug(f'AFTER PARTICULAR KEYWORDS FILTER | {self.statement_type} | \n {self.df_pdfdata}')

        return df_datasheet


### sepecific utility fucntion for particular line items in particular statement type

def second_filter_PPE(std_hrzntl_note_df,month):
    ## this function will filter PPE note further for month of given annual statemnt
    month_indices = []
    for idx,row in std_hrzntl_note_df.iterrows():
        if month in row["line_item"].lower():
            month_indices.append(idx)
    # print(month_indices)
    if len(month_indices)>0:
        std_hrzntl_note_df = std_hrzntl_note_df.iloc[month_indices]
    std_hrzntl_note_df.reset_index(drop=True,inplace=False)
    return std_hrzntl_note_df


    
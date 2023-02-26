import pandas as pd
import numpy as np
import re
import openpyxl
from dateutil import parser
from typing import List
from datetime import date
from functools import reduce
from typing import Optional


def find_column_numbers(df):
    col_len = len(df.columns)
    return col_len


def get_note_column(df):
    notes_regex = '(?:note(?:|s))'
    note_row_num = -1
    note_col_num = -1
    for idx,row in df.iterrows():
        bool_row = row.str.contains(notes_regex, flags=re.IGNORECASE,regex=True)
        if bool_row.any():
            note_row_num = idx
            res = [i for i, val in enumerate(bool_row) if val==True]
            note_col_num = res[0]
            break
    return note_row_num,note_col_num

def get_years_and_positions_with_notes(df,notes_indices):
    def get_regex_year(val):
        year_val = -1
        regex = '(\d{4}|\d{2})'
        match = re.findall(regex, val)
        match.sort(key=len, reverse=True) 
        if len(match) > 0:
            for value in match:
                if len(value) == 4:
                    year_val = value
                elif len(value) == 2:
                    year_val = '20'+str(value)
                if int(year_val) <= int(date.today().year):
                    return str(year_val)        
        else:
            return year_val
    
    note_x = notes_indices[0]
    note_y = notes_indices[1]
    year_list: list = []
    year_indices: List(List) = []
    raw_year_text:list = []
    for idx,row in df.iterrows():
        if (note_x-2) <= idx <= (note_x+2):
            for col_idx, item in row.iteritems():
                if col_idx > note_y:
                    try:
                        year = parser.parse(str(item), fuzzy=True).year
                    except:
                        year = get_regex_year(str(item))
                    if int(year) > 0:
                        year_list.append(int(year))
                        year_indices.append([idx,col_idx])
                        raw_year_text.append(item)
        if len(year_list) == (len(df.columns) - note_y-1):
            break
    return year_list,year_indices,raw_year_text


def get_years_and_positions_without_notes(df):
    ## this is without notes column
    def get_regex_year(val):
        year_val = -1
        regex = '(\d{4}|\d{2})'
        match = re.findall(regex, val)
        match.sort(key=len, reverse=True) 
        if len(match) > 0:
            for value in match:
                if len(value) == 4:
                    year_val = value
                elif len(value) == 2:
                    year_val = '20'+str(value)
                if int(year_val) <= int(date.today().year):
                    return str(year_val)        
        else:
            return year_val

    year_list: list = []
    year_indices: List(List) = []
    raw_year_text:list = []
    for idx,row in df.iterrows():
        # if (note_x-2) < idx < (note_x+2):
        for col_idx, item in row.iteritems():
            if col_idx > 0:
                try:
                    year = parser.parse(str(item), fuzzy=True).year
                except:
                    year = get_regex_year(str(item))
                if year and int(year) > 0:
                    year_list.append(int(year))
                    year_indices.append([idx,col_idx])
                    raw_year_text.append(item)
        if len(year_list) == 2:
            break
    return year_list,year_indices,raw_year_text


def get_data_chunk_span_with_notes(df,notes_indices,years_indices):
    # def get_max_from_nested_list(lista)
    notes_x = notes_indices[0]
    notes_y = notes_indices[1]
    max_year_x = list(np.max(np.array(years_indices),axis=0))[0]#max(years_indices,key=max)[0]
    min_year_y = list(np.min(np.array(years_indices),axis=0))[1]#min(years_indices,key=min)[1]
    max_year_y = list(np.max(np.array(years_indices),axis=0))[1]#max(years_indices,key=max)[1]
    max_header = max([notes_x,max_year_x])
    data_start_x = -1
    particulars_y = -1
    data_start_y = -1
    data_end_y = -1
    for i in range(max_header+1,len(df)):
        if not pd.isnull(df.loc[i,notes_y-1]):
            data_start_x = i
            particulars_y = notes_y-1
            data_start_y = min_year_y
            data_end_y = max_year_y
            break
    return data_start_x,data_start_y,data_end_y,particulars_y
    

def get_data_chunk_span_without_notes(df,years_indices):
    max_year_x = list(np.max(np.array(years_indices),axis=0))[0]#max(years_indices,key=max)[0]
    min_year_y = list(np.min(np.array(years_indices),axis=0))[1]#min(years_indices,key=min)[1]
    max_year_y = list(np.max(np.array(years_indices),axis=0))[1]#max(years_indices,key=max)[1]
    max_header = max_year_x
    data_start_x = -1
    particulars_y = -1
    data_start_y = -1
    data_end_y = -1
    for i in range(max_header+1,len(df)):
        if not pd.isnull(df.loc[i,min_year_y-1]):
            data_start_x = i
            particulars_y = min_year_y-1
            data_start_y = min_year_y
            data_end_y = max_year_y
            break
    return data_start_x,data_start_y,data_end_y,particulars_y

def split_numbers(number,threshold=60):
    ### this function is ued to split combined noted number
    #  eg: 1214 -> 12,14
    def split_by_n( seq, n ):
        """A generator to divide a sequence into chunks of n units."""
        seq = str(seq)
        while seq:
            yield int(seq[:n])
            seq = seq[n:]
    num_list = []
    if len(number) <= 3:
        if int(number[1:]) > threshold:
            num_list.append(number[0:2])
            num_list.append(number[2:])
        else:
            num_list.append(number[0])
            num_list.append(number[1:])
    else:
        number_split = list(split_by_n(number,2))
        for split in number_split:
            if split>threshold:
                more_split = list(split_by_n(split,1))
                num_list.extend(more_split)
            else:
                num_list.extend([split])
    return num_list

def find_note_subnote_number(number):
    note = ''
    subnote = ''
    if bool(re.match(r'\d+.\d+',str(number))):
        note = str(number).split('.')[0]
        subnote = str(number).split('.')[1]
    elif bool(re.match(r'\d+\(\w+\)',str(number))):
            note = str(number).split('(')[0]
            subnote = "(" + str(number).split('(')[1]
    elif bool(re.match(r'\d+[A-Za-z]+',str(number))):
            res = re.findall(r'[A-Za-z]+|\d+', str(number))
            note = res[0]
            subnote = res[1]
    elif bool(re.match(r'\d+',str(number))):
            note = number
            subnote = ''
    return note,subnote

def get_note_pattern(note,subnote):
    if str(subnote).isnumeric():
        note_pattern = str(note)+'.'+str(subnote)
    else:
        note_pattern = str(note)+str(subnote)
    return note_pattern

def notes_number_processing(df,notes_indices,data_start_x,particulars_y,notes_dict):
    
    ###r"and|[\s,-]+" to split by (and comma space and hypen)
    # notes_col = df.iloc[notes_indices[0]+1:,notes_indices[1]]
    notes_col = df['Notes']
    # particulars_col = df.iloc[notes_indices[0]+1:,particulars_y]
    particulars_col = df['Particulars']
    ref_list : list = []
    for idx,val in enumerate(notes_col):
        notes_list = []
        if not pd.isnull(val):
            if len(str(val)) > 2 and str(val).isdigit():
                split_notes_list = split_numbers(val,60)
                notes_list = split_notes_list
            elif ',' in str(val):
                split_notes_list = re.split(r',',str(val))
                notes_list = split_notes_list
            elif 'and' in str(val):
                split_notes_list = re.split(r'and',str(val))
                notes_list = split_notes_list
            else:
                 notes_list = [str(val)]
            note_no : list= []
            subnote_no : list = []
            for raw_note in notes_list:
                note,subnote = find_note_subnote_number(str(raw_note))
                note_no.extend([note])
                subnote_no.extend([subnote])
            temp_dict = {}
            temp_dict['particular'] = particulars_col.iloc[idx]
            temp_dict['raw_note_no'] = val
            temp_dict['processed_raw_note'] = notes_list
            temp_dict['main_note_number']=note_no
            temp_dict['subnote_number'] = subnote_no
            ref_list.append(temp_dict)
            # print(note_no)
            for noteno,subnoteno in zip(note_no,subnote_no):
                if notes_dict.get(noteno, {}).get(subnoteno):
                    notes_dict[noteno][subnoteno].append(particulars_col.iloc[idx])
                else:
                    notes_dict[noteno][subnoteno] = [particulars_col.iloc[idx]]
    # print("ref list:", ref_list)
    return ref_list,notes_dict


def number_data_processing(df,data_start_x,data_start_y,data_end_y):
    def clean_number(number):
        number = str(number).replace(r',',"")
        number = str(number).replace(r')',"")
        number = str(number).replace(r'(',"-")
        return number
    def split_merge_rows(row):
        pass
    for i in range(data_start_y,data_end_y+1):
        df.iloc[data_start_x:,i] = df.iloc[data_start_x:,i].apply(clean_number).apply(pd.to_numeric , errors='coerce')
#     for idx,row in df.iterrows()
    return df


def set_headers(df,data_start_x,data_end_y,headers):
    subset_df = df.iloc[data_start_x:,:]
    subset_df.columns = headers
    subset_df = subset_df.reset_index(drop=True)
    return subset_df
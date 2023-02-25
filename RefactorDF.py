import pandas as pd
import numpy as np
from utils import *


class RefactorCBS:
    def __init__(self,df) -> None:
        self.df = df

    
    def start_refactoring(self):
        col_num = self.find_template()
        if col_num == 4 :
            standard_df, temp_df = self.ideal_format_processing()
        if col_num == 3:
            standard_df, temp_df = self.non_ideal_format_without_notes_processing()
        if col_num == 6:
            standard_df, temp_df = self.non_ideal_format_processing()
        return standard_df, temp_df

        
    def find_template(self):
        col_number = find_column_numbers(self.df)
        return col_number
        

    def ideal_format_processing(self):
        # print(self.df)
        note_row_num,note_col_num = get_note_column(self.df)
        year_list,year_indices,raw_year_text = get_years_and_positions_with_notes(df=self.df,notes_indices=[note_row_num,note_col_num])
        # print( note_row_num,note_col_num)
        # print(year_list,year_indices,raw_year_text)
        data_start_x,data_start_y,data_end_y,particulars_y = get_data_chunk_span_with_notes(df=self.df,notes_indices=[note_row_num,note_col_num],years_indices= year_indices)
        # print(self.df)
        # print(data_start_x,data_start_y,data_end_y,particulars_y)
        # print(year_list,year_indices,raw_year_text)
        # print(note_row_num,note_col_num)
        df_numeric = number_data_processing(df=self.df,data_start_x= data_start_x,data_start_y= data_start_y,data_end_y= data_end_y)
        headers = ['Particulars','Notes',year_list[0],year_list[1]]
        standard_df = set_headers(df_numeric,data_start_x,data_end_y,headers)
        temp_df = {}
        temp_df["note_col_x"] = note_row_num
        temp_df["note_col_y"] = note_col_num
        temp_df["year_list"] = year_list
        temp_df["year_indices"] = year_indices
        temp_df["raw_text_year"] = raw_year_text
        temp_df["data_start_x"] = data_start_x
        temp_df["data_satrt_y"] = data_start_y
        temp_df["data_end_y"] = data_end_y
        temp_df["particulars_y"] = particulars_y
        temp_df["headers"] = headers
        temp_df["total_columns"] = 4
        return standard_df, temp_df

    def non_ideal_format_without_notes_processing(self):
        year_list,year_indices,raw_year_text = get_years_and_positions_without_notes(df=self.df)
        data_start_x,data_start_y,data_end_y,particulars_y = get_data_chunk_span_without_notes(df=self.df,years_indices=year_indices)
        # print(self.df)
        # print(data_start_x,data_start_y,data_end_y,particulars_y)
        # print(year_list,year_indices,raw_year_text)
        df_numeric = number_data_processing(df=self.df,data_start_x= data_start_x,data_start_y= data_start_y,data_end_y= data_end_y)
        headers = ['Particulars',year_list[0],year_list[1]]
        standard_df = set_headers(df_numeric,data_start_x,data_end_y,headers)
        temp_df = {}
        # temp_df["note_col_x"] = note_row_num
        # temp_df["note_col_y"] = note_col_num
        temp_df["year_list"] = year_list
        temp_df["year_indices"] = year_indices
        temp_df["raw_text_year"] = raw_year_text
        temp_df["data_start_x"] = data_start_x
        temp_df["data_satrt_y"] = data_start_y
        temp_df["data_end_y"] = data_end_y
        temp_df["particulars_y"] = particulars_y
        temp_df["headers"] = headers
        temp_df["total_columns"] = 3
        return standard_df , temp_df

    def non_ideal_format_processing(self):
        self.df = self.df.iloc[:,4]  # select first 4 columns
        standard_df,temp_df = self.ideal_format_processing()
        temp_df["total_columns"] = 6
        return standard_df,temp_df
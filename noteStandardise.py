from note_standardise_utils import *
from typing import Dict



class NoteStandardised:
    def __init__(self,cropped_table_dict) -> None:
        self.cropped_table_dict = cropped_table_dict
        self.standard_note_df : Dict = {}
        # self.final_df = pd.DataFrame
        # self.main_sheet_num_cols = main_sheet_num_cols

    def trigger_job(self):
        self.ideal_template_processing()
        # if len(self.note_df)>0:
        #     note_df = note_df.dropna(axis = 1, how = 'all').T.reset_index(drop=True).T
        #     columns_number,row_number,raw_text,extracted_year = find_date_location(note_df)
        #     data_row_coords,particular_end_col,particular_start_row = find_data_block_location(note_df=note_df.copy(),date_block_coordinates=(columns_number,row_number))
        #     header_indices = find_col_headers(note_df,data_row_coords,particular_end_col,particular_start_row)
        #     nte_df,particular_end_col = check_and_remove_duplicate_particulars_column(note_df,particular_end_col,particular_start_row)
        #     databox_end_coordinates = (len(nte_df.columns)-1,len(nte_df)-1)
        #     row_header_indices = find_row_headers(nte_df,particular_end_col,particular_start_row)
        #     fill_missing_multilevel_header_df = fill_missing_multilevel_header(nte_df,header_indices,particular_end_col)
        #     row_header_to_columns_df = convert_row_header_to_columns(fill_missing_multilevel_header_df,row_header_indices,particular_start_row)
        #     fin_df = convert_col_header_to_columns(row_header_to_columns_df,header_indices,particular_end_col,particular_start_row,databox_end_coordinates)
        #     self.final_df = set_year_column_for_final_df(fin_df,(columns_number,row_number),header_indices)

    def ideal_template_processing(self):
        for key,note_df in self.cropped_table_dict.items():
            if len(note_df)>0:
                final_df = pd.DataFrame()
                try:
                    note_df = note_df.dropna(axis = 1, how = 'all').T.reset_index(drop=True).T
                    columns_number,row_number,raw_text,extracted_year = find_date_location(note_df)
                    data_row_coords,particular_end_col,particular_start_row = find_data_block_location(note_df=note_df.copy(),date_block_coordinates=(columns_number,row_number))
                    header_indices = find_col_headers(note_df,data_row_coords,particular_end_col,particular_start_row)
                    nte_df,particular_end_col = check_and_remove_duplicate_particulars_column(note_df,particular_end_col,particular_start_row)
                    databox_end_coordinates = (len(nte_df.columns)-1,len(nte_df)-1)
                    row_header_indices = find_row_headers(nte_df,particular_end_col,particular_start_row)
                    fill_missing_multilevel_header_df = fill_missing_multilevel_header(nte_df,header_indices,particular_end_col)
                    row_header_to_columns_df = convert_row_header_to_columns(fill_missing_multilevel_header_df,row_header_indices,particular_start_row)
                    fin_df = convert_col_header_to_columns(row_header_to_columns_df,header_indices,particular_end_col,particular_start_row,databox_end_coordinates)
                    final_df = set_year_column_for_final_df(fin_df,(columns_number,row_number),header_indices)
                except Exception e:
                    print(e)
                self.standard_note_df[key] = final_df


    def non_ideal_template_processing(self):
        pass
import cv2
import os
import pandas as pd
import numpy as np
import re
from database import get_db, get_db1
import db_models
from typing import Dict,List,Any,Optional
from note_utils import *

db = get_db1()


class getNotesDataTables:
    def __init__(self,fileid,notes_dict,max_main_page) -> None:
        self.fileid = fileid
        self.final_notes_dict: Dict = notes_dict
        self.max_main_page = max_main_page
        self.ocr_line_df_dict:dict = {}
        self.notes_span_df = pd.DataFrame()
        self.cropped_table_dict :Dict = {}

    def trigger_job(self):
        self.prepare_ocr_line_df()
        self.findNotesArea()
        self.getTableData()

    def prepare_ocr_line_df(self):
        page_query = db.query(db_models.PageLogs).filter(db_models.PageLogs.fileid == self.fileid).order_by(db_models.PageLogs.time.desc())
        pages = page_query.all()
        processed_pages = []
        for page in pages:
            if page.page_number not in processed_pages:
                ocr_query = db.query(db_models.OCRDump).filter(db_models.OCRDump.pageid == page.pageid).order_by(db_models.OCRDump.time.desc())
                ocr_df = pd.read_sql(ocr_query.statement, ocr_query.session.bind)
                ocr_line_df = ocr_dump_to_line_df(ocr_df)
                self.ocr_line_df_dict[page.page_number] = ocr_line_df
                processed_pages.append(page.page_number)


    def findNotesArea(self):
        note_span_list = []
        for note,v in self.final_notes_dict.items():
            for subnote, q in v.items():
                for account in q:
                    # note_pattern = str(note)+str(subnote)
                    note_pattern = get_note_pattern(note,subnote)
                    notes_pages,notes_start_bbox= find_note_start_index(note_pattern,account,self.ocr_line_df_dict,self.max_main_page)
                    notes_pages,notes_start_bbox = refinement(notes_pages,notes_start_bbox)
                    next_note,next_subnote = find_next_note_subnote(note,subnote)
                    # next_note_pattern = next_note+next_subnote
                    if len(subnote)>0:
                        next_note_pattern = get_note_pattern(note,next_subnote)
                        notes_end_page,notes_end_bbox = find_note_end_index(notes_pages,notes_start_bbox,self.ocr_line_df_dict,next_note_pattern)
                    else:
                        next_note_pattern = next_note
                        notes_end_page,notes_end_bbox = find_note_end_index(notes_pages,notes_start_bbox,self.ocr_line_df_dict,next_note_pattern)                
                    notes_end_page,notes_end_bbox = refinement(notes_end_page,notes_end_bbox)
                    # print(f"note_patter:{note_pattern} {account}")
                    # print("notes pages and notes start bbox: ",notes_pages,notes_start_bbox)
                    # print("nextnote pattern: ",next_note_pattern)
                    # print("notes_endpage and notes end bbox:",notes_end_page,notes_end_bbox )
                    if len(notes_pages)<=0:
                        if len(subnote) > 0:
                            print(f"{subnote} {account}")
                            note_pattern = str(subnote)
                            notes_pages,notes_start_bbox= find_note_start_index(note_pattern,account,self.ocr_line_df_dict,self.max_main_page)
                            next_note,next_subnote = find_next_note_subnote(note,subnote)
                            next_note_pattern = next_subnote
                            notes_end_page,notes_end_bbox = find_note_end_index(notes_pages,notes_start_bbox,self.ocr_line_df_dict,next_note_pattern)
                            if len(notes_end_page)<=0:
                                next_note_pattern = next_note
                                notes_end_page,notes_end_bbox = find_note_end_index(notes_pages,notes_start_bbox,self.ocr_line_df_dict,next_note_pattern)
                            if len(notes_end_page)<=0:
                                pass
                        # print(f"subnote note_patter:{note_pattern} {account}")
                        # print("subnote notes pages and notes start bbox: ",notes_pages,notes_start_bbox)
                        # print("subote nextnote and next subnote: ",next_note,next_subnote)
                        # print("subnote notes_endpage and notes end bbox:",notes_end_page,notes_end_bbox )
                    tmp_lst:list = []
                    tmp_lst.append(note)
                    tmp_lst.append(subnote)
            #             tmp_lst.append(account)
                    tmp_lst.append(notes_pages)
                    tmp_lst.append(notes_start_bbox)
                    tmp_lst.append(next_note_pattern)
                    tmp_lst.append(notes_end_page)
                    tmp_lst.append(notes_end_bbox)
                    note_span_list.append(tmp_lst)
            notes_span_df = pd.DataFrame(note_span_list,columns=["note","subnote","start_page","star_bbox","next_note_pattern","end_page","end_bbox"])
            notes_span_df_cleaned = notes_span_df.loc[notes_span_df[["note","subnote","start_page","star_bbox","end_page","end_bbox"]].astype(str).drop_duplicates().index].reset_index(drop=True)
            self.notes_span_df = notes_span_df_cleaned

    def getTableData(self):
        self.notes_span_df['tableid'] = None
        self.notes_span_df['row_numbers'] = None
        self.notes_span_df['tableslist'] = None
        for idx,row in self.notes_span_df.iterrows():
            if len(row['start_page']) > 0:
                # print(row['note'],row['subnote'])
                # print(row['start_page'][0],row['star_bbox'][0],row['end_page'][0],row['end_bbox'][0])
                table_list,row_numbers = self.get_notes_tables(self.fileid,row['start_page'][0],row['star_bbox'][0],row['end_page'][0],row['end_bbox'][0])
                processed_tables = []
                # print(table_list,row_numbers)
                append_table_list = []
                append_row_num_list = []
                append_tableid_list = []
                for table,row_number in zip(table_list,row_numbers):
                    if table.tableid not in processed_tables:
                        table_df = pd.read_html(table.html_string)[0]
                        unique_rows = list(np.array(list(set(row_number)))-1)
                        if len(unique_rows) > 1:
                            cropped_df = table_df.iloc[unique_rows]
                            cropped_df = cropped_df.reset_index(drop=True)
                            processed_tables.append(table.tableid)
                            append_table_list.append(table)
                            append_row_num_list.append(unique_rows)
                            append_tableid_list.append(table.tableid)
                            self.cropped_table_dict[str(table.tableid)] = cropped_df
                self.notes_span_df.at[idx, 'tableslist'] = append_table_list
                self.notes_span_df.at[idx,'tableid'] = append_tableid_list
                self.notes_span_df.at[idx, 'row_numbers'] = append_row_num_list


    def get_row_columns(self,tableid,start_bbox,end_bbox,scaling_factor):
        row_col_query = db.query(db_models.RowColLogs).filter(db_models.RowColLogs.tableid == tableid and db_models.RowColLogs.type=="row").order_by(db_models.RowColLogs.time.desc())
        rows_cols = row_col_query.distinct().all()
        included_row_col_num_list : list =[]
        for row_col in rows_cols:
            if int(row_col.top_img*scaling_factor) >= start_bbox[1] and int(row_col.down_img*scaling_factor) <= end_bbox[3] and row_col.type == 'row':
                included_row_col_num_list.append(row_col.row_col_num)
            if int(row_col.down_img*scaling_factor) >= start_bbox[1] and int(row_col.down_img*scaling_factor) <= end_bbox[3] and row_col.type == 'row':
                if int(row_col.top_img*scaling_factor) >= (int(start_bbox[1])-20):
                    included_row_col_num_list.append(row_col.row_col_num)
        return included_row_col_num_list


    def get_page_tables(self,fileid, page_number):
        page_query = db.query(db_models.PageLogs).filter(db_models.PageLogs.fileid == fileid).order_by(db_models.PageLogs.time.desc())
        pages = page_query.distinct().all()
        page_height = -1
        scaling_ratio = -1
        for page in pages:
            if page.page_number == int(page_number):
                table_query = db.query(db_models.TableLogs).filter(db_models.TableLogs.pageid == page.pageid)
                tables = table_query.distinct().all()
                page_height = page.height
    #             print(page.page_number,page.height,page.height_TE,page.width_TE)
                #scaling_ratio = (page.height/page.height_TE) ## facing issue of null height te even though there is data present in db
                scaling_ratio = (page.height/page.height_TE)
                if tables:
                    return True,tables,page_height,scaling_ratio
                else:
                    return False,None, page_height,scaling_ratio
    

    def find_tables(self,fileid,page_number,start_bbox,end_bbox):
        flag,tables,page_height,scaling_ratio = self.get_page_tables(fileid,page_number)
        table_list:list = []
        row_numbers:list = []
        if flag:
            for table in tables:
                if int(table.top*scaling_ratio) >= start_bbox[1] and int(table.down*scaling_ratio) <= end_bbox[3]:
                    table_list.append(table)
                    included_row_col_num_list = self.get_row_columns(table.tableid,start_bbox,end_bbox,scaling_ratio)
                    row_numbers.append(included_row_col_num_list)
                if int(table.down*scaling_ratio) >= start_bbox[1] and int(table.down*scaling_ratio) <= end_bbox[3]:
                    table_list.append(table)
                    included_row_col_num_list = self.get_row_columns(table.tableid,start_bbox,end_bbox,scaling_ratio)
                    row_numbers.append(included_row_col_num_list)
                if int(table.top*scaling_ratio) >= start_bbox[1] and int(table.top*scaling_ratio) <= end_bbox[3]:
                    table_list.append(table)
                    included_row_col_num_list = self.get_row_columns(table.tableid,start_bbox,end_bbox,scaling_ratio)
                    row_numbers.append(included_row_col_num_list)
                if int(table.top*scaling_ratio) <= start_bbox[1] and int(table.down*scaling_ratio) >= end_bbox[3]:
                    table_list.append(table)
                    included_row_col_num_list = self.get_row_columns(table.tableid,start_bbox,end_bbox,scaling_ratio)
                    row_numbers.append(included_row_col_num_list)
        return table_list,row_numbers
    
    def get_notes_tables(self,fileid,start_page,start_bbox,end_page,end_bbox):
        table_list : list = []
        row_numbers :list = []
        page_query = db.query(db_models.PageLogs).filter(db_models.PageLogs.fileid == fileid).order_by(db_models.PageLogs.time.desc())
        pages = page_query.all()
        page_height = -1
        for page in pages:
            if page.page_number == int(start_page):
                page_height = page.height
                break
        if int(start_page) == int(end_page):
            table_lst,row_lst = self.find_tables(fileid,start_page,start_bbox,end_bbox)
            table_list.extend(table_lst)
            row_numbers.extend(row_lst)
        if int(end_page) > int(start_page) and int(end_page) == int(start_page)+1:
            table_lst,row_lst = self.find_tables(fileid,start_page,start_bbox,[0,0,0,page_height])
            table_list.extend(table_lst)
            row_numbers.extend(row_lst)
            second_table_lst,row_lst = self.find_tables(fileid,end_page,[0,0,0,0],end_bbox)
            table_list.extend(second_table_lst)
            row_numbers.extend(row_lst)
            
        return table_list,row_numbers
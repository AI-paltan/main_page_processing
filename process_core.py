import pandas as pd
import numpy as np
import re
import openpyxl
from dateutil import parser
from typing import List,Dict
from datetime import date
from functools import reduce
from typing import Optional, List,Dict, Any
from copy import deepcopy
from database import get_db, get_db1
import db_models
from RefactorDF import RefactorCBS
from utils import *
from CBS_Sections import CBSsections
from CCF_Sections import CCFsections
from getNotesData import getNotesDataTables
from noteStandardise import NoteStandardised

from collections import defaultdict

class NestedDefaultDict(defaultdict):
    def __init__(self, *args, **kwargs):
        super(NestedDefaultDict, self).__init__(NestedDefaultDict, *args, **kwargs)

    def __repr__(self):
        return repr(dict(self))

db = get_db1()

class mainPageProcess:
    def __init__(self) -> None:
        self.fileid:str
        self.filename : str
        self.filtered_cbs_pages : List
        self.filtered_cpl_pages : List
        self.filtered_ccf_pages : List
        self.all_filtered_pages : List
        self.max_main_page : int
        self.min_page : int
        self.cbs_df_dict :  Dict = {}
        self.ccf_df_dict:Dict ={}
        self.cpl_df_dict:Dict = {}
        self.meta_dict : Dict = {}
        self.final_notes_dict :dict = {}
        self.notes_ref_dict: Dict = {}
        self.notes_region_meta_data = pd.DataFrame()
        self.cropped_table_dict : Dict = {}
        self.standardised_cropped_dict : Dict = {}
        self.standard_note_meta_dict = {}
        self.transformed_standardised_cropped_dict : Dict = {}

    def process_main_pages(self,fileid:str):
        self.fileid=fileid
        self.get_standardize_main_pages()
        self.merge_df() # if any statement spans over 2 pages
        self.notes_number_processing_cls()
        self.set_sections_subsections()
        # self.find_note_page_area()
        self.get_note_data_tables()
        self.standardize_notes_data()
        # self.transform_standardised_notes_data()
        self.save_logs_in_db()
        return self.cbs_df_dict,self.cpl_df_dict,self.ccf_df_dict,self.meta_dict,self.final_notes_dict,self.notes_ref_dict, self.notes_region_meta_data, self.cropped_table_dict

    def get_standardize_main_pages(self):
        file_query = db.query(db_models.FileLogs).filter(db_models.FileLogs.fileid == self.fileid).order_by(db_models.FileLogs.time.desc()).first()
        page_query = db.query(db_models.PageLogs).filter(db_models.PageLogs.fileid == self.fileid).order_by(db_models.PageLogs.time.desc())
        pages = page_query.all()
        self.filtered_cbs_pages = file_query.filtered_cbs_pages
        self.filtered_cpl_pages = file_query.filtered_cpl_pages
        self.filtered_ccf_pages = file_query.filtered_ccf_pages
        self.all_filtered_pages = deepcopy(self.filtered_cbs_pages)
        self.all_filtered_pages.extend(self.filtered_cpl_pages)
        self.all_filtered_pages.extend(self.filtered_ccf_pages)
        self.min_page = min(self.all_filtered_pages)
        self.max_main_page = max(self.all_filtered_pages)
        for page in pages:
            if page.page_number  in self.filtered_cbs_pages:
                tabale_query = db.query(db_models.TableLogs).filter(db_models.TableLogs.pageid == page.pageid).order_by(db_models.TableLogs.time.desc()).first()
                html_string = tabale_query.html_string
                tmp_df = pd.read_html(html_string)[0]
                RCB = RefactorCBS(df=tmp_df)
                process_cbs,temp_df = RCB.start_refactoring()
                self.cbs_df_dict[page.page_number] = process_cbs
                self.meta_dict[page.page_number] = temp_df
            if page.page_number  in self.filtered_cpl_pages:
                tabale_query = db.query(db_models.TableLogs).filter(db_models.TableLogs.pageid == page.pageid).order_by(db_models.TableLogs.time.desc()).first()
                html_string = tabale_query.html_string
                tmp_df = pd.read_html(html_string)[0]
                RCB = RefactorCBS(df=tmp_df)
                process_cpl,temp_df = RCB.start_refactoring()
                self.cpl_df_dict[page.page_number] = process_cpl
                self.meta_dict[page.page_number] = temp_df
            if page.page_number  in self.filtered_ccf_pages:
                tabale_query = db.query(db_models.TableLogs).filter(db_models.TableLogs.pageid == page.pageid).order_by(db_models.TableLogs.time.desc()).first()
                html_string = tabale_query.html_string
                tmp_df = pd.read_html(html_string)[0]
                RCB = RefactorCBS(df=tmp_df)
                process_ccf,temp_df = RCB.start_refactoring()
                self.ccf_df_dict[page.page_number] = process_ccf
                self.meta_dict[page.page_number] = temp_df
    
    def merge_df(self):
        if len(self.cbs_df_dict) > 1 and len(set(self.cbs_df_dict.keys())) > 1 :
            keys = list(self.cbs_df_dict.keys())
            appended_df = []
            for k in keys:
                appended_df.append(self.cbs_df_dict.get(k))
            appended_df = pd.concat(appended_df)
            min_keys = min(keys)
            self.cbs_df_dict.update({min_keys:appended_df})
            for k in keys:
                if k!= min_keys:
                    del self.cbs_df_dict[k]
        if len(self.cpl_df_dict) > 1 and len(set(self.cpl_df_dict.keys())) > 1 :
            keys = list(self.cpl_df_dict.keys())
            appended_df = []
            for k in keys:
                appended_df.append(self.cpl_df_dict.get(k))
            appended_df = pd.concat(appended_df)
            min_keys = min(keys)
            self.cpl_df_dict.update({min_keys:appended_df})
            for k in keys:
                if k!= min_keys:
                    del self.cpl_df_dict[k]
        if len(self.ccf_df_dict) > 1 and len(set(self.ccf_df_dict.keys())) > 1 :
            keys = list(self.ccf_df_dict.keys())
            appended_df = []
            for k in keys:
                appended_df.append(self.ccf_df_dict.get(k))
            appended_df = pd.concat(appended_df)
            min_keys = min(keys)
            self.ccf_df_dict.update({min_keys:appended_df})
            for k in keys:
                if k!= min_keys:
                    del self.ccf_df_dict[k]
    
    def notes_number_processing_cls(self):
        notes_dict = NestedDefaultDict()
        cbs_key = list(self.cbs_df_dict.keys())[0]
        cpl_key = list(self.cpl_df_dict.keys())[0]
        ccf_key = list(self.ccf_df_dict.keys())[0]
        cbs_meta = self.meta_dict.get(cbs_key)
        cpl_meta = self.meta_dict.get(cpl_key)
        ccf_meta = self.meta_dict.get(ccf_key)
        cbs_header = cbs_meta['headers']
        cpl_header = cpl_meta['headers']
        ccf_header = ccf_meta['headers']
        if 'Notes' in cbs_header:
            ref_list_cbs,notes_dict = notes_number_processing(self.cbs_df_dict.get(cbs_key),[cbs_meta['note_col_x'],cbs_meta['note_col_y']],cbs_meta['data_start_x'],cbs_meta['particulars_y'],notes_dict)
            self.notes_ref_dict['cbs'] = ref_list_cbs
        if 'Notes' in cpl_header:
            ref_list,notes_dict = notes_number_processing(self.cpl_df_dict.get(cpl_key),[cpl_meta['note_col_x'],cpl_meta['note_col_y']],cpl_meta['data_start_x'],cpl_meta['particulars_y'],notes_dict)
            self.notes_ref_dict['cpl'] = ref_list
        if 'Notes' in ccf_header:
            ref_list,notes_dict = notes_number_processing(self.ccf_df_dict.get(ccf_key),[ccf_meta['note_col_x'],ccf_meta['note_col_y']],ccf_meta['data_start_x'],ccf_meta['particulars_y'],notes_dict)
            self.notes_ref_dict['ccf'] = ref_list
        self.final_notes_dict = notes_dict

    def set_sections_subsections(self):
        for k,v in self.cbs_df_dict.items():
            obj_cbs_sections = CBSsections(v)
            obj_cbs_sections.set_section_details()
            # print(obj_cbs_sections.cbs_dataframe)
            self.cbs_df_dict.update({k:obj_cbs_sections.cbs_dataframe})
        for k,v in self.ccf_df_dict.items():
            obj_ccf_sections = CCFsections(v)
            obj_ccf_sections.set_section_details()
            self.ccf_df_dict.update({k:obj_ccf_sections.cbs_dataframe})

    def find_note_page_area(self):
        pass

    def get_note_data_tables(self):
        obj_notes_data = getNotesDataTables(fileid=self.fileid,notes_dict=self.final_notes_dict,max_main_page=self.max_main_page)
        obj_notes_data.trigger_job()
        self.notes_region_meta_data = obj_notes_data.notes_span_df
        self.cropped_table_dict = obj_notes_data.cropped_table_dict
        self.remove_empty_rows_from_notes_meta_data()
        self.add_raw_note_to_notes_meta_data()

    def remove_empty_rows_from_notes_meta_data(self):
        self.notes_region_meta_data = self.notes_region_meta_data[self.notes_region_meta_data['start_page'].str.len()>0].reset_index(drop=True)

    def add_raw_note_to_notes_meta_data(self):
        self.notes_region_meta_data['raw_note_number'] = [[] for _ in range(len(self.notes_region_meta_data))]
        for statement,value in self.notes_ref_dict.items():
            for dct  in value:
                note = dct.get('main_note_number')[0]
                subnote = dct.get('subnote_number')[0] 
                index_to_add_arr = self.notes_region_meta_data[(self.notes_region_meta_data['note']==str(note)) & (self.notes_region_meta_data['subnote']==str(subnote))].index.values
                if len(index_to_add_arr)>0:
                    # self.notes_region_meta_data.at[index_to_add_arr[0],'raw_note_number'] = dct.get('raw_note_no')
                    self.notes_region_meta_data.at[index_to_add_arr[0],'raw_note_number'].append(dct.get('raw_note_no'))


    def standardize_notes_data(self):
        obj_noteStandardise = NoteStandardised(self.cropped_table_dict)
        obj_noteStandardise.trigger_job()
        self.standardised_cropped_dict = obj_noteStandardise.standard_note_df
        self.standard_note_meta_dict = obj_noteStandardise.standard_note_meta_dict
        self.transformed_standardised_cropped_dict = obj_noteStandardise.transformed_standardised_cropped_dict
    # def transform_standardised_notes_data():


    def save_logs_in_db(self):
        pass
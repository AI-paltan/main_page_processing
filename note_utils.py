from fuzzywuzzy import fuzz
import re
import pandas as pd
import numpy as np




def note_end_testing(note_pattern,text_line):
    flag = bool(re.match(r"([A-Za-z]*)(\s*)({})([^0-9]+|\s+)".format(note_pattern),text_line.lower()))
    return flag


def find_note_start_index(note_pattern,account_text,ocr_line_df_dict,max_main_page):
    page_number = []
    start_bbox = []
    for k,df in ocr_line_df_dict.items():
        if k > max_main_page : ## alaways start after cash flow page
            for idx,row in df.iterrows():
                if note_end_testing(note_pattern,row['text'].lower()):
                    ratio = fuzz.partial_ratio(account_text.lower(), row['text'].lower())
                    if ratio > 85:
                        page_number.append(k)
                        start_bbox.append([row['left'],row['top'],row['right'],row['down']])
    return page_number,start_bbox

def find_note_end_index(start_page_number,start_bbox,ocr_line_df_dict,next_note_pattern):
    end_page_number = []
    end_bbox= []
    for k,df in ocr_line_df_dict.items():
        for strt_page in start_page_number:
            if k>=strt_page and k<=strt_page+1:
                for idx,row in df.iterrows():
                    flag = note_end_testing(next_note_pattern,row['text'].lower())
                    if flag:
                        end_page_number.append(k)
                        end_bbox.append([row['left'],row['top'],row['right'],row['down']])
        ### below code is to find next page end bbox if strat page bbox present and next note bbox not found 
#         if len(end_bbox)==0 and len(end_page_number)==0  and if len(start_page_number)>0:
#             end_page_number = int(start_page_number[0])+1
#             end_bbox = ocr_line_df_dict[int(start_page_number[0])+1].
    return end_page_number,end_bbox


def find_next_note_subnote(note,subnote=''):
    # next_note = chr(ord(str(note)) + 1)
    next_note = int(note)+1
    next_subnote = ''
    if len(subnote)<=2 and len(subnote)>0 :
        if subnote.isnumeric():
            next_subnote = int(subnote)+1
        if subnote.isalpha():
            next_subnote = chr(ord(str(subnote)) + 1)
    if len(subnote)>2:
        for char in subnote:
            if char.isalpha():
                next_subnote = next_subnote+chr(ord(str(char)) + 1)
            elif char.isnumeric():
                next_subnote = next_subnote + str(int(char)+1)
            else:
                next_subnote = next_subnote + char
    return str(next_note),str(next_subnote)


def x_cord_filter(bbox):
    if int(bbox[0]) < 150:
        return True
    else:
        return False
    
def get_first_note_occurance(notes_pages,notes_bbox):
    final_page = []
    final_bbox = []
    tmp_pge = notes_pages[0]
    tmpbbox = notes_bbox[0]
    for pge,bbox in zip(notes_pages,notes_bbox):
        if pge < tmp_pge:
            tmp_pge = pge
            tmpbbox = bbox
        elif pge == tmp_pge and (bbox[1]<tmpbbox[1]):
            tmpbbox = bbox
    final_page.append(tmp_pge)
    final_bbox.append(tmpbbox)
    return final_page,final_bbox


def refinement(page_list,bbox_list):
    filtered_pages = []
    filtered_bbox = []
    final_page = []
    final_bbox =[]
    for page,bbox in zip(page_list,bbox_list):
        if x_cord_filter(bbox):
            filtered_pages.append(page)
            filtered_bbox.append(bbox)
    if len(filtered_pages)>1:
        filtered_pages,filtered_bbox = get_first_note_occurance(filtered_pages,filtered_bbox)
    return filtered_pages,filtered_bbox


def get_note_pattern(note,subnote):
    if str(subnote).isnumeric():
        note_pattern = str(note)+'.'+str(subnote)
    else:
        note_pattern = str(note)+str(subnote)
    return note_pattern


def ocr_dump_to_line_df(ocr_df):
    line_list = []
    for i,(idx,row) in enumerate(ocr_df.iterrows()):
        if i == 0:
            prev_row = row
            min_left = row['left']
            min_top = row['top']
            max_right = row['left'] + row['width']
            max_down = row['top'] + row['height']
            row_text = row['text']
            # temp_row.append[]
        elif prev_row['line_num'] != row['line_num']:
            line_list.append([row['pageid'],prev_row['line_num'],min_left,min_top,max_right,max_down,row_text])
            prev_row = row
            min_left = row['left']
            min_top = row['top']
            max_right = row['left'] + row['width']
            max_down = row['top'] + row['height']
            row_text = row['text']
        else:
            min_left = min(min_left,row['left'])
            min_top = min(min_top,row['top'])
            max_right = max(max_right,(row['left'] + row['width']))
            max_down = max(max_down,(row['top'] + row['height']))
            row_text =row_text+" "+row['text']
        if i == len(ocr_df) - 1:
            line_list.append([row['pageid'],row['line_num'],min_left,min_top,max_right,max_down,row_text])
    ocr_line_df = pd.DataFrame(line_list,columns=['pageid','line_num','left','top','right','down','text'])
    return ocr_line_df






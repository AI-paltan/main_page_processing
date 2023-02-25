# Mailer functions will go inside this file
"""
@author: sagar.salunkhe
@since: Oct 31, 2019 11:30
"""
import re
# import string
from string import punctuation, digits
from fuzzywuzzy import fuzz
from nltk.corpus import words
from operator import itemgetter
from nltk.stem import PorterStemmer
# from flask import current_app as app


class TechMagicFuzzy:
    def __init__(self):
        self.portstem = PorterStemmer()
        self.eng_words_library = words.words()

    def token_sort_pro(self, primary_keyword, list_keywords):
        res_fuzz = []
        stem_target_keywords = []
        split_char = ' '

        for target_keyword in list_keywords:
            target_keyword = self.string_cleaning(target_keyword)
            stem_words = [self.portstem.stem(i) for i in target_keyword.split(split_char)]
            stem_target_keywords.append(split_char.join(stem_words))

        label_data = self.string_cleaning(primary_keyword)
        label_data = split_char.join([self.portstem.stem(i) for i in label_data.split(split_char)])

        for i in range(len(stem_target_keywords)):
            res_fuzz.append(tuple((list_keywords[i], fuzz.token_sort_ratio(stem_target_keywords[i], label_data))))

        res_fuzz.sort(key=itemgetter(1), reverse=True)

        # app.logger.info(f'FUZZY_PRO RESULT: {label_data} | {res_fuzz}')

        return res_fuzz

    def token_set_pro(self, primary_keyword, list_keywords):
        res_fuzz = []
        stem_target_keywords = []
        split_char = ' '

        for target_keyword in list_keywords:
            target_keyword = self.string_cleaning(target_keyword)
            stem_words = [self.portstem.stem(i) for i in target_keyword.split(split_char)]
            stem_target_keywords.append(split_char.join(stem_words))

        label_data = self.string_cleaning(primary_keyword)
        label_data = split_char.join([self.portstem.stem(i) for i in label_data.split(split_char)])

        for i in range(len(stem_target_keywords)):
            res_fuzz.append(tuple((list_keywords[i], fuzz.token_set_ratio(stem_target_keywords[i], label_data))))

        res_fuzz.sort(key=itemgetter(1), reverse=True)

        return res_fuzz

    def partial_ratio_pro(self, primary_keyword, list_keywords):
        res_fuzz = []
        stem_target_keywords = []
        split_char = ' '

        for target_keyword in list_keywords:
            target_keyword = self.string_cleaning(target_keyword)
            stem_words = [self.portstem.stem(i) for i in target_keyword.split(split_char)]
            stem_target_keywords.append(split_char.join(stem_words))

        label_data = self.string_cleaning(primary_keyword)
        label_data = split_char.join([self.portstem.stem(i) for i in label_data.split(split_char)])

        for i in range(len(stem_target_keywords)):
            res_fuzz.append(tuple((list_keywords[i], fuzz.partial_ratio(stem_target_keywords[i], label_data))))

        res_fuzz.sort(key=itemgetter(1), reverse=True)

        return res_fuzz

    def check_particular_garbage_chars(self, str_line):
        str_line_cp = str_line[:]
        str_line_cp = self.string_cleaning(str_line_cp)

        if str_line_cp:
            return str_line
        else:
            return str(str_line_cp).center(3)

    def string_cleaning(self, str_line):

        pattern = r"[{}]".format(punctuation)                    # create the pattern
        remove_digits = str.maketrans('', '', digits)            # remove digits from string

        # str_line = self.strip_string_bullets(str_line)

        str_line = str_line.replace("/", " ")               # replace "/" with space
        str_line = str_line.replace("\n", " ")              # replace "\n" with space
        str_line = re.sub(r'(non)(\s+)(-)(\s+)', r'\1\3', str(str_line), flags=re.IGNORECASE)   # Regex replace "Non - <TEXT>" to "Non-<TEXT>"
        res_str = re.sub(pattern, "", str_line).strip()     # replace special chars with space
        res_str = re.sub(r'\b[a-zA-Z]{1,2}\b', "", res_str)   # replace single letter with empty space
        res_str = res_str.translate(remove_digits)          # replace digits from string
        res_str = re.sub(r"\s{2,}", ' ', res_str)           # replace multiple spaces with single space

        return res_str.strip()

    def strip_string_bullets(self, str_txt):

        res_txt = str(str_txt).strip().lower()

        if not res_txt:
            return res_txt

        word_detect = res_txt.split()[0]

        if word_detect:

            # IF WORD HAS NO SPECIAL CHAR RETURN
            if not any(elem in word_detect for elem in punctuation):
                return res_txt

            word_detect = re.sub(r'[()]', '', word_detect)  # replace single letter with empty space

            word_detect = re.sub(r'\b[a-zA-Z]{1,2}\b', "", word_detect)  # replace single letter with empty space

            if len(word_detect) > 3:
                return res_txt
            elif word_detect not in self.eng_words_library:
                res_txt = res_txt.split(res_txt.split()[0], 1)[-1]
                res_txt = res_txt.strip()
        return res_txt

    def fix_ocr_decimal_issue(self, num):
        num = str(num)

        try:
            tmp = num.replace(',', '')
            tmp = float(tmp)
        except Exception as ex:
            return num

        split_num = num.split('.')
        
        if len(split_num)>1:
            if not len(split_num[1])==2:
                num = num.replace('.', '')

        list_num = list(num)

        if len(list_num) >= 4:
            if list_num[-3] == ',':
                list_num[-3] = '.'
            elif list_num[-4] == '.':
                list_num[-4] = ''

        num = ''.join(list_num)

        return num

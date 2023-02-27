from pydantic import BaseSettings
from dotenv import load_dotenv
from os import path
import os

class Settings(BaseSettings):
    database_hostname: str = "localhost"
    database_port: str = "5432"
    database_password: str = "admin"
    database_name: str="crm_syd_dev"
    database_username: str="jayesh"


class Main_Page_CoreSettings(BaseSettings):
    file_storage :str=os.path.join(path.dirname(__file__),'../..','FILE_DB/FILES')
    page_storage: str= os.path.join(path.dirname(__file__),'../..','FILE_DB/PAGES')
    cbs_statement_sections : str =os.path.join(path.dirname(__file__),'keywords_library/cbs_statement_sections.tsv')
    cbs_refactor_sections : str =os.path.join(path.dirname(__file__),'keywords_library/cbs_refactor_sections.tsv')
    cbs_refactor_subsections : str =os.path.join(path.dirname(__file__),'keywords_library/cbs_refactor_subsections.tsv')
    ccf_refactor_sections:str=os.path.join(path.dirname(__file__),'keywords_library/ccf_refactor_sections.tsv')
    ccf_refactor_subsections:str=os.path.join(path.dirname(__file__),'keywords_library/ccf_refactor_subsections.tsv')

class Keyword_mapping_Settings(BaseSettings):
    ccf_section_subtotal_keywords :str = os.path.join(path.dirname(__file__),'keywords_library/ccf_section_subtotal_keywords.tsv')


settings = Settings()
main_page_core_settings = Main_Page_CoreSettings()
keyword_mapping_settings = Keyword_mapping_Settings()

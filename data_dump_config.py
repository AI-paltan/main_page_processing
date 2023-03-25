from pydantic import BaseSettings
from dotenv import load_dotenv
from os import path
import os


class DataDumpCoreSettings(BaseSettings):
    cdm_template :str= os.path.join(path.dirname(__file__),'CDM_new_template.xlsx')
    bs_breakdown_particular_colidx = 2
    pl_breakdown_particular_colidx = 2
    
    
datadump_core_settings = DataDumpCoreSettings()
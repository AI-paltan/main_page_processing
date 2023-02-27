from database import get_db, get_db1
import db_models
from CashFlowDataBucketing import CashFlowDataBucketing
import pandas as pd

db = get_db1()

class CCFMapping:
    def __init__(self,ccf_df):
        self.datasheet = ccf_df
        self.df_nlp_bucket_master = pd.DataFrame()
        self.df_response = pd.DataFrame()

    def trigger_job(self):
        self.get_nlp_bucket_df_from_db()
        self.calculate_CCF()

    def calculate_CCF(self):
        obj_ccf_bucketing = CashFlowDataBucketing(df_datasheet=self.datasheet,df_nlp_bucket_master=self.df_nlp_bucket_master)
        obj_ccf_bucketing.fetch_report()
        self.df_response = obj_ccf_bucketing.df_response
        self.df_nlp_bucket_master = obj_ccf_bucketing.df_nlp_bucket_master

    def get_nlp_bucket_df_from_db(self):
        crm_nlp_query = db.query(db_models.CRM_nlp_bucketing).filter(db_models.CRM_nlp_bucketing.statement_type == "ccf")
        self.df_nlp_bucket_master = pd.read_sql(crm_nlp_query.statement, crm_nlp_query.session.bind)

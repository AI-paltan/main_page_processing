from database import get_db, get_db1
import db_models
from ProfitLossDataBucketing import ProfitLossDataBucketing
import pandas as pd

db = get_db1()

class CPLMapping:
    def __init__(self,cpl_df,notes_ref_dict,notes_region_meta_data,standardised_cropped_dict,standard_note_meta_dict,transformed_standardised_cropped_dict,month):
        self.datasheet = cpl_df
        self.df_nlp_bucket_master = pd.DataFrame()
        self.df_response = pd.DataFrame()
        self.notes_ref_dict =notes_ref_dict
        self.notes_region_meta_data = notes_region_meta_data
        self.standardised_cropped_dict = standardised_cropped_dict
        self.standard_note_meta_dict = standard_note_meta_dict
        self.transformed_standardised_cropped_dict = transformed_standardised_cropped_dict
        self.pl_bucket_dict = {}
        self.month = month

    def trigger_job(self):
        self.get_nlp_bucket_df_from_db()
        self.preprocess_cpl_main_page()
        self.calculate_CPL()

    def calculate_CPL(self):
        obj_cbs_bucketing = ProfitLossDataBucketing(df_datasheet=self.datasheet,df_nlp_bucket_master=self.df_nlp_bucket_master,notes_ref_dict=self.notes_ref_dict,notes_region_meta_data=self.notes_region_meta_data,standardised_cropped_dict=self.standardised_cropped_dict,standard_note_meta_dict=self.standard_note_meta_dict,transformed_standardised_cropped_dict= self.transformed_standardised_cropped_dict,month=self.month)
        obj_cbs_bucketing.fetch_report()
        self.pl_bucket_dict = obj_cbs_bucketing.pl_bucketing_dict
        # self.df_response = obj_ccf_bucketing.df_response
        # self.df_nlp_bucket_master = obj_ccf_bucketing.df_nlp_bucket_master

    def get_nlp_bucket_df_from_db(self):
        crm_nlp_query = db.query(db_models.CRM_nlp_bucketing).filter(db_models.CRM_nlp_bucketing.statement_type == "cpl")
        self.df_nlp_bucket_master = pd.read_sql(crm_nlp_query.statement, crm_nlp_query.session.bind)

    def preprocess_cpl_main_page(self):
        pass
    ### need to crop pl df till 'profit for the year' or 'loss for the year'
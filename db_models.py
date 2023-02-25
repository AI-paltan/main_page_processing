from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID,ARRAY

from database import Base


class FileLogs(Base):
    __tablename__ = "FileLogs"
    id = Column(Integer, primary_key=True, nullable=False)
    fileid = Column(UUID(as_uuid=True),nullable=False)
    filename = Column(String)
    filepath = Column(String)
    page_count = Column(Integer)
    predicted_cbs_pages = Column(ARRAY(Integer))
    predicted_cpl_pages = Column(ARRAY(Integer))
    predicted_ccf_pages = Column(ARRAY(Integer))
    filtered_cbs_pages = Column(ARRAY(Integer))
    filtered_cpl_pages = Column(ARRAY(Integer))
    filtered_ccf_pages = Column(ARRAY(Integer))
    region = Column(String)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))

class PageLogs(Base):
    __tablename__ = "PageLogs"
    id = Column(Integer, primary_key=True, nullable=False)
    fileid = Column(UUID(as_uuid=True), nullable=False)
    pageid = Column(UUID(as_uuid=True), nullable=False)
    page_number = Column(Integer)
    page_path = Column(String)
    page_filename = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    width_TE = Column(Integer)
    height_TE = Column(Integer)
    predicted_type_id=Column(Integer)
    predicted_type_name=Column(String)
    Number_of_Tables = Column(Integer)
    fetched_for_training = Column(Integer,server_default='0')
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))

class OCRDump(Base):
    __tablename__ = "OCRDump"
    id = Column(Integer, primary_key=True, nullable=False)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))
    pageid = Column(UUID(as_uuid=True), nullable=False)
    block_num = Column(Integer)
    line_num = Column(Integer)
    word_num =Column(Integer)
    left = Column(Integer)
    top = Column(Integer)
    right = Column(Integer)
    down = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    conf = Column(Float)
    text = Column(String)
    
class OCRText(Base):
    __tablename__ = "OCRText"
    id = Column(Integer, primary_key=True, nullable=False)
    pageid = Column(UUID(as_uuid=True), nullable=False)
    raw_text = Column(String)
    structured_text = Column(String)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))

class TableLogs(Base):
    __tablename__ = "TableLogs"
    id = Column(Integer, primary_key=True, nullable=False)
    pageid = Column(UUID(as_uuid=True), nullable=False)
    tableid = Column(UUID(as_uuid=True),nullable=False)
    left = Column(Integer)
    top = Column(Integer)
    right = Column(Integer)
    down = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    conf = Column(Float)
    table_img_save_path = Column(String)
    html_string = Column(String)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))

class CellLogs(Base):
    __tablename__ = "CellLogs"
    id = Column(Integer, primary_key=True, nullable=False)
    cellid = Column(UUID(as_uuid=True), nullable=False)
    tableid = Column(UUID(as_uuid=True), nullable=False)
    left_img = Column(Integer)
    top_img = Column(Integer)
    right_img = Column(Integer)
    down_img = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    left_table = Column(Integer)
    top_table = Column(Integer)
    right_table = Column(Integer)
    down_table = Column(Integer)
    conf = Column(Float)
    row_number = Column(Integer)
    col_number = Column(Integer)
    row_span = Column(Integer)
    col_span = Column(Integer)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))



class RowColLogs(Base):
    __tablename__ = "RowColLogs"
    id = Column(Integer, primary_key=True, nullable=False)
    row_col_id = Column(UUID(as_uuid=True), nullable=False)
    tableid = Column(UUID(as_uuid=True),nullable=False)
    type = Column(String)
    left_img = Column(Integer)
    top_img = Column(Integer)
    right_img = Column(Integer)
    down_img = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    left_table = Column(Integer)
    top_table = Column(Integer)
    right_table = Column(Integer)
    down_table = Column(Integer)
    conf = Column(Float)
    row_col_num = Column(Integer)
    time = Column(TIMESTAMP(timezone=True),
                        nullable=False, server_default=text('now()'))
    


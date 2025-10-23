from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://postgres:07052001@localhost:5432/excel_data")
Session = sessionmaker(bind=engine)
session = Session()

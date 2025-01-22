import pandas as pd 
import pyodbc 
import os
from sqlalchemy import create_engine
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, ForeignKey, Table, MetaData
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class ProductDim(Base):
    __tablename__ = 'product_dim'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(255))
    category = Column(String(100))

class CustomerDim(Base):
    __tablename__ = 'customer_dim'
    customer_id = Column(Integer, primary_key=True)
    customer_name = Column(String(255))
    age = Column(Integer)
    gender = Column(String(10))

class LocationDim(Base):
    __tablename__ = 'location_dim'
    location_id = Column(Integer, primary_key=True)
    city = Column(String(100))
    country = Column(String(100))


class TimeDim(Base):
    __tablename__ = 'time_dim'
    date_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)


class SalesFact(Base):
    __tablename__ = 'sales_fact'
    sale_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customer_dim.customer_id'))
    product_id = Column(Integer, ForeignKey('product_dim.product_id'))
    location_id = Column(Integer, ForeignKey('location_dim.location_id'))
    date_id = Column(Integer, ForeignKey('time_dim.date_id'))
    quantity = Column(Integer)
    revenue = Column(Float)
    cost = Column(Float)

def load(sales_fact, product_dim, customer_dim, location_dim, time_dim):
    try:
        #SQLAlchemy connection string
        engine = create_engine(
            'mssql+pyodbc://DESKTOP-2P256TK/test?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        )

        #Verify connection
        with engine.connect() as conn:
            print("Connected to the database successfully.")

        #Create tables
        Base.metadata.create_all(engine)

        #Load data into tables
        tables = {
            SalesFact: sales_fact,
            ProductDim: product_dim,
            CustomerDim: customer_dim,
            LocationDim: location_dim,
            TimeDim: time_dim
        }

        for table_class, df in tables.items():
            df.to_sql(
                table_class.__tablename__,
                con=engine,
                schema='dbo',
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            print(f"Data successfully loaded into {table_class.__tablename__}.")

    except Exception as e:
        print(f"An error occurred: {e}") 

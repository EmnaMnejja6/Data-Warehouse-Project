import pandas as pd 
import pyodbc 
import os
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, ForeignKey, Table, MetaData
)
from sqlalchemy.orm import declarative_base
import requests
from geopy.geocoders import Nominatim
import aiohttp
import asyncio

def extract():
    try:
        #Fetching sales_2022 data from the database
        cnx = pyodbc.connect(
            'DRIVER={SQL Server};SERVER=server_name;DATABASEdb_name;Trusted_Connection=yes;'
        )
        sales_2022_data = pd.read_sql(
            'SELECT sale_id, product_id, customer_id, quantity, sale_date, price FROM sales2222', 
            cnx
        )
        print("Successfully fetched sales_2022 data.")
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        sales_2022_data = None
    except Exception as ex:
        print(f"General error: {ex}")
        sales_2022_data = None
    finally:
        if 'cnx' in locals() and cnx is not None:
            cnx.close()

    try:
        #Fetching sales_2023 data from the csv file
        sales_2023_data = pd.read_csv('./data/raw/sales_2023.csv')
        print("Successfully loaded sales_2023 data.")
    except Exception as ex:
        print(f"Error loading sales_2023 data: {ex}")
        sales_2023_data = None

    try:
        #Fetching customer data from the csv file
        customer_data = pd.read_csv('./data/raw/customer_data.csv')
        print("Successfully loaded customer data.")
    except Exception as ex:
        print(f"Error loading customer data: {ex}")
        customer_data = None

    try:
        #Fetching product data from the json file
        product_data = pd.read_json('./data/raw/products_data.json')
        print("Successfully loaded product data.")
    except Exception as ex:
        print(f"Error loading product data: {ex}")
        product_data = None

    return sales_2022_data, sales_2023_data, customer_data, product_data

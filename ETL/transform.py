import pandas as pd 
import pyodbc 
import os
import requests
from geopy.geocoders import Nominatim
import aiohttp
import asyncio

async def get_country_from_city_async(city):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api.opencagedata.com/geocode/v1/json?q={city}&key=api_key') as response:
            data = await response.json()
            if data['status']['code'] == 200:
                country = data['results'][0]['components'].get('country')
                return city, country
            return city, None

async def get_all_countries(cities):
    tasks = [get_country_from_city_async(city) for city in cities]
    return await asyncio.gather(*tasks)

async def transform(sales_2022, sales_2023, product_data, customer_data):
    
    #Convert sale_date to consistent format in sales_2022
    sales_2022['sale_date'] = pd.to_datetime(
        sales_2022['sale_date'], 
        errors='coerce').dt.date

    #Merge sales_2022 and sales_2023
    sales_data = pd.concat([sales_2022, sales_2023], axis=0)
    sales_data.drop(columns=['sale_id'], inplace=True)

    #Merge with product_data
    sales_data = sales_data.merge(
        product_data[['product_id', 'price', 'cost']],
        on='product_id',
        suffixes=('_sale', '_product')
    )

    #Calculate revenue and profit
    sales_data['revenue'] = sales_data['price_sale'] * sales_data['quantity']
    sales_data['profit'] = sales_data['revenue'] - (sales_data['cost'] * sales_data['quantity'])

    #Create time dimension from sales_data
    unique_dates = sales_data['sale_date'].dropna().unique()
    date_range = pd.to_datetime(unique_dates)
    time_dim = pd.DataFrame({'date': date_range})
    time_dim['date_id'] = range(1, len(time_dim) + 1)
    time_dim['year'] = time_dim['date'].dt.year
    time_dim['month'] = time_dim['date'].dt.month
    time_dim['day'] = time_dim['date'].dt.day

    #Merging time dimension with sales_data
    sales_data['sale_date'] = pd.to_datetime(sales_data['sale_date'], errors='coerce')
    sales_data = pd.merge(
        sales_data,
        time_dim[['date', 'date_id']],
        left_on='sale_date',
        right_on='date',
        how='left'
    )
    sales_data.drop(columns=['sale_date', 'date'], inplace=True)
    time_dim.drop(columns=['date'],inplace=True)


    # Enriching the customer_data with the country
    cities = customer_data['city'].unique()
    countries = await get_all_countries(cities)
    countries_dict = dict(countries)
    customer_data['country'] = customer_data['city'].apply(lambda city: countries_dict.get(city, None))

    #Create location dimension with unique city and country combinations
    location_dim = customer_data[['city', 'country']].drop_duplicates()

    # Assign a unique location_id for each unique city-country combination
    location_dim['location_id'] = range(1, len(location_dim) + 1)

    # Merge location_dim with customer_data to get customer_id associated with location_id
    customer_location_mapping = customer_data[['customer_id', 'city', 'country']]
    customer_location_mapping = pd.merge(customer_location_mapping, location_dim, on=['city', 'country'], how='left')

    # Merge sales_data with the customer_location_mapping to add location_id
    sales_data = pd.merge(
        sales_data, 
        customer_location_mapping[['customer_id', 'location_id']], 
        on='customer_id', 
        how='left'
    )

    # Finalize location_dim (city and country are already unique)
    location_dim = location_dim[['location_id', 'city', 'country']]

    # Create product dimension
    product_dim = product_data[['product_id', 'product_name', 'category']].drop_duplicates()

    # Create customer dimension
    customer_dim = customer_data[['customer_id', 'customer_name', 'age', 'gender']].drop_duplicates()
    
    # Keep only specific columns in the sales_data DataFrame
    sales_data = sales_data[['customer_id', 'product_id', 'location_id', 'date_id', 'quantity', 'revenue', 'cost']]
    sales_data['sale_id'] = range(1, len(sales_data) + 1)

    sales_data = sales_data[['sale_id', 'customer_id', 'product_id', 'location_id', 'date_id', 'quantity', 'revenue', 'cost']]
    customer_dim['gender'] = customer_dim['gender'].map({'Male': 0, 'Female': 1})
    return sales_data, product_dim, customer_dim, location_dim, time_dim

# Data Warehouse Project: Sales Analysis

Welcome to the repository for the **Sales Analysis Data Warehouse Project**! This project is a collaborative effort between **Emna Mnejja** and **Ons Mhiri**, focused on analyzing sales data to uncover valuable insights and answer key business questions.

## Project Overview

This project involves building a structured data warehouse using a **Star Schema** model and performing analysis on sales data. The aim is to address the following key questions:

1. **Revenue Trends:** How have revenues varied over the years?
2. **Top-Selling Products:** What are the most sold products by category in the last two years?
3. **Cost Analysis:** What is the total cost of sold products per category, and how does it impact profitability?
4. **Customer Distribution:** How are our customers geographically distributed?

## Data Sources

The data used in this project is derived from multiple sources, including:

- `sales_2022.sql`: Sales data for the year 2022.
- `sales_2023.csv`: Sales data for the year 2023.
- `customers.csv`: Customer details.
- `products.json`: Product information.

### Data Enhancements

- We added a `country` column to the **Customers Dimension** using the OpenCage API for geocoding.

## Methodology and Tools Used

### Data Warehouse Design

We implemented a **Star Schema** to structure the data warehouse. (The schema design is included as an image in this repository.)

### ETL (Extract, Transform, Load)

- **Python:** Used for processing and transforming data with libraries such as:
  - `pandas`
  - `numpy`
  - `pyodbc`
- **Talend:** Further ETL processes with data stored in PostgreSQL.

### OLAP Cube Construction

- Built an OLAP cube using **SQL Server Analysis Services (SSAS)** for multidimensional analysis.

### Data Visualization

- Conducted analysis using **MDX queries**.
- Built interactive dashboards using **Power BI**.

## Key Takeaways

This project provided hands-on experience in:

- Building data warehouses.
- Designing and implementing ETL pipelines.
- Conducting multidimensional analysis with OLAP cubes.
- Visualizing data insights using Power BI.

## How to Explore the Repository

1. **Data Files:**
   - `sales_2022.sql`
   - `sales_2023.csv`
   - `customers.csv`
   - `products.json`
     
2. **Star Schema:**
   - A diagram of the Star Schema design is included in this repository.

3. **ETL Notebook:**
   - If you'd like to explore the ETL process in detail, let me know, and I can share the `ETL.ipynb` file.

4. **Dashboards:**
   - Power BI dashboards showcasing sales insights are included for visualization.

## Disclaimer

This project is based on the analysis of a fictitious company. The data used is for academic purposes only.



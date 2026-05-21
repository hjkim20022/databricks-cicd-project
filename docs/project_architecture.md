# Project Architecture

## Overview
This project demonstrates a beginner-friendly Medallion Architecture ETL pipeline.

The pipeline processes retail sales CSV data using Python and Pandas.

---

## Architecture Flow

Bronze Layer → Silver Layer → Gold Layer

---

## Bronze Layer
Stores raw retail sales CSV data.

File:
- sales_data.csv

Purpose:
- preserve original source data

---

## Silver Layer
Performs data cleaning and transformations.

Transformations:
- remove extra spaces
- convert data types
- calculate Revenue column

Output:
- silver_sales_data.csv

---

## Gold Layer
Creates business-ready aggregated data.

Business Logic:
- total revenue by category

Output:
- gold_sales_by_category.csv

---

## Technologies Used
- Python
- Pandas
- Git
- GitHub
- PyCharm

---

## Future Improvements
- Databricks integration
- Delta Lake
- PySpark migration
- GitHub Actions CI/CD
- Automated testing
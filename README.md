# Databricks CI/CD Data Engineering Project

## Project Overview
This project demonstrates a simple Medallion Architecture ETL pipeline using Python and Pandas.

The pipeline processes retail sales CSV data through:

- Bronze Layer (raw data)
- Silver Layer (cleaned and transformed data)
- Gold Layer (business summary data)

## Technologies Used - 
- Python
- Pandas
- Git
- GitHub
- Medallion Architecture
- ETL Pipeline

## Project Structure - 

data/
├── bronze/
├── silver/
├── gold/

pipelines/
├── etl_pipeline.py

## Pipeline Process

### Bronze Layer
Reads raw CSV sales data.

### Silver Layer
Cleans data and calculates Revenue.

### Gold Layer
Creates business summary by Category.

## Sample Business Output

| Category | Revenue |
|----------|---------|
| Electronics | 4000 |
| Furniture | 850 |

## Future Improvements
- Databricks integration
- CI/CD with GitHub Actions
- Automated testing
- Delta Lake integration
- PySpark migration

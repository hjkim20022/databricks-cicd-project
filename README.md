# Databricks CI/CD Retail Sales Project

## Project Overview
This project demonstrates an end-to-end Data Engineering pipeline using Databricks, PySpark, Delta Lake, GitHub, and CI/CD concepts.

The pipeline follows the Medallion Architecture:

Bronze → Silver → Gold

## Architecture
CSV file is uploaded into Databricks Volume.

Bronze Layer:
- raw sales data

Silver Layer:
- cleaned data
- converted data types
- calculated Revenue

Gold Layer:
- revenue summary by category

## Technologies Used
- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- GitHub
- GitHub Actions
- pytest
- Databricks Workflows

## CI/CD Workflow
1. Create feature branch
2. Make code changes
3. Commit and push
4. Create pull request
5. GitHub Actions runs tests
6. Merge into main

## Databricks Workflow
The Databricks job runs the notebook automatically.

Features:
- scheduled daily run
- Delta table validation
- failure email notification

## Data Quality Tests
This project includes automated tests for:
- revenue calculation
- null checks
- duplicate checks
- data type checks
- negative value checks

## Resume Summary
Built an end-to-end Databricks Medallion Architecture pipeline with PySpark, Delta Lake, Unity Catalog, GitHub Actions CI/CD, automated testing, workflow scheduling, and failure notifications.

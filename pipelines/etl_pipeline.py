import pandas as pd
from pathlib import Path

# Project root folder
BASE_DIR = Path(__file__).resolve().parent.parent

# Bronze: read raw data
bronze_df = pd.read_csv(BASE_DIR / "data" / "bronze" / "sales_data.csv")
bronze_df.columns = bronze_df.columns.str.strip()

# Silver: clean and transform data
silver_df = bronze_df.copy()
silver_df["Quantity"] = silver_df["Quantity"].astype(int)
silver_df["Price"] = silver_df["Price"].astype(float)
silver_df["Revenue"] = silver_df["Quantity"] * silver_df["Price"]

silver_df.to_csv(BASE_DIR / "data" / "silver" / "silver_sales_data.csv", index=False)

# Gold: business summary data
gold_df = silver_df.groupby("Category", as_index=False)["Revenue"].sum()
gold_df.to_csv(BASE_DIR / "data" / "gold" / "gold_sales_by_category.csv", index=False)

print("Bronze data loaded")
print("Silver data created")
print("Gold data created")
print(gold_df)
print("Medallion ETL Pipeline Completed Successfully")


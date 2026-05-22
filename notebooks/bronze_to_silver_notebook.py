from pyspark.sql.functions import col, sum

# Bronze Layer
bronze_df = spark.read.option("header", True).csv(
    "/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv"
)

display(bronze_df)

# Silver Layer
silver_df = bronze_df.withColumn(
    "Quantity", col("Quantity").cast("int")
).withColumn(
    "Price", col("Price").cast("double")
).withColumn(
    "Revenue", col("Quantity") * col("Price")
)

display(silver_df)

# Gold Layer
gold_df = silver_df.groupBy("Category").agg(
    sum("Revenue").alias("Total_Revenue")
)

display(gold_df)

# Save Silver as Delta Table
silver_df.write.format("delta").mode("overwrite").saveAsTable(
    "retail_sales_catalog.default.silver_sales"
)

# Save Gold as Delta Table
gold_df.write.format("delta").mode("overwrite").saveAsTable(
    "retail_sales_catalog.default.gold_sales_summary"
)

print("Delta Tables Created Successfully")

# Delta Table Validation

silver_count = spark.table(
    "retail_sales_catalog.default.silver_sales"
).count()

gold_count = spark.table(
    "retail_sales_catalog.default.gold_sales_summary"
).count()

print("Silver rows:", silver_count)
print("Gold rows:", gold_count)

assert silver_count > 0
assert gold_count > 0

print("Delta Table Validation Passed")
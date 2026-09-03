from pyspark.sql.functions import *

orders = spark.table("silver_orders")
returns = spark.table("silver_returns")
inventory = spark.table("silver_inventory")

inventory_product = (
    inventory
    .groupBy("ProductName")
    .agg(
        sum("Stock").alias("Total_Stock"),
        avg("CostPrice").alias("Avg_Cost")
    )
)

returned_orders = (
    returns
    .select("OrderID")
    .distinct()
    .withColumn("IsReturned", lit(1))
)

enriched = (
    orders
    .join(
        returned_orders,
        on="OrderID",
        how="left"
    )
    .fillna({"IsReturned": 0})
    .join(
        inventory_product,
        on="ProductName",
        how="left"
    )
    .withColumn(
        "OrderMonth",
        date_format(col("OrderDate"), "yyyy-MM")
    )
    .withColumn(
        "COGS",
        col("Quantity") * col("Avg_Cost")
    )
)

df_kpi = (
    enriched
    .groupBy("ProductName", "OrderMonth")
    .agg(
        countDistinct("OrderID").alias("Total_Orders"),
        countDistinct("CustomerID").alias("Unique_Customers"),
        sum("IsReturned").alias("Total_Returns"),
        round(
            (sum("IsReturned") / countDistinct("OrderID")) * 100,
            2
        ).alias("Return_Rate_%"),
        round(sum("OrderAmount"), 2).alias("Total_Revenue"),
        round(avg("OrderAmount"), 2).alias("Avg_Order_Value"),
        round(sum("COGS"), 2).alias("Total_COGS"),
        round(
            sum("OrderAmount") - sum("COGS"),
            2
        ).alias("Net_Profit"),
        round(avg("Avg_Cost"), 2).alias("Avg_Cost"),
        first("Total_Stock").alias("Total_Stock")
    )
)

display(df_kpi)

df_kpi.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("gold_product_month_kpis")

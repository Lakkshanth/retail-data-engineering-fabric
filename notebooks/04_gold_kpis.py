from pyspark.sql.functions import *

# STEP 1: Load cleaned Silver tables with aliases
orders = spark.table("silver_orders").alias("o")
returns = spark.table("silver_returns").alias("r")
inventory = spark.table("silver_inventory").alias("i")


# STEP 2: Join Orders with Returns (LEFT)
order_return = orders.join(
    returns,
    col("o.OrderID") == col("r.OrderID"),
    how="left"
)


# STEP 3: Join with Inventory (LEFT)
enriched = order_return.join(
    inventory,
    col("o.ProductName") == col("i.ProductName"),
    how="left"
)


# STEP 4: Add derived column for month
enriched = enriched.withColumn(
    "OrderMonth",
    date_format(col("o.OrderDate"), "yyyy-MM")
)


# STEP 5: Select explicit columns to avoid ambiguity
df_enriched = enriched.select(
    col("o.ProductName").alias("ProductName"),
    col("o.OrderID").alias("OrderID"),
    col("o.CustomerID").alias("CustomerID"),
    col("o.OrderAmount").alias("OrderAmount"),
    col("r.ReturnID").alias("ReturnID"),
    col("i.Stock").alias("Stock"),
    col("i.CostPrice").alias("CostPrice"),
    col("OrderMonth")
)


# STEP 6: Aggregate KPIs by Product AND Month
df_kpi = (
    df_enriched
    .groupBy("ProductName", "OrderMonth")
    .agg(
        # Total unique orders
        countDistinct("OrderID").alias("Total_Orders"),

        # Unique customers
        countDistinct("CustomerID").alias("Unique_Customers"),

        # Total unique returns
        countDistinct("ReturnID").alias("Total_Returns"),

        # Return rate percentage
        round(
            (
                countDistinct("ReturnID") /
                countDistinct("OrderID")
            ) * 100,
            2
        ).alias("Return_Rate_%"),

        # Total revenue
        round(
            sum("OrderAmount"),
            2
        ).alias("Total_Revenue"),

        # Average order value
        round(
            avg("OrderAmount"),
            2
        ).alias("Avg_Order_Value"),

        # Total stock
        sum("Stock").alias("Total_Stock"),

        # Average cost
        round(
            avg("CostPrice"),
            2
        ).alias("Avg_Cost"),

        # Net profit
        round(
            sum("OrderAmount") -
            (sum("Stock") * avg("CostPrice")),
            2
        ).alias("Net_Profit")
    )
)


# STEP 7: Display results
display(df_kpi)


# STEP 8: Save to Gold Delta table
df_kpi.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_product_month_kpis")

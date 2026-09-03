from pyspark.sql.functions import *
from pyspark.sql.types import *

# STEP 1: Rename raw columns first to avoid Spark case-sensitivity issues
df_inventory = (
    df_inventory_raw

    .withColumnRenamed("productName", "ProductName")
    .withColumnRenamed("stock", "stock_raw")
    .withColumnRenamed("last_stocked", "last_stocked_raw")
    .withColumnRenamed("warehouse", "warehouse_raw")
    .withColumnRenamed("cost_price", "cost_price_raw")
    .withColumnRenamed("available", "available_raw")

    # STEP 2: Clean Stock
    .withColumn(
        "Stock",
        when(
            trim(col("stock_raw")).rlike(r"^[0-9]+$"),
            col("stock_raw").cast(IntegerType())
        )
        .when(
            lower(trim(col("stock_raw"))).contains("twenty five"),
            lit(25)
        )
        .when(
            lower(trim(col("stock_raw"))).contains("twenty"),
            lit(20)
        )
        .when(
            lower(trim(col("stock_raw"))).contains("eighteen"),
            lit(18)
        )
        .when(
            lower(trim(col("stock_raw"))).contains("fifteen"),
            lit(15)
        )
        .when(
            lower(trim(col("stock_raw"))).contains("twelve"),
            lit(12)
        )
        .otherwise(
            lit(None).cast(IntegerType())
        )
    )

    # STEP 3: Clean LastStocked
    .withColumn(
        "LastStocked",
        coalesce(
            to_date(col("last_stocked_raw"), "yyyy-MM-dd"),
            to_date(col("last_stocked_raw"), "yyyy/MM/dd"),
            to_date(col("last_stocked_raw"), "dd-MM-yyyy"),
            to_date(col("last_stocked_raw"), "dd/MM/yyyy"),
            to_date(col("last_stocked_raw"), "yyyy.MM.dd"),
            to_date(col("last_stocked_raw"), "dd.MM.yyyy")
        )
    )

    # STEP 4: Clean CostPrice
    .withColumn(
        "CostPrice",
        regexp_extract(
            col("cost_price_raw"),
            r"(\d+\.?\d*)",
            1
        ).cast(DoubleType())
    )

    # STEP 5: Clean Warehouse
    .withColumn(
        "Warehouse",
        initcap(
            trim(
                regexp_replace(
                    col("warehouse_raw"),
                    r"[^a-zA-Z0-9\s]",
                    " "
                )
            )
        )
    )

    # STEP 6: Standardize Available
    .withColumn(
        "Available",
        when(
            lower(trim(col("available_raw"))).isin(
                "yes", "y", "true"
            ),
            lit(True)
        )
        .when(
            lower(trim(col("available_raw"))).isin(
                "no", "n", "false"
            ),
            lit(False)
        )
        .otherwise(
            lit(None).cast("boolean")
        )
    )

    # STEP 7: Remove temporary raw columns
    .drop(
        "stock_raw",
        "last_stocked_raw",
        "warehouse_raw",
        "cost_price_raw",
        "available_raw"
    )
)



# STEP 8: Display cleaned inventory
display(df_inventory)

# STEP 9: Save to Silver
df_inventory.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_inventory")

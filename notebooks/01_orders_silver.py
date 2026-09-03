from pyspark.sql.functions import *
from pyspark.sql.types import *

df_orders = (
    df_orders_raw

    # 2. Clean column names
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("Cust_ID", "CustomerID")
    .withColumnRenamed("Product_Name", "ProductName")
    .withColumnRenamed("Qty", "Quantity")
    .withColumnRenamed("Order_Date", "OrderDate")
    .withColumnRenamed("Order_Amount$", "OrderAmount")
    .withColumnRenamed("Delivery_Status", "DeliveryStatus")
    .withColumnRenamed("Payment_Mode", "PaymentMode")
    .withColumnRenamed("Ship_Address", "ShipAddress")
    .withColumnRenamed("Promo_Code", "PromoCode")
    .withColumnRenamed("Feedback_Score", "FeedbackScore")

    # 3. Normalize Quantity
    .withColumn(
        "Quantity",
        when(lower(trim(col("Quantity"))) == "one", 1)
        .when(lower(trim(col("Quantity"))) == "two", 2)
        .when(lower(trim(col("Quantity"))) == "three", 3)
        .otherwise(col("Quantity").cast("integer"))
    )

    # 4. Standardize date format using multiple patterns
    .withColumn(
        "OrderDate",
        coalesce(
            to_date(col("OrderDate"), "yyyy/MM/dd"),
            to_date(col("OrderDate"), "dd-MM-yyyy"),
            to_date(col("OrderDate"), "MM-dd-yyyy"),
            to_date(col("OrderDate"), "yyyy.MM.dd"),
            to_date(col("OrderDate"), "dd/MM/yyyy"),
            to_date(col("OrderDate"), "dd.MM.yyyy"),
            to_date(col("OrderDate"), "MM/dd/yyyy")
        )
    )

    # 5. Clean and convert OrderAmount
    .withColumn(
        "OrderAmount",
        regexp_replace(
            col("OrderAmount").cast("string"),
            r"[$₹€,USDINR\s]",
            ""
        )
    )
    .withColumn(
        "OrderAmount",
        col("OrderAmount").cast("double")
    )

    # 6. Standardize PaymentMode
    .withColumn(
        "PaymentMode",
        lower(
            regexp_replace(
                col("PaymentMode"),
                "[^a-zA-Z]",
                ""
            )
        )
    )

    # 7. Standardize DeliveryStatus
    .withColumn(
        "DeliveryStatus",
        lower(
            regexp_replace(
                col("DeliveryStatus"),
                "[^a-zA-Z]",
                ""
            )
        )
    )

    # 8. Validate Email
    .withColumn(
        "Email",
        when(
            col("Email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            ),
            col("Email")
        ).otherwise(None)
    )

    # 9. Clean Address
    # Keep letters, numbers, spaces, commas, dots and hyphens
    .withColumn(
        "ShipAddress",
        regexp_replace(
            col("ShipAddress"),
            r"[^a-zA-Z0-9\s,.-]",
            ""
        )
    )

    # 10. FeedbackScore: convert to float
    .withColumn(
        "FeedbackScore",
        col("FeedbackScore").cast("double")
    )

    # 11. Fill nulls where possible
    .fillna({
        "Quantity": 0,
        "OrderAmount": 0.0,
        "DeliveryStatus": "unknown",
        "PaymentMode": "unknown"
    })

    # 12. Drop rows with no CustomerID or ProductName
    .na.drop(
        subset=["CustomerID", "ProductName"]
    )

    # 13. Remove duplicates by OrderID
    .dropDuplicates(
        ["OrderID"]
    )
)

# Save cleaned Orders data
df_orders.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_orders")

display(df_orders)

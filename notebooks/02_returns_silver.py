df_returns = (

   df_returns_raw
    # 2.1 Standardize column names (if needed)
    .withColumnRenamed("Return_ID", "ReturnID")
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("Customer_ID", "CustomerID")
    .withColumnRenamed("Return_Reason", "ReturnReason")
    .withColumnRenamed("Return_Date", "ReturnDate")
    .withColumnRenamed("Refund_Status", "RefundStatus")
    .withColumnRenamed("Pickup_Address", "PickupAddress")
    .withColumnRenamed("Return_Amount", "ReturnAmount")
    
    # 2.2 Clean ReturnDate → standardize date formats
    .withColumn("ReturnDate", to_date(
        regexp_replace("ReturnDate", r"[./]", "-"), "dd-MM-yyyy"
    ))
    
    # 2.3 Clean RefundStatus → lowercase, remove special characters
    .withColumn("RefundStatus", lower(regexp_replace(col("RefundStatus"), r"[^a-zA-Z]", "")))
    
    # 2.4 Clean ReturnAmount → extract numeric part regardless of currency
    .withColumn("ReturnAmount", 
        regexp_extract(col("ReturnAmount"), r"(\d+\.?\d*)", 1).cast(DoubleType())
    )
    
    # 2.5 Clean PickupAddress → remove special characters
    .withColumn("PickupAddress", initcap(trim(regexp_replace(col("PickupAddress"), r"[^a-zA-Z0-9\s]", " "))))
    
    # 2.6 Clean Product → remove extra symbols and spaces
    .withColumn("Product", initcap(trim(regexp_replace(col("Product"), r"[^a-zA-Z0-9\s]", ""))))
    
    # 2.7 Clean CustomerID → trim, fix wrong prefixes
    .withColumn("CustomerID", trim(upper(col("CustomerID"))))
    
  
)

# Step 3: Show cleaned Silver data
display(df_returns)

# Step 4: Save to Silver Delta Table
df_returns.write.mode("overwrite").format("delta").saveAsTable("silver_returns")

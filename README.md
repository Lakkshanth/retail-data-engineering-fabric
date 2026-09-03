# 🛒 Retail Data Quality and Profitability Analysis

An end-to-end **retail data engineering project** built using **Microsoft Fabric, PySpark, Delta Lake, Medallion Architecture, and Power BI**.

This project demonstrates how heterogeneous and messy retail data from **CSV, Excel, and JSON** sources can be ingested, cleaned, transformed, integrated, and converted into business-ready analytics.

---

## 📌 Project Overview

Retail organizations often receive data from multiple systems in different formats. The data may contain inconsistent column names, date formats, currencies, missing values, duplicate records, invalid emails, and inconsistent categorical values.

This project solves these challenges by implementing an end-to-end data engineering pipeline using **Microsoft Fabric**.

The pipeline follows the **Medallion Architecture**:

```text
             SOURCE DATA
                  │
       ┌──────────┼──────────┐
       │          │          │
     Orders     Returns    Inventory
      CSV        Excel        JSON
       │          │          │
       └──────────┼──────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │  FABRIC DATA        │
       │     PIPELINE        │
       │    Copy Data        │
       └──────────┬──────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │     🥉 BRONZE       │
       │      RAW DATA       │
       └──────────┬──────────┘
                  │
               PySpark
                  │
                  ▼
       ┌─────────────────────┐
       │     🥈 SILVER       │
       │ CLEANED & VALIDATED │
       │        DATA         │
       └──────────┬──────────┘
                  │
             Joins + KPIs
                  │
                  ▼
       ┌─────────────────────┐
       │      🥇 GOLD        │
       │   BUSINESS KPIs     │
       └──────────┬──────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │      POWER BI       │
       │     DASHBOARD       │
       └─────────────────────┘
🏗️ Architecture

The project uses the following architecture:

Source Systems → Fabric Data Pipeline → Bronze → Silver → Gold → Power BI

Technologies Used
Component	Technology
Cloud Platform	Microsoft Azure
Data Platform	Microsoft Fabric
Data Ingestion	Fabric Data Pipeline
Storage	Fabric Lakehouse
Processing	PySpark
Table Format	Delta Lake
Architecture	Medallion Architecture
Visualization	Power BI
Version Control	GitHub
📂 Data Sources

The project integrates three retail datasets.

1. Orders

Format: CSV

The Orders dataset contains information such as:

Order ID
Customer ID
Product Name
Quantity
Order Date
Order Amount
Delivery Status
Payment Mode
Shipping Address
Email
Promo Code
Feedback Score

Example data-quality issues include:

Quantity:
1
two
three

Order Amount:
$700
INR 72000
Rs. 25000
350USD

Dates:
2023/07/01
01-07-2023
2023.07.01
2. Returns

Format: Excel

The Returns dataset contains information such as:

Return ID
Order ID
Customer ID
Product
Return Reason
Return Date
Refund Status
Pickup Address
Return Amount
3. Inventory

Format: JSON

The Inventory dataset contains:

Product ID
Product Name
Stock
Last Stocked Date
Warehouse
Cost Price
Availability

Example raw values include:

Stock:
12
20
twenty
fifteen

Cost Price:
$700
INR 72000
Rs. 25000
350USD
🥉 Bronze Layer — Raw Data

The Bronze layer is the landing layer for source data.

A Microsoft Fabric Data Pipeline is used to ingest the source files into the Fabric Lakehouse using Copy Data activities.

Orders CSV ───────┐
Returns Excel ────┼──► Fabric Data Pipeline
Inventory JSON ───┘
                         │
                         ▼
                      Bronze
Objectives
Preserve the original source data
Maintain a raw historical copy
Enable reprocessing
Support auditing
Separate ingestion from transformation
Provide a reliable starting point for downstream processing

The Bronze layer intentionally performs minimal transformation.

🥈 Silver Layer — Data Cleaning and Transformation

The Silver layer converts raw data into clean, standardized, and analytics-ready datasets.

PySpark notebooks perform the data quality and transformation operations.

Main Data Quality Operations
Column Standardization

Raw column names such as:

Order_ID
Cust_ID
Product_Name
Order_Amount

are standardized to:

OrderID
CustomerID
ProductName
OrderAmount
Date Standardization

Multiple date formats are handled and converted into proper Spark date values.

Examples:

2023/07/01
01-07-2023
07/01/2023
2023.07.01

are standardized into a consistent date representation.

Currency Cleaning

Currency symbols and text are removed before converting the values to numeric types.

Example:

$700
INR 72000
Rs. 25000
350USD

becomes:

700.0
72000.0
25000.0
350.0

This allows numerical aggregation using Spark functions such as:

sum()
avg()
Quantity Normalization

Text-based quantities are converted into integers.

Example:

one   → 1
two   → 2
three → 3
Email Validation

Email addresses are validated using regular expressions.

Valid email addresses are retained, while invalid values are converted to NULL.

Address Cleaning

Special characters are removed from shipping addresses while retaining useful characters such as:

Letters
Numbers
Spaces
Commas
Periods
Hyphens
Null Handling

Missing values are handled according to the meaning of the field.

Examples:

Quantity        → 0
OrderAmount     → 0.0
DeliveryStatus  → unknown
PaymentMode     → unknown

Critical fields such as CustomerID and ProductName are validated before being retained.

Duplicate Removal

Duplicate Orders are removed using:

.dropDuplicates(["OrderID"])

This ensures that the Orders Silver table maintains a unique order-level grain.

Silver Tables

The cleaned data is stored as Delta tables:

silver_orders
silver_returns
silver_inventory
Silver Inventory Schema

The final inventory table contains:

product_id
ProductName
Stock
LastStocked
CostPrice
Warehouse
Available

with appropriate data types:

Stock       → Integer
LastStocked → Date
CostPrice   → Double
Warehouse   → String
Available   → Boolean
🥇 Gold Layer — Business Aggregation

The Gold layer combines the cleaned Silver tables and applies business logic.

The following tables are integrated:

silver_orders
       │
       ├──── OrderID ────► silver_returns
       │
       └── ProductName ──► silver_inventory

The result is an enriched dataset containing order, return, and inventory information.

📊 Gold Table Grain

The Gold table is maintained at:

ProductName + OrderMonth

grain.

This means each row represents the KPIs for a specific product during a specific month.

Example:

ProductName       OrderMonth
--------------------------------
Apple iPhone13    2023-06
Samsung Galaxy    2023-07
Oneplus Nord      2023-07
📈 Business KPIs

The Gold layer calculates the following metrics:

KPI	Description
Total Orders	Number of unique orders
Unique Customers	Number of distinct customers
Total Returns	Number of unique returns
Return Rate	Percentage of orders that were returned
Total Revenue	Total order amount
Average Order Value	Average value of an order
Total Stock	Inventory quantity
Average Cost	Average product cost
Net Profit	Project-defined profitability metric
Return Rate

The return rate is calculated as:

Return Rate =
(Total Returns / Total Orders) × 100
Total Revenue
Total Revenue = SUM(OrderAmount)
Average Order Value
Average Order Value =
Total Revenue / Total Orders
🔗 Data Integration

The Gold transformation uses Spark joins.

Orders → Returns

The tables are joined using:

OrderID

A LEFT JOIN is used so that orders without returns are not removed.

Orders
   │
   │ OrderID
   ▼
Returns
Orders → Inventory

Orders are joined with Inventory using:

ProductName
Orders
   │
   │ ProductName
   ▼
Inventory

Table aliases are used to avoid column ambiguity:

orders.alias("o")
returns.alias("r")
inventory.alias("i")
🧮 PySpark Processing

Example Gold aggregation:

df_kpi = (
    df_enriched
    .groupBy("ProductName", "OrderMonth")
    .agg(
        countDistinct("OrderID").alias("Total_Orders"),
        countDistinct("CustomerID").alias("Unique_Customers"),
        countDistinct("ReturnID").alias("Total_Returns"),
        round(
            (countDistinct("ReturnID") /
             countDistinct("OrderID")) * 100,
            2
        ).alias("Return_Rate_%"),
        round(sum("OrderAmount"), 2).alias("Total_Revenue"),
        round(avg("OrderAmount"), 2).alias("Avg_Order_Value"),
        sum("Stock").alias("Total_Stock"),
        round(avg("CostPrice"), 2).alias("Avg_Cost")
    )
)

The final Gold table is stored as:

gold_product_month_kpis
📊 Power BI Dashboard

The Gold Delta table is designed to be consumed by Power BI.

Potential dashboard components include:

KPI Cards
Total Revenue
Total Orders
Unique Customers
Return Rate
Total Profit
Visualizations
Revenue by Product
Revenue by Month
Returns by Product
Return Rate by Product
Inventory by Product
Profitability by Product
Filters
Product
Order Month

The goal is to provide business users with an interactive view of retail performance without requiring them to work directly with the raw datasets.

🔄 End-to-End Pipeline
                    SOURCE SYSTEMS
                         │
          ┌──────────────┼──────────────┐
          │              │              │
       Orders          Returns       Inventory
        CSV             Excel           JSON
          │              │              │
          └──────────────┼──────────────┘
                         │
                         ▼
                FABRIC DATA PIPELINE
                         │
                         ▼
                     BRONZE
                    Raw Data
                         │
                         ▼
                     PySpark
                         │
                         ▼
                     SILVER
              Cleaned & Validated
                         │
                         ▼
                  Spark Joins
                         │
                         ▼
              Product + Month KPIs
                         │
                         ▼
                      GOLD
                  Business Data
                         │
                         ▼
                    POWER BI
                    Dashboard
📁 Repository Structure
retail-data-engineering-fabric/
│
├── README.md
├── .gitignore
│
├── architecture/
│   └── architecture.png
│
├── notebooks/
│   ├── 01_orders_silver.py
│   ├── 02_returns_silver.py
│   ├── 03_inventory_silver.py
│   └── 04_gold_kpis.py
│
├── pipeline/
│   └── README.md
│
├── screenshots/
│   ├── fabric_pipeline.png
│   ├── bronze_layer.png
│   ├── silver_orders.png
│   ├── silver_inventory.png
│   ├── gold_kpis.png
│   └── powerbi_dashboard.png
│
├── powerbi/
│   └── README.md
│
└── data/
    └── README.md
🚀 How to Run
Prerequisites
Microsoft Azure subscription
Microsoft Fabric workspace
Fabric Lakehouse
Power BI
Source datasets
GitHub account
Steps
1. Create Fabric Workspace

Create a Microsoft Fabric workspace with appropriate capacity.

2. Create Lakehouse

Create a Fabric Lakehouse to store the Bronze, Silver, and Gold data.

3. Configure Data Pipeline

Create a Fabric Data Pipeline using Copy Data activities to ingest:

Orders CSV
Returns Excel
Inventory JSON
4. Run Bronze Ingestion

Load the source data into the Bronze layer.

5. Run Silver Notebooks

Execute the PySpark notebooks to:

Clean the data
Standardize schemas
Validate values
Handle missing values
Remove duplicates
Convert data types
6. Validate Silver Tables

Verify:

silver_orders
silver_returns
silver_inventory
7. Run Gold Notebook

Join the Silver tables and calculate Product + Month KPIs.

8. Connect Power BI

Connect Power BI to the Gold table:

gold_product_month_kpis
9. Build Dashboard

Create interactive visualizations for revenue, orders, returns, inventory, and profitability.

🧠 Key Data Engineering Concepts Demonstrated

This project demonstrates practical knowledge of:

Medallion Architecture
ETL / ELT
Data ingestion
Data quality
Data validation
Schema standardization
Data type conversion
Data cleaning
Regex transformations
Null handling
Duplicate removal
PySpark DataFrames
Spark SQL functions
Spark joins
Aggregations
Delta Lake
Microsoft Fabric Lakehouse
Data pipelines
Business KPI development
Power BI reporting
GitHub version control
⚠️ Data Quality Considerations

Several data-quality challenges were intentionally handled in this project.

Examples include:

Inconsistent column names
Inconsistent dates
Currency symbols
Text-based quantities
Missing values
Invalid emails
Special characters
Duplicate orders
Inconsistent categorical values

The Silver layer acts as the data-quality boundary between raw source data and business analytics.

🔍 Important Engineering Considerations
Data Grain

Each layer has a different logical grain.

Orders
One record per OrderID
Inventory
One record per Product
Gold
One record per ProductName + OrderMonth

Understanding data grain is important to prevent incorrect aggregations.

Join Duplication

When joining a one-to-many relationship, duplicate rows can potentially inflate measures such as revenue.

For a production implementation, orders and returns should be aggregated at the appropriate grain before calculating financial metrics.

🔮 Future Improvements

The project can be extended with:

Incremental data ingestion
Automated data-quality checks
Pipeline scheduling
Failure handling and retry mechanisms
Data lineage
Microsoft Purview integration
Data governance
Slowly Changing Dimensions
Star schema / dimensional modeling
Automated Power BI refresh
CI/CD deployment
Monitoring and alerting
Data quality dashboards
Historical inventory tracking
🎯 Project Outcome

The project demonstrates how Microsoft Fabric can be used to build an end-to-end data engineering solution that transforms heterogeneous and low-quality retail data into reliable business insights.

The final architecture provides:

Reliable ingestion
       +
Data quality
       +
Scalable transformation
       +
Business aggregation
       +
Interactive analytics

resulting in a complete retail data engineering and analytics pipeline.

👨‍💻 Author

Lakkshanth

Technologies

Microsoft Fabric Azure PySpark Delta Lake Lakehouse Power BI GitHub

⭐ If you found this project useful, feel free to explore the notebooks and architecture documentation.

### One small thing before you commit it

The README contains:

```markdown
![Architecture](architecture/architecture.png)

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import dlt
import shutil
import os

spark = SparkSession.builder \
    .appName("Read Parquet from ADLS Gen2") \
    .getOrCreate()

# COMMAND ----------

storage_account_name = "wathba"
storage_account_key = "QpocKizFnzxjlsLPkjVQqknb60GrQuIG6lsZ5kV0pCuaXcioOxgk0FTwQliFrXbTVsMd2hHzxsRS+AStBKSeiw=="
container_name = "wajbah"

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)
Chefs_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/Chefs"
ChefPromoCode_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/ChefPromoCode"
Companies_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/Companies"
Customers_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/Customers"
Itemrate_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/ItemRateRecords"
menuitems_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/MenuItems"
Order_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/Orders"
OrderMenuItems_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/OrderMenuItem"
Promocode_source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Bronze/dbo/PromoCodes"

# COMMAND ----------

# Static DataFrames to infer schemas
static_chefs_df = spark.read.format("parquet").load(Chefs_source_path)
static_chefpromocode_df = spark.read.format("parquet").load(ChefPromoCode_source_path)
static_companies_df = spark.read.format("parquet").load(Companies_source_path)
static_customers_df = spark.read.format("parquet").load(Customers_source_path)
static_itemrate_df = spark.read.format("parquet").load(Itemrate_source_path)
static_menuitems_df = spark.read.format("parquet").load(menuitems_source_path)
static_order_df = spark.read.format("parquet").load(Order_source_path)
static_ordermenuitems_df = spark.read.format("parquet").load(OrderMenuItems_source_path)
static_promocode_df = spark.read.format("parquet").load(Promocode_source_path)

# Infer schemas from static DataFrames
chefs_schema = static_chefs_df.schema
chefpromocode_schema = static_chefpromocode_df.schema
companies_schema = static_companies_df.schema
customers_schema = static_customers_df.schema
itemrate_schema = static_itemrate_df.schema
menuitems_schema = static_menuitems_df.schema
order_schema = static_order_df.schema
ordermenuitems_schema = static_ordermenuitems_df.schema
promocode_schema = static_promocode_df.schema

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="chefs_bronze",
  comment="Live Bronze Table For Chefs",
  table_properties={
    "quality": "bronze"
  }
)
def chefs_bronze_table():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(chefs_schema).format("parquet").load(Chefs_source_path)
    bronze_df.createOrReplaceTempView("Chefs_bronze")
    return bronze_df

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="chefpromo_bronze",
  comment="Live Bronze Table For Chef Promo Code",
  table_properties={
    "quality": "bronze"
  }
)
def chefpromo_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(chefpromocode_schema).format("parquet").load(ChefPromoCode_source_path)
    bronze_df.createOrReplaceTempView("chefpromo_bronze")
    return bronze_df

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="promocode_bronze",
  comment="Live Bronze Table For Promo Code",
  table_properties={
    "quality": "bronze"
  }
)
def promocode_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(promocode_schema).format("parquet").load(Promocode_source_path)
    bronze_df.createOrReplaceTempView("promocode_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="chefs_silver",
  comment="Live Silver Table For Chefs",
  table_properties={
    "quality": "silver"
  }
)
def chefs_silver():
    # Read from the Bronze tables as streaming sources
    chefs_bronze_df = dlt.read_stream("chefs_bronze")
   
    # Join chefs_bronze_df with ChefPromoCode_BronzeTable and PromoCode_BronzeTable using SQL
    silver_df = spark.sql("""
        SELECT 
            cb.ChefId,
            CONCAT_WS(' ', cb.ChefFirstName, cb.ChefLastName) AS Chef_name,
            cb.RestaurantName,
            DAY(cb.BirthDate) AS BirthDay,
            MONTH(cb.BirthDate) AS BirthMonth,
            YEAR(cb.BirthDate) AS BirthYear,
            cb.Rating,
            cb.Governorate,
            cb.City,
            cb.Role,
            cb.Active,
            datediff(current_date(), cb.BirthDate) AS Age 
        FROM 
            chefs_bronze cb
    """)
    silver_df.createOrReplaceTempView("chefs_silver")
    return silver_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="promocode_silver",
  comment="Live Silver Table For PromoCode",
  table_properties={
    "quality": "silver"
  }
)
def promocode_silver():
    # Read from the Bronze tables as streaming sources
    promocode_bronze_df = dlt.read_stream("promocode_bronze")
    
    # Join chefs_bronze_df with ChefPromoCode_BronzeTable and PromoCode_BronzeTable using SQL
    silver_df = spark.sql("""
        SELECT 
        *
        FROM 
            promocode_bronze 
    """)
    silver_df.createOrReplaceTempView("promocode_silver")
    return silver_df


# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="chefpromo_silver",
  comment="Live Silver Table For PromoCode",
  table_properties={
    "quality": "silver"
  }
)
def chefpromo_silver():
    # Read from the Bronze tables as streaming sources
    promocode_bronze_df = dlt.read_stream("chefpromo_bronze")
    
    # Join chefs_bronze_df with ChefPromoCode_BronzeTable and PromoCode_BronzeTable using SQL
    silver_df = spark.sql("""
        SELECT 
        *
        FROM 
            chefpromo_bronze 
    """)
    silver_df.createOrReplaceTempView("chefpromo_silver")
    return silver_df

# COMMAND ----------

@dlt.table(
  name="Companies_bronze",
  comment="Live Bronze Table For Chef Companies",
  table_properties={
    "quality": "bronze"
  }
)
def Companies_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(companies_schema).format("parquet").load(Companies_source_path)
    bronze_df.createOrReplaceTempView("Companies_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(  
  name="Companies_silver",
  comment="Live Silver Table For Companies",
  table_properties={
    "quality": "silver"
  }
)
def Companies_silver():
    # Read from the Bronze tables as streaming sources
    Companies_bronze_df = dlt.read_stream("Companies_bronze")
    
    # Join chefs_bronze_df with ChefPromoCode_BronzeTable and PromoCode_BronzeTable using SQL
    silver_df = spark.sql("""
        SELECT 
            CompanyId,
            CompanyName,
            Wallet,
            DeliveryFees,
            Area
        FROM 
            Companies_bronze 
    """)
    silver_df.createOrReplaceTempView("Companies_silver")
    return silver_df

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="Customers_bronze",
  comment="Live Bronze Table For Customers",
  table_properties={
    "quality": "bronze"
  }
)
def Customers_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(customers_schema).format("parquet").load(Customers_source_path)
    bronze_df.createOrReplaceTempView("Customers_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="Customers_silver",
  comment="Live Silver Table For Customers",
  table_properties={
    "quality": "silver"
  }
)
def Customers_silver():
    # Read from the Bronze tables as streaming sources
    Customers_bronze_df = dlt.read_stream("Customers_bronze")
    
    silver_df = spark.sql("""
        SELECT 
            CustomerId,
            concat(FirstName,' ', LastName) AS Name,
            Wallet,
            DAY(BirthDate) AS BirthDay,
            MONTH(BirthDate) AS BirthMonth,
            YEAR(BirthDate) AS BirthYear,
            datediff(current_date(), BirthDate) AS Age ,
            UsedCoupones,
            Role,
            Favourites,
            State
        FROM 
            Customers_bronze 
    """)
    silver_df.createOrReplaceTempView("Customers_silver")
    return silver_df

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="menuitems_bronze",
  comment="Live Bronze Table For menuitems",
  table_properties={
    "quality": "bronze"
  }
)
def menuitems_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(menuitems_schema).format("parquet").load(menuitems_source_path)
    bronze_df.createOrReplaceTempView("menuitems_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="menuitems_silver",
  comment="Live Silver Table For menuitems",
  table_properties={
    "quality": "silver"
  }
)
def menuitems_silver():
    # Read from the Bronze tables as streaming sources
    menuitems_bronze_df = dlt.read_stream("menuitems_bronze")
    
    silver_df = spark.sql("""
        SELECT 
            MenuItemId,
            Category,
            Occassions,
            EstimatedTime,
            OrderingTime,
            HealthyMode,
            CreatedOn ,
            UpdatedOn,
            ChefId,
            PriceLarge,
            PriceMedium,
            PriceSmall,
            Rate
        FROM 
            menuitems_bronze 
    """)
    silver_df.createOrReplaceTempView("menuitems_silver")
    return silver_df

# COMMAND ----------

# Define the Bronze Table as a streaming live table
@dlt.table(
  name="Order_bronze",
  comment="Live Bronze Table For Order",
  table_properties={
    "quality": "bronze"
  }
)
def Order_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(order_schema).format("parquet").load(Order_source_path)
    bronze_df.createOrReplaceTempView("Order_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="Order_silver",
  comment="Live Silver Table For Order",
  table_properties={
    "quality": "silver"
  }
)
def Order_silver():
    # Read from the Bronze tables as streaming sources
    Order_bronze_df = dlt.read_stream("Order_bronze")
    
    silver_df = spark.sql("""
        SELECT 
            OrderId,
            DeliveryFees,
            CreatedOn,
            DeliveryTime,
            Status,
            Copoun ,
            CashDelivered,
            EstimatedTime,
            CompanyId,
            CustomerId,
            SubTotal,
            TotalPrice,
            ChefId
        FROM 
            Order_bronze 
    """)
    silver_df.createOrReplaceTempView("Order_silver")
    return silver_df

# COMMAND ----------

@dlt.table(
  name="OrderMenuItems_bronze",
  comment="Live Bronze Table For OrderMenuItems",
  table_properties={
    "quality": "bronze"
  }
)
def OrderMenuItems_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(ordermenuitems_schema).format("parquet").load(OrderMenuItems_source_path)
    bronze_df.createOrReplaceTempView("OrderMenuItems_bronze")
    return bronze_df

# COMMAND ----------

@dlt.table(
  name= "OrderMenuItems_silver",
  comment="Live Silver Table For OrderMenuItems",
  table_properties={
    "quality": "silver"
  }
)
def OrderMenuItems_silver():
    # Read from the Bronze tables as streaming sources
    Order_bronze_df = dlt.read_stream("OrderMenuItems_bronze")
    menuitems_bronze_df = dlt.read_stream("menuitems_bronze")
    
    silver_df = spark.sql("""
SELECT 
    om.*,
    CASE
        WHEN om.size = 'Large' THEN m.PriceLarge
        WHEN om.size = 'Medium' THEN m.PriceMedium
        WHEN om.size = 'Small' THEN m.PriceSmall
    END AS Price,
    om.quantity * 
    CASE
        WHEN om.size = 'Large' THEN m.PriceLarge
        WHEN om.size = 'Medium' THEN m.PriceMedium
        WHEN om.size = 'Small' THEN m.PriceSmall
    END AS TotalPrice
FROM 
    OrderMenuItems_bronze AS om
JOIN
    menuitems_bronze AS m
ON
    om.MenuItemId = m.MenuItemId
    """)
    silver_df.createOrReplaceTempView("OrderMenuItems_silver")
    return silver_df 


# COMMAND ----------

@dlt.table(
  name="Itemrate_bronze",
  comment="Live Bronze Table For OrderMenuItems",
  table_properties={
    "quality": "bronze"
  }
)
def Itemrate_bronze():
    # Read from the streaming source table
    bronze_df = spark.readStream.schema(itemrate_schema).format("parquet").load(Itemrate_source_path)
    bronze_df.createOrReplaceTempView("Itemrate_bronze")
    return bronze_df

# COMMAND ----------

# Define the Silver Table as a streaming live table
@dlt.table(
  name="Itemrate_silver",
  comment="Live Silver Table For Itemrate",
  table_properties={
    "quality": "silver"
  }
)
def Itemrate_silver():
    # Read from the Bronze tables as streaming sources
    promocode_bronze_df = dlt.read_stream("Itemrate_bronze")
    
    # Join chefs_bronze_df with ChefPromoCode_BronzeTable and PromoCode_BronzeTable using SQL
    silver_df = spark.sql("""
        SELECT 
        *
        FROM 
            Itemrate_bronze 
    """)
    silver_df.createOrReplaceTempView("Itemrate_silver")
    return silver_df

# COMMAND ----------

checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints"

# Function to delete directory in ADLS Gen2
def delete_adls_directory(path):
    dbutils.fs.rm(path, True)

# Function to delete parquet files in a given directory
def delete_parquet_files(path):
    files = dbutils.fs.ls(path)
    for file in files:
        if file.path.endswith(".parquet"):
            dbutils.fs.rm(file.path, False)

# Delete the checkpoints directory
delete_adls_directory(checkpoint_location)


# COMMAND ----------

# Define the writeStream operation with the checkpoint location specified
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/chefs_silver_table"
output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/Chefs"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("chefs_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()


# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/promocode_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/promocode_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("promocode_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/chefpromo_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/chefpromo_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("chefpromo_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/Companies_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/Companies_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("Companies_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()


# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/Customers_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/Customers_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("Customers_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/menuitems_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/menuitems_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("menuitems_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/Order_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/Order_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("Order_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .option("mergeSchema", "true") \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/OrderMenuItems_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/OrderMenuItems_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("ordermenuitems_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# COMMAND ----------

output_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/dbo/Itemrate_silver"
checkpoint_location = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Silver/checkpoints/Itemrate_silver"

# Read from the Delta table created by DLT
silver_df = spark.readStream.format("delta").table("Itemrate_silver")

# Delete all parquet files in the output directory
delete_parquet_files(output_path)

# Define the writeStream operation with the checkpoint location specified
streaming_query = silver_df.writeStream \
    .outputMode("append") \
    .format("Delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

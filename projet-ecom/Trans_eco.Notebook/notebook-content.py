# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e650871a-e3a3-419b-acce-db9f744c428b",
# META       "default_lakehouse_name": "lakehousecom",
# META       "default_lakehouse_workspace_id": "fd352658-0b24-4b93-a091-6ec635bbc1c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "e650871a-e3a3-419b-acce-db9f744c428b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# <mark>****PROJET E-COMMERCE**
# **</mark>


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**TRANSFORMATION TABLE CUSTOMER**</mark>

# CELL ********************

df_customer = spark.read.format("csv").option("header","true").load("Files/Bronze/dim_customer.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>TRANSFORMATION TABLE PRODUT</mark>**

# CELL ********************

df_product = spark.read.format("csv").option("header","true").load("Files/Bronze/dim_product.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**TRANSFORMATION TABLE VENTE**</mark>

# CELL ********************

df_vente = spark.read.format('csv').option('header','true').load('Files/Bronze/fact_sales.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vente = df_vente.withColumnRenamed('total_price','price')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vente = df_vente.withColumn("total_price", col('quantity') * col('price'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>****TRANSFORMATION TABLE DATE**
# **</mark>

# CELL ********************

dim_date = spark.read.format('csv').option('header','true').load('Files/Bronze/dim_date.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**INGESTION**</mark>

# CELL ********************

df_customer.write.format('parquet').mode('overwrite').save('Files/silver/dim_custmer')
df_product.write.format('parquet').mode('overwrite').save('Files/silver/dim_produit')
df_vente.write.format('parquet').mode('overwrite').save('Files/silver/fact_vente')
dim_date.write.format('parquet').mode('overwrite').save('Files/silver/dim_date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d884e221-370b-45ae-b7c1-fda4e4a4fbd2",
# META       "default_lakehouse_name": "lakehousecom",
# META       "default_lakehouse_workspace_id": "edad9ba8-9b8c-44e5-823f-e4212ccf5f24",
# META       "known_lakehouses": [
# META         {
# META           "id": "d884e221-370b-45ae-b7c1-fda4e4a4fbd2"
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

df_customer = spark.read.format("csv").option("header","true").load("Files/bronze/dim_customer.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<mark>TRANSFORMATION TABLE PRODUT</mark>**

# CELL ********************

df_product = spark.read.format("csv").option("header","true").load("Files/bronze/dim_product.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>**TRANSFORMATION TABLE VENTE**</mark>

# CELL ********************

df_vente = spark.read.format('csv').option('header','true').load('Files/bronze/fact_sales.csv')

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

dim_date = spark.read.format('csv').option('header','true').load('Files/bronze/dim_date.csv')

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

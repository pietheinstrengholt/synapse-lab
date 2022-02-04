#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Set arguments
dfDataOriginalPath = "/bronze/"
dfDataChangedPath = "/silver/"
cw_database = "demodatabase"
cw_table = "SalesLT.Address"


# In[ ]:


get_ipython().run_cell_magic('pyspark', '', '\nfrom pyspark import *\nfrom pyspark.sql.window import Window\nfrom pyspark.sql.functions import *\nfrom pyspark.sql import Row\nfrom pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, DateType\nfrom typing import List\nfrom datetime import datetime\n\nprint("Database: " + cw_database)\nprint("Table: " + cw_table)\n\n# Read CSV data from landing zone location\ndataChanged = spark.read.load(\'abfss://synapsedeltademo@synapsedeltademo.dfs.core.windows.net/\' + dfDataOriginalPath + cw_database + \'/\' + cw_table + \'.parquet\', format=\'parquet\', header=True)\ndataChanged.printSchema()\ndataChanged.show()')


# In[ ]:


get_ipython().run_cell_magic('pyspark', '', '\nfrom datetime import date\ncurrent_date = datetime.today().date()\n\nfrom notebookutils import mssparkutils\n\ntry:\n    # Read original data - this is your scd type 2 table holding all data\n    dataOriginal = spark.read.load(\'abfss://synapsedeltademo@synapsedeltademo.dfs.core.windows.net/\' + dfDataChangedPath + "/" + cw_database + "/" + cw_table, format=\'delta\')\nexcept:\n    # Use first load when no data exists yet\n    newOriginalData = dataChanged.withColumn(\'current\', lit(True)).withColumn(\'effectiveDate\', lit(current_date)).withColumn(\'endDate\', lit(date(9999, 12, 31)))\n    newOriginalData.write.format("delta").mode("overwrite").save(\'abfss://synapsedeltademo@synapsedeltademo.dfs.core.windows.net/\' + dfDataChangedPath + "/" + cw_database + "/" + cw_table)\n    newOriginalData.show()\n    newOriginalData.printSchema()\n    mssparkutils.notebook.exit("Done loading data! Newly loaded data will be used to generate original data.")')


# In[ ]:


if cw_table == "SalesLT.Address":
    primaryKey = "AddressID"
elif cw_table == "SalesLT.Customer":
    primaryKey = "CustomerID"
else:
    mssparkutils.notebook.exit("Exit! No primary key defined!")


# In[ ]:


# Prepare for merge, rename columns of newly loaded data, append 'src_'
from pyspark.sql import functions as F

# Capture column names of incoming dataset
columnNames = dataChanged.schema.names

# Rename all columns in dataChanged, prepend src_, and add additional columns
df_new = dataChanged.select([F.col(c).alias("src_"+c) for c in dataChanged.columns])
src_columnNames = df_new.schema.names
df_new2 = df_new.withColumn('src_current', lit(True)).withColumn('src_effectiveDate', lit(current_date)).withColumn('src_endDate', lit(date(9999, 12, 31)))
df_new2.printSchema()

import hashlib

# Create dynamic columns
src_primaryKey = 'src_' + primaryKey

# FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = dataOriginal.join(df_new2, (df_new2[src_primaryKey] == dataOriginal[primaryKey]), how='fullouter')

# Derive new column to indicate the action
df_merge = df_merge.withColumn('action',
    when(md5(concat_ws('+', *columnNames)) == md5(concat_ws('+', *src_columnNames)), 'NOACTION')
    .when(df_merge.current == False, 'NOACTION')
    .when(df_merge[src_primaryKey].isNull() & df_merge.current, 'DELETE')
    .when(df_merge[src_primaryKey].isNull(), 'INSERT')
    .otherwise('UPDATE')
)

df_merge.show()

# Generate target selections based on action codes
column_names = columnNames + ['current', 'effectiveDate', 'endDate']
src_column_names = src_columnNames + ['src_current', 'src_effectiveDate', 'src_endDate']

# Generate target selections based on action codes
column_names = columnNames + ['current', 'effectiveDate', 'endDate']
src_column_names = src_columnNames + ['src_current', 'src_effectiveDate', 'src_endDate']

# For records that needs no action
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

# For records that needs insert only
df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(src_column_names)
df_merge_p2_1 = df_merge_p2.select([F.col(c).alias(c.replace(c[0:4], "")) for c in df_merge_p2.columns])

# For records that needs to be deleted
df_merge_p3 = df_merge.filter(df_merge.action == 'DELETE').select(column_names).withColumn('current', lit(False)).withColumn('endDate', lit(current_date))

# For records that needs to be expired and then inserted
df_merge_p4_1 = df_merge.filter(df_merge.action == 'UPDATE').select(src_column_names)
df_merge_p4_2 = df_merge_p4_1.select([F.col(c).alias(c.replace(c[0:4], "")) for c in df_merge_p2.columns])

# Replace src_ alias in all columns
df_merge_p4_3 = df_merge.filter(df_merge.action == 'UPDATE').withColumn('endDate', date_sub(df_merge.src_effectiveDate, 1)).withColumn('current', lit(False)).select(column_names)

# Union all records together
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3).unionAll(df_merge_p4_2).unionAll(df_merge_p4_3)

df_merge_final.show()

# At last, you can overwrite existing data using this new data frame
df_merge_final.write.format("delta").mode("overwrite").save('abfss://synapsedeltademo@synapsedeltademo.dfs.core.windows.net/' + dfDataChangedPath + "/" + cw_database + "/" + cw_table)


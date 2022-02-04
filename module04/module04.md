# Module 04 - Create Spark Script, Process to Silver layer (Delta)

[< Previous Module](../module03/module03.md) - **[Home](../README.md)** - [Next Module >](../module05/module05.md)

## :dart: Objectives

* The objective for this module is to move data from our bronze layer into the silver layer. In the bronze layer data typically has different file types and formats, while in the Silver layer we standardize on a fixed file format. In our example we will use Delta as the file format. Also we will use Spark to process any data.

## 1. Deploy Spark Pool

1. Navigate to managed environment and create a new Spark Pool.

    ![Spark Pool](../module04/screen01.png)  

2. Next go to Develop and create your first notebook: **BronzeToSilver**. For our demo pipeline you create a new notebook and start with a parameter cell, defining the paths for storing data. The cw_database and cw_table are for this demo pre-populated, but will be overwritten when arguments are correctly set with the database and table names. For the data itself we will use slowly changing dimensions, so each time data is changed, we will compare it with the previous dataset and add it. Within the script I've defined a section for the primary keys. Feel free to add more keys.

    ![Create Script](../module04/screen02.png)

    [BronzeToSilver.py](../module04/BronzeToSilver.py)

    ```
    # Set arguments
    dfDataOriginalPath = "/bronze/"
    dfDataChangedPath = "/silver/"
    cw_database = "demodatabase"
    cw_table = "SalesLT.Address"
    ```

    ```
    %%pyspark

    from pyspark import *
    from pyspark.sql.window import Window
    from pyspark.sql.functions import *
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, DateType
    from typing import List
    from datetime import datetime

    print("Database: " + cw_database)
    print("Table: " + cw_table)

    # Read CSV data from landing zone location
    dataChanged = spark.read.load('abfss://synapsedeltademo@synapsedeltademo.dfs.core.windows.net/' + dfDataOriginalPath + cw_database + '/' + cw_table + '.parquet', format='parquet', header=True)
    dataChanged.printSchema()
    dataChanged.show()
    ```

3. When ready and tested, go back to your Pipeline, open the ForEach and drag in a Notebook Step. Select the spark pool, notebook and configure the arguments. These will be passed into our script.

    ![Add notebook step](../module04/screen03.png)

4. Trigger the pipeline and validate the monitoring. If everything works as expected your Delta folders should be created.

    ![Trigger pipeline](../module04/screen04.png) 

5. Navigate to your delta folder. Right click to select top 100. Select delta as the file format, and validate the results.

    ![See data](../module04/screen05.png)

6. Next, go back to your Azure SQL database. Select your Sample database and open the Query Editor. You should whitelist your IP-address. Update the first record:

    `UPDATE [SalesLT].[Customer] SET FirstName = 'Jason' WHERE CustomerID = 1`

    ![Update record](../module04/screen06.png)

7. Head back to your Synapse Studio, Trigger the workflow again and validate that your slowly changing dimension works as expected:

    ![Validate SCD2](../module04/screen07.png)  

<div align="right"><a href="#module-04---setup-delta">↥ back to top</a></div>


## :tada: Summary

In this module module you used Spark to transform our data into Slowly Changing Dimension, stored using Delta. The Notebook you added to your pipeline.

[Continue >](../module04/module04.md)
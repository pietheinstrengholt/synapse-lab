# Module 03 - Setup Data Lake, Ingest data to Bronze layer (Parquet)

[< Previous Module](../module02/module02.md) - **[Home](../README.md)** - [Next Module >](../module04/module04.md)

## :dart: Objectives

* Create folder structure for datalake.
* Allow Azure Services to access Azure SQL database.

## 1. Create Data Lake folder structure

1. Navigate to your Azure Synapse Storage Account and create three folders: **bronze**, **silver** and **gold**.

    ![Create lake house folders](../module03/screen01.png)  

2. Inside each folder create a folder names: **demodatabase**.

    ![Create database folders](../module02/screen02.png)  

3. Navigate to your Azure SQL Firewall Settings and allow Azure services to connect your Azure SQL server. Click on **Save**.

    ![Configure Firewall](../module02/screen02a.png)

4. Open Synapse Studio and navigate to Linked Services under Management. Click on **New**.

    ![Create a linked service](../module02/screen03.png)  

5. Search for Azure SQL. Click on **Continue**.

    ![Create a linked service](../module02/screen04.png)

6. Select your newly created Azure SQL database from your subscription and resource list. Provide your SQL credentials. Hit test connection and click on **Create**.

    ![Create a linked service](../module02/screen05.png)

7. Next we will create a new Pipeline. Click on Integrate and **Create a new pipeline**.

    ![Create a new pipeline](../module02/screen06.png) 

8. Drag in a lookup step from the items on the left.

    ![Create a lookup step](../module02/screen07.png)

9. Under settings, select Query and copy paste the code block from below.

    SELECT table_Schema+'.'+TABLE_NAME AS Table_Name FROM information_Schema.tables WHERE TABLE_SCHEMA = 'SalesLT' AND TABLE_TYPE = 'BASE TABLE'

    ![Create query for looking up tables](../module02/screen08.png)    


<div align="right"><a href="#module-03---setup-datalake">↥ back to top</a></div>


## :tada: Summary

This module provided an overview of how to provision an Azure SQL Database using the Azure Portal.

[Continue >](../module04/module04.md)
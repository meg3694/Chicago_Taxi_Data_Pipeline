Building a Data Pipeline for Chicago Taxi Trip Dataset

Introduction
This README provides instructions on how to build a data pipeline for the Chicago Taxi Trip dataset.

The dataset is available on the City of Chicago Data Portal website. The data is available in CSV format and can be downloaded using the following link: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew/data.

The goal of this data pipeline is to extract, transform, and load the data from the Chicago Taxi Trip dataset into a database for further analysis.

Prerequisites
Python 3.7 or later
PySpark Framework
Java SDE 8 or later 
Pandas Python library
Snowflake (Data Warehouse)
Airflow (Workflow Orchestration)
Jupyter Notebook
Intellij IDE
Github

Setup
Set up the environment for building the data pipeline by installing the softwares and the IDE.

Create a new python project and install the python libraries.

Download the Chicago Taxi Trip dataset from the City of Chicago Data Portal website.

The pipeline will perform the following steps:

Read and Load the CSV file into a Spark dataframe.
Clean and transform the data.
Load the data into the Snowflake database.
Once the pipeline has completed successfully, SQL queries can be used to analyze the data in the database.
Orchestration of the data pipeline is carried out by Airflow with the creation of DAGs that are defined task to perform the jobs.

Conclusion
This README provided instructions on how to build a data pipeline for the Chicago Taxi Trip dataset. By following the steps outlined in this document, one should be able to successfully extract, transform, and load the data from the Chicago Taxi Trip dataset for further analysis.



# Chicago_Taxi_Data_Pipeline
Please see the "`Architectural Overview CF Part 2.drawio`" file as a response to the Part 2 of the assignment. To open the file please use the draw.io application (offline or online). 



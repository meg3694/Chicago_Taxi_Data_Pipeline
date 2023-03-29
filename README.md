
# Chicago_Taxi_Data_Pipeline

## Part 1 - Data Pipeline
Building a Data Pipeline for Chicago Taxi Trip Dataset

#### Introduction:

This README provides instructions on how to build a data pipeline for the Chicago Taxi Trip dataset.

The dataset is available on the City of Chicago Data Portal website. The data is available in CSV format and can be downloaded using the following link: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew/data.

The goal of this data pipeline is to extract, transform, and load the data from the Chicago Taxi Trip dataset into a database for further analysis.

#### Prerequisites:
- Python 3.7 or later 
- PySpark Framework 
- Java SDE 8 or later 
- Pandas Python library 
- Snowflake (Data Warehouse)
- Airflow (Workflow Orchestration)
- Jupyter Notebook (for quick prototyping/PoC)
- Intellij IDE or an IDE for your choice 
- Github & Cloud Account (for using cloud native services)

#### Setup:
1. Set up the environment for building the data pipeline by installing the softwares and the IDE.
2. Create a new python project and install the python libraries. 
3. Download the Chicago Taxi Trip dataset from the City of Chicago Data Portal website. 
4. The pipeline will perform the following steps:
   - Read and Load the CSV file into a Spark dataframe.
   - Clean and transform the data.
   - Load the data into the Snowflake database.
   - Once the pipeline has completed successfully, SQL queries can be used to analyze the data in the database.
   - Orchestration of the data pipeline is carried out by Airflow with the creation of DAGs that are defined task to perform the jobs.
5. Please see the comments in the code to understand the pipeline logic such as what has been cleaned and transformed, specifically the file "`data_pipeline.py`"
6. Please see the file "`dag_for_scheduling.py`" file for pipeline orchestration tasks

Additionally, I would like to mention that I do not have a tool stack at my disposal such as database, or API gateway or a server, in short, unavailability of such critical tool stack/set limited my output to an extent

#### Conclusion:
This README provided instructions on how to build a data pipeline for the Chicago Taxi Trip dataset. By following the steps outlined in this document, one should be able to successfully extract, transform, and load the data from the Chicago Taxi Trip dataset for further analysis.

## Part 2 - Architectural Overview
- Please see the "`Architectural Overview CF Part 2.drawio`" file as a response to Part 2 of the assignment. To open the file please use the draw.io application (offline or online), simply google for draw.io, go to the tool, upload the design by choosing Device option. 



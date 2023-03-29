
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

### Short Explanation on the Architecture 

- As we all know Data have become one of the primary business assets, and for a taxi service company good quality data will be the foundation of efficient operations. In order to actually 
extract the value from data and monetize the value one needs an efficient data analytics platform that serves both the end customer (taxi service users) and business users (finance dept. or planning dept. for example), and is agile in its performance. 
With the above being said, the non-functional requirements such as scalibility, reliability, and high-availibility is equally important as the functional ones. 

- The diagram in the attached file represents one such high-level architecture of a data analytics platform that caters to acquire, organize, ingestion, processing, storage and consumption of data in an cost-effective, scalable and secure manner.
- In an organization, there can be various data sources (both internal and external) of importance from which daily (or any other desired frequency) acquiring of data needs to happen. For this an automated collection agent will be very helpful through which one can manage the addition or deletion of various data sources. All this data, raw in nature, needs to be stored
in a cheap storage and there one can leverage cloud native solutions like S3 (AWS) or Blob (Azure). 
This storage can serve as the data lake for the organization, that provides data for downstream stages of the pipeline. 
- Once the data is there in the data lake, the processing stage workflows can be triggered to ETL and load the data in warehouse storage from where other stages like presentation or API portal can consume it further for business insights.
- It is important to note that there can be many solution designs or architectures for this given problem, but all those solutions will be primarily driven by the actual specification and requirements, which at the moment is vague for me, but as a general rule of thumb I suggest the `following pointers to reach or narrow down on decisions`
  - Understand & quantify the amount of data (the platform might store billion rows but it might not need to ingest billion rows everyday, by understanding this one gets the clarity that one may need storage for billion rows but not necessarily the compute capacity to process it daily)
  - Choose open-source over paid license tool wherever possible (helps in to avoid vendor lock-in, or set costs)
  - Choose cloud native design 
  - Understand and choose between frameworks such as data mesh, or data warehouse
  - Run/build proof of concepts before selecting tools 
  - Understand the in-house capability and capacity in terms of technology before solutioning
  - Set clear business goals and use-cases to have sponsorship from management 
  - Understand the integration needs of the data platform such as how it will connect with your existing technology stack
  - and many more
- It is highly recommended to run proof of concepts with a focus group before finalizing the stack




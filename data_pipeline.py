import findspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, sum
from pyspark.sql.types import FloatType
from pyspark.sql.types import TimestampType
import pandas as pd
import numpy as np

filepath= "/Users/meghnagupta/Downloads/Taxi_Trips.csv"

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Chicago Taxi Trip Data Pipelines") \
        .getOrCreate()
spark = SparkSession(scSpark)


############# ---- Data Extraction ---------- ################

def data_extraction(filepath):

    # Read the CSV file into a DataFrame
    df = spark.read.csv(filepath, header=True, inferSchema=True)

    # Return the DataFrame
    return df
   # df.show()

data_extraction(filepath)

########### -------Data Validation and Data Cleaning ---- ##########

def data_cleaning_load_to_csv(df):

    df = spark.read.csv(filepath, header=True, inferSchema=True)
    # Drop any rows with missing values
    df = df.dropna()

    #df = df.select("Trip Start Timestamp", "Trip End Timestamp", "Trip Seconds", "Trip Miles", "Pickup Community Area", "Dropoff Community Area")
    df = df.withColumnRenamed("Trip Start Timestamp", "start_timestamp") \
            .withColumnRenamed("Trip End Timestamp", "end_timestamp") \
            .withColumnRenamed("Trip Seconds", "trip_seconds") \
            .withColumnRenamed("Trip Miles", "trip_miles") \
            .withColumnRenamed("Pickup Community Area", "pickup_community_area") \
            .withColumnRenamed("Dropoff Community Area", "dropoff_community_area")

# Filter out any rows with null values in the relevant columns
    df = df.filter(col("start_timestamp").isNotNull() & col("end_timestamp").isNotNull() & col("trip_seconds").isNotNull() & col("trip_miles").isNotNull())

    # Remove any rows where the pickup datetime is after the dropoff datetime
    df = df[df['start_timestamp'] < df['end_timestamp']]

    # Remove any rows where the trip distance is negative or zero
    df = df[df['trip_miles'] > 0]

    # Remove any rows where the fare is negative or zero
    df = df[df['fare'] > 0]

    # Remove any rows where the trip duration is negative or zero
    df = df[df['trip_seconds'] > 0]

    # Remove any rows where the trip speed is greater than 100 miles per hour
    df = df[df['trip_miles'] / (df['trip_seconds'] / 3600) <= 100]


    df = df.drop(*('Tolls', 'Pickup Census Tract',
                  'Dropoff Census Tract', 'Pickup Centroid Latitude',
                  'Pickup Centroid Longitude', 'Dropoff Centroid Latitude' ,
                  'Dropoff Centroid Longitude'))


    df= df.withColumn('trip_miles',df['trip_miles'].cast(FloatType())) \
        .withColumn('start_timestamp',df['start_timestamp'].cast(TimeStampType())) \
        .withColumn('end_timestamp',df['end_timestamp'].cast(TimeStampType()))\
        .withColumn('Fare',df['Fare'].cast(FloatType())) \
        .withColumn('Trip Total',df['Trip Total'].cast(FloatType()))

    return df

    #df.show()
   # return df_cleaned

df = spark.read.csv(filepath, header=True, inferSchema=True)
df_cleaned = data_cleaning_load_to_csv(df)
print("Cleaned Data")
df_cleaned.show()
df_cleaned.printSchema()

df_cleaned.write.csv('/Users/meghnagupta/Downloads/cleaned_and_validated_data.csv', header=True)


######### ------- Data Transformation ---------- ############

def data_transformation_load_to_csv(df):

    df = spark.read.csv('/Users/meghnagupta/Downloads/cleaned_and_validated_data.csv', header=True, inferSchema=True)
    # Add a new column for the duration of each trip in seconds
    df = df.withColumn("Trip Duration", (df["end_timestamp"].cast("long") - df["start_timestamp"].cast("long")))

    # Add a new column for the average speed of each trip in miles per hour
    df = df.withColumn("Avg Speed", df["trip_miles"] / (df["Trip Duration"] / 3600))
    return df

df_transform = data_transformation_load_to_csv(df)
print("Transformed Data")
df_transform.show()
df_transform.printSchema()

df_cleaned.write.csv('/Users/meghnagupta/Downloads/transformed_data.csv', header=True)

################# Data Ingestion to Snowflake DataWarehouse ##########

def load_data_to_snowflake(df):

    # Define the database connection parameters
    sf_scope = 'snowflake-scope'
    sf_user = 'snowflake-username'
    sf_password = 'snowflake-password'
    sf_account = 'snowflake-account'
    sf_database = 'snowflake-database'
    sf_schema = 'snowflake-schema'
    sf_warehouse = 'snowflake-warehouse'
    sf_url= "snowflakecomputing.com"


    # Connect to the Snowflake database
    sf_options = {
        "sfUrl": sf_url ,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        "sfDatabase": sf_database,
        "sfSchema": sf_schema,
        "sfWarehouse": sf_warehouse
    }

    print ("Reading the transformed CSv file to load into the Snowflake")
    # Load the CSV data into a PySpark DataFrame
    csv_df = spark.read.csv('/Users/meghnagupta/Downloads/transformed_data.csv', header=True, inferSchema=True).withColumn("load_ts", current_timestamp())

    # Write the DataFrame to Snowflake
    csv_df.write \
        .format('snowflake') \
        .option(**sf_options)\
        .option('dbtable', "snowflake_table_taxi_trip") \
        .mode('overwrite') \
        .save()

    print ("Data Ingested into the Snowflake")

load_data_to_snowflake(df)

############# API Response for Queries and Getting Dataset ##############

def get_dataset():

    # Define the database connection parameters
    sf_user = 'snowflake-username'
    sf_password = 'snowflake-password'
    sf_account = 'snowflake-account'
    sf_database = 'snowflake-database'
    sf_schema = 'snowflake-schema'
    sf_warehouse = 'snowflake-warehouse'
    sf_url= "snowflakecomputing.com"


    # Connect to the Snowflake database
    sf_options = {
        "sfUrl": sf_url ,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        "sfDatabase": sf_database,
        "sfSchema": sf_schema,
        "sfWarehouse": sf_warehouse
    }

    temp_df = spark.sql( """
    create or replace temporary view TempTable as select * from snowflake_table_taxi_trip
    """
                        )

    sf_df1 = spark.read.format("snowflake")\
        .options(**sf_options)\
        .option("query", """
            SELECT company, AVG(fare / `Trip Miles`) AS avg_fare_per_mile
            FROM TempTable
            GROUP BY company
            """)\
        .load()
    print("Result of Query : Calculating the average fare per mile for each taxi company ")
    sf_df1.show()

    sf_df2 = spark.read.format("snowflake") \
        .options(**sf_options) \
        .option("query", """
            SELECT `Pickup Community Area`, COUNT(*) AS pickup_count
            FROM TempTable
            GROUP BY `Pickup Community Area`
            ORDER BY pickup_count DESC
            LIMIT 10
            """) \
        .load()
    print("Result of Query : Finding the top 10 most frequent pickup locations ")
    sf_df2.show()

    sf_df3 = spark.read.format("snowflake") \
        .options(**sf_options) \
        .option("query", """
            SELECT company, COUNT(*) AS TotalRides
            FROM TempTable
            group by company
            """) \
        .load()

    print("Result of Query : How many rides for a specific company ")
    sf_df3.show()

    # Convert SQL data to CSV and JSON formats
    query1_csv = sf_df1.write.csv('/Users/meghnagupta/Downloads/query1_csv.csv', header=True)
    query1_json = sf_df1.write.json('/Users/meghnagupta/Downloads/query1_json.json')

    query2_csv = sf_df2.write.csv('/Users/meghnagupta/Downloads/query2_csv.csv', header=True)
    query2_json = sf_df2.write.json('/Users/meghnagupta/Downloads/query2_json.json')

    query3_csv = sf_df3.write.csv('/Users/meghnagupta/Downloads/query3_csv.csv', header=True)
    query3_json = sf_df3.write.json('/Users/meghnagupta/Downloads/query3_json.json')

    response = {
        'query1_csv': query1_csv,
        'query1_json': query1_json,
        'query2_csv': query2_csv,
        'query2_json': query2_json,
        'query3_csv': query3_csv,
        'query3_json': query3_json
    }
    return response

get_dataset()
import findspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, sum

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .master("local[*]")\
        .appName("Chicago Taxi Trip Data Pipeline") \
        .getOrCreate()
spark = SparkSession(scSpark)

filepath= "/Users/meghnagupta/Downloads/Taxi_Trips.csv"

#### ----Extracting and exploring the data ---- ####

#######
def read_csv_to_dataframe(filepath):
        """
        Reads a CSV file into a PySpark DataFrame.

        Returns:
            pyspark.sql.dataframe.DataFrame: A PySpark DataFrame containing the CSV data.
        """
    # Read the CSV file into a DataFrame
        df = spark.read.csv(filepath, header=True, inferSchema=True)

        # Return the DataFrame
        return df
        #df.show()

read_csv_to_dataframe(filepath)

#########
def show_schema_dataframe(filepath):

    # Read the CSV file into a DataFrame
    #read_csv_to_dataframe(filepath)
    df = spark.read.csv(filepath, header=True, inferSchema=True)

    # Show the schema of the DataFrame
    df.printSchema()
    return df
schema_df= show_schema_dataframe(filepath)

############
def show_data_dataframe(df):

    # Read the CSV file into a DataFrame
    #read_csv_to_dataframe(filepath)
    df = spark.read.csv(filepath, header=True, inferSchema=True)

    # Show the contents of the DataFrame
    df.show()

show_df= show_data_dataframe(filepath)

##### --Data Validation and Data Cleaning ---- ####

#########
def find_missing_values(df):
    """
        Find the missing values
    """
    # Select the count of null values per column
    missing_values = df.select([sum(col(column).isNull().cast("int")).alias(column) for column in df.columns])

    # Return the missing values DataFrame
    return missing_values

# Read a CSV file into a DataFrame
df = spark.read.csv(filepath, header=True, inferSchema=True)

# Find the missing values in the DataFrame
missing_values_df = find_missing_values(df)

# Show the missing values DataFrame
missing_values_df.show()

#######

def find_duplicates(df):
# Group the DataFrame by all columns and count the number of occurrences of each row
    counts = df.groupBy(df.columns).count()

# Filter the DataFrame to only keep the rows that have a count greater than 1
    duplicates = counts.filter(col("count") > 1)

# Return the duplicates as a new DataFrame
    return duplicates

# Load a DataFrame from a CSV file
df = spark.read.csv(filepath, header=True, inferSchema=True)

# Find the duplicates in the DataFrame
duplicates = find_duplicates(df)

# Show the duplicates
duplicates.show()

#########
def drop_columns(df, cols_to_drop):
    """
    Drops columns from a PySpark DataFrame and returns the resulting DataFrame.

    """
    # Drop the specified columns
    drop_df = df.drop(*cols_to_drop)

    # Return the resulting DataFrame
    return drop_df

df = spark.read.csv(filepath, header=True, inferSchema=True)

# Drop the specified columns from the DataFrame
cols_to_drop = ['Tolls', 'Pickup Census Tract', 'Dropoff Census Tract', 'Pickup Centroid Latitude','Pickup Centroid Longitude', 'Dropoff Centroid Latitude' , 'Dropoff Centroid Longitude']
drop_df = drop_columns(df, cols_to_drop)
drop_df.show()


def change_datatypes(df, col_datatypes):
    """
    Changes the data types of columns in a PySpark DataFrame and returns the resulting DataFrame.

    """
    # Loop through the columns and data types in the dictionary
    for col_name, col_type in col_datatypes.items():
        # Convert the column to the specified data type
        df = df.withColumn(col_name, col(col_name).cast(col_type))

    # Return the resulting DataFrame
    return df

# Read a CSV file into a DataFrame
df = spark.read.csv(filepath, header=True, inferSchema=True)

# Convert the specified columns to the specified data types
col_datatypes = {'Trip Start Timestamp': 'timestamp', 'Trip End Timestamp': 'timestamp', 'Trip Miles': 'float', 'Fare': 'float' , 'Trip Total': 'float'}
changed_datatypes_df = change_datatypes(df, col_datatypes)
changed_datatypes_df.printSchema()


######### ------- Data Transformation ---------- ############

def add_new_col(df):

    df = spark.read.csv(filepath, header=True, inferSchema=True)
# Add a new column for the duration of each trip in seconds
    new_df = df.withColumn("Trip Duration", (df["Trip End Timestamp"].cast("long") - df["Trip Start Timestamp"].cast("long")))

# Add a new column for the average speed of each trip in miles per hour
    new_df = new_df.withColumn("Avg Speed", new_df["Trip Miles"] / (new_df["Trip Duration"] / 3600))
    return new_df

add_new_df= add_new_col(df)
add_new_df.show()
#add_new_df.show()

########

#####  What is the average fare per trip by day of the week? #####

#df = spark.read.csv(filepath, header=True, inferSchema=True)
#df.createOrReplaceTempView("TempTable")
add_new_df.createOrReplaceTempView("TempTable")
query_df1 = spark.sql("""
    SELECT
        DATE_FORMAT(temptable.`Trip Start Timestamp`, 'EEEE' ) AS day_of_week,
        AVG(temptable.Fare) AS avg_fare
    FROM
        TempTable
    WHERE
        temptable.Fare > 0 AND
        temptable.`Trip Start Timestamp` IS NOT NULL
    GROUP BY
        day_of_week
    ORDER BY
        CASE day_of_week
            WHEN 'Sunday' THEN 1
            WHEN 'Monday' THEN 2
            WHEN 'Tuesday' THEN 3
            WHEN 'Wednesday' THEN 4
            WHEN 'Thursday' THEN 5
            WHEN 'Friday' THEN 6
            WHEN 'Saturday' THEN 7
        END
"""
                      )
print("Result of Query : What is the average fare per trip by day of the week?")
query_df1.show()

###### Calculating the average fare per mile for each taxi company #####
query_df2 = spark.sql("""
SELECT company, AVG(fare / `Trip Miles`) AS avg_fare_per_mile
FROM TempTable
GROUP BY company
"""
                      )
print("Result of Query : Calculating the average fare per mile for each taxi company ")
query_df2.show()

#####Finding the top 10 most frequent pickup locations ###

query_df3 = spark.sql("""
SELECT `Pickup Community Area`, COUNT(*) AS pickup_count
FROM TempTable
GROUP BY `Pickup Community Area`
ORDER BY pickup_count DESC
LIMIT 10
                      """
                      )
print("Result of Query : Finding the top 10 most frequent pickup locations ")
query_df3.show()

####### How many rides for a specific company
query_df4 = spark.sql("""SELECT company, COUNT(*) AS TotalRides
FROM TempTable
group by company
"""
                      )
print("Result of Query : How many rides for a specific company ")
query_df4.show()

###############################################


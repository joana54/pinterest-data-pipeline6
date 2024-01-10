# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import urllib

file_type = "delta"
first_row_is_header = "true"
delimiter = ","
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/user/hive/warehouse/authentication_credentials")


ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

#creates schema for pin data
pin_data_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", StringType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])

#read pin data from Kinesis streams
df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0eeeb621168f-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

#deserializes the json data and loads it into a DataFrame using the schema created above
df_pin = df_pin.selectExpr("CAST(data as STRING)")
df_pin = df_pin.withColumn("data", from_json(col("data"), pin_data_schema))
df_pin = df_pin.selectExpr("data.*")

display(df_pin)


# COMMAND ----------

#create schema for geo data
geo_data_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("country", StringType()),
    StructField("latitude", FloatType()),
    StructField("longitude", FloatType()),
    StructField("timestamp", TimestampType())
])

#read geo data from Kinesis streams
df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0eeeb621168f-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()   

#deserializes the json data and loads it into a DataFrame using the schema created above
df_geo = df_geo.selectExpr("CAST(data as STRING)")
df_geo = df_geo.withColumn("data", from_json(col("data"), geo_data_schema))
df_geo = df_geo.selectExpr("data.*")

display(df_geo)

# COMMAND ----------

#create schema for user data
user_data_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", StringType()),
    StructField("date_joined", TimestampType())
])

#read user data from Kinesis streams
df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0eeeb621168f-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

#deserializes the json data and loads it into a DataFrame using the schema created above
df_user = df_user.selectExpr("CAST(data as STRING)")
df_user = df_user.withColumn("data", from_json(col("data"), user_data_schema))
df_user = df_user.selectExpr("data.*")

display(df_user)

# COMMAND ----------


def pin_data_cleaner(df_pin):
    """Takes in df_pin, cleans it through various methods and returns the cleaned DataFrame"""
    df_pin = df_pin.replace({'User Info Error': None})
    df_pin = df_pin.replace({'Image src error.':None})
    df_pin = df_pin.replace({'Untitled':None})
    df_pin = df_pin.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e':None})
    df_pin = df_pin.replace({'No Title Data Available':None})
    df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
    df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
    df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("integer"))
    df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in", ""))
    df_pin = df_pin.withColumnRenamed("index", "ind")
    df_pin = df_pin.drop("downloaded")
    df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
    df_pin = df_pin.dropDuplicates(['description'])
    return df_pin

clean_pin_data = pin_data_cleaner(df_pin)
display(clean_pin_data)

# COMMAND ----------

def geo_data_cleaner(df_geo):
    """Takes in df_geo, cleans it through various methods and returns the cleaned DataFrame"""
    df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
    df_geo = df_geo.drop("latitude", "longitude")
    df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")
    df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
    df_geo = df_geo.dropDuplicates(['ind'])
    return df_geo
    
clean_geo_data = geo_data_cleaner(df_geo)
display(clean_geo_data)

# COMMAND ----------

def user_data_cleaner(df_user):
    """Takes in df_user, cleans it through various methods and returns the cleaned DataFrame"""
    df_user = df_user.withColumn("user_name", concat("first_name",lit(" "), "last_name"))
    df_user = df_user.dropDuplicates(['ind'])
    df_user = df_user.drop("first_name", "last_name")
    df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
    df_user = df_user.select("ind", "user_name", "age", "date_joined")
    return df_user

clean_user_data = user_data_cleaner(df_user)
display(clean_user_data)

# COMMAND ----------

#deletes the checkpoint folders if they have been made through a previous run of this notebook
dbutils.fs.rm("/tmp/kinesis/0eeeb621168f_pin_table_checkpoints/", True)
dbutils.fs.rm("/tmp/kinesis/0eeeb621168f_geo_table_checkpoints", True)
dbutils.fs.rm("/tmp/kinesis/0eeeb621168f_user_table_checkpoints", True)


#writes the transformed data to Datbricks to three different tables 
clean_pin_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/0eeeb621168f_pin_table_checkpoints/") \
    .table("0eeeb621168f_pin_table") 


clean_geo_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/0eeeb621168f_geo_table_checkpoints/") \
    .table("0eeeb621168f_geo_table") 


clean_user_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/0eeeb621168f_user_table_checkpoints") \
    .table("0eeeb621168f_user_table")

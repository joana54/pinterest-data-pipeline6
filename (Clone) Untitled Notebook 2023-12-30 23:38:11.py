# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Specify file type to be csv
file_type = "delta"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/user/hive/warehouse/authentication_credentials")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0eeeb621168f-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest_data"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/pinterest_data"))

# COMMAND ----------

def load_df(file_name):
 
    file_location = f"/mnt/pinterest_data/topics/{file_name}/partition=0/*.json" 
    file_type = "json"
    infer_schema = "true"
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)
    return df

df_pin = load_df("0eeeb621168f.pin")
df_geo = load_df("0eeeb621168f.geo")
df_user = load_df("0eeeb621168f.user")
display(df_pin)
display(df_geo)
display(df_user)



# COMMAND ----------

df_pin = df_pin.replace({'User Info Error': None})
df_pin = df_pin.replace({' ':None})
df_pin = df_pin.replace({'Image src error.':None})
df_pin = df_pin.replace({'Untitled':None})
df_pin = df_pin.replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e':None})
df_pin = df_pin.replace({'No Title Data Available':None})

df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("integer"))

# df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("integer"))

df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in", ""))

df_pin = df_pin.withColumnRenamed("index", "ind")

df_pin = df_pin.drop("downloaded")

df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video",
                   "image_src", "save_location", "category")


# df_pin = df_pin.dropDuplicates(['description'])

df_pin.printSchema()
display(df_pin)

# COMMAND ----------

# df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))
# df_geo = df_geo.drop("latitude", "longitude")
# df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

df_geo = df_geo.dropDuplicates(['ind'])

display(df_geo)
df_geo.printSchema()

# COMMAND ----------

df_user = df_user.withColumn("user_name", concat("first_name",lit(" "), "last_name"))
df_user = df_user.dropDuplicates(['ind'])
df_user = df_user.drop("first_name", "last_name")

df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

df_user = df_user.select("ind", "user_name", "age", "date_joined")

display(df_user)
df_user.printSchema()

# COMMAND ----------


df_pin.write.format("parquet").mode("overwrite").saveAsTable("pin_table")
df_geo.write.format("parquet").mode("overwrite").saveAsTable("geo_table")

result_df = spark.sql("""
    SELECT DISTINCT 
        geo_table.country, 
        pin_table.category, 
        COUNT(pin_table.category) as category_count
    FROM 
        geo_table
    INNER JOIN 
        pin_table ON geo_table.ind = pin_table.ind
    GROUP BY 
        geo_table.country, 
        pin_table.category 
""")
display(result_df)


# COMMAND ----------

post_count_per_year = spark.sql("""
    SELECT DISTINCT
        YEAR(geo_table.timestamp) AS post_year, 
        pin_table.category,
        COUNT(pin_table.category) as category_count
    FROM
        geo_table
    INNER JOIN
        pin_table ON geo_table.ind = pin_table.ind
     WHERE 
        YEAR(geo_table.timestamp) >= 2018 AND YEAR(geo_table.timestamp) <= 2022
    GROUP BY
        post_year,
        pin_table.category
    ORDER BY post_year ASC
""")
display(post_count_per_year)

# COMMAND ----------

most_followers_per_country = spark.sql("""
                      
    WITH RankedResults AS (
        SELECT DISTINCT
            geo_table.country AS country,
            pin_table.poster_name AS poster_name,
            pin_table.follower_count AS follower_count,
            ROW_NUMBER() OVER (PARTITION BY geo_table.country ORDER BY pin_table.follower_count DESC) AS row_num
        FROM
            geo_table
        INNER JOIN pin_table ON geo_table.ind = pin_table.ind
        )
    SELECT
        country,
        poster_name,
        follower_count
    FROM
        RankedResults
    WHERE
        row_num = 1;
""")
display(most_followers_per_country)

most_followers_per_country.write.format("parquet").mode("overwrite").saveAsTable("followers_table")

user_with_most_followers = spark.sql("""
    SELECT 
        country, follower_count
    FROM 
        followers_table
    ORDER BY 
        follower_count DESC
    LIMIT 1;
""")
display(user_with_most_followers)

# COMMAND ----------

df_user.write.format("parquet").mode("overwrite").saveAsTable("user_table")

most_popular_category = spark.sql("""
    WITH age_group_table AS (
        SELECT
            ind,
            CASE
                WHEN age BETWEEN 18 AND 24 THEN '18-24'
                WHEN age BETWEEN 25 AND 35 THEN '25-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                ELSE '+50'
            END AS age_group
        FROM
            user_table
    )

    SELECT
        age_group_table.age_group,
        pin_table.category,
        COUNT(DISTINCT age_group_table.ind, pin_table.category) AS category_count
    FROM
        age_group_table
    JOIN
        pin_table ON age_group_table.ind = pin_table.ind
    GROUP BY
        age_group_table.age_group, 
        pin_table.category
    ORDER BY
        age_group_table.age_group, 
        category_count DESC;
""")

display(most_popular_category)
     

# COMMAND ----------

median_follower_count = spark.sql("""
 WITH age_group_table AS (
     SELECT
        ind,
        CASE
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 35 THEN '25-35'
            WHEN age BETWEEN 36 AND 50 THEN '36-50'
            ELSE '+50'
        END AS age_group
    FROM 
        user_table
 )
 SELECT
        age_group_table.age_group,
        percentile_approx(pin_table.follower_count, 0.5) AS median_follower_count
    FROM
        age_group_table
    JOIN
        pin_table ON age_group_table.ind = pin_table.ind
    GROUP BY
        age_group_table.age_group
    ORDER BY 
        median_follower_count DESC;
                                       
 """)
display(median_follower_count)

# COMMAND ----------

users_joined = spark.sql("""
    SELECT 
        YEAR(user_table.date_joined) AS post_year,
        COUNT(DISTINCT(ind)) AS number_users_joined
    FROM
        user_table
    GROUP BY
        post_year
    ORDER BY
        post_year DESC;
""")
display(users_joined)

# COMMAND ----------

med_users_2015_2020 = spark.sql("""
   SELECT 
        YEAR (user_table.date_joined) AS post_year,
        percentile_approx(pin_table.follower_count, 0.5) AS median_follower_count
    FROM
        user_table
    JOIN
        pin_table ON user_table.ind = pin_table.ind
    GROUP BY
        post_year
    ORDER BY 
        post_year ASC;                             
""")
display(med_users_2015_2020)

# COMMAND ----------

med_follower_join_age = spark.sql("""
    WITH age_group_table AS(
        SELECT
            ind,
            CASE
                WHEN age BETWEEN 18 AND 24 THEN '18-24'
                WHEN age BETWEEN 25 AND 35 THEN '25-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                ELSE '+50'
            END AS age_group
        FROM 
            user_table
    )
    SELECT 
        age_group_table.age_group AS age_group,
        YEAR(user_table.date_joined) AS post_year,
        percentile_approx(pin_table.follower_count, 0.5) AS median_follower_count
    FROM
        user_table
     JOIN
        pin_table ON user_table.ind = pin_table.ind
     JOIN
        age_group_table ON user_table.ind = age_group_table.ind
    GROUP BY 
        age_group, post_year
    ORDER BY
        age_group DESC,
        post_year DESC;
""")
display(med_follower_join_age)

# COMMAND ----------



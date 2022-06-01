# Databricks notebook source
from pyspark.sql.functions import split, explode, col, udf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# COMMAND ----------

# Setting storage account connection
container_name = "datasource"
storage_account_name = "sghuierdatabricks"
# storage_account_access_key = BLOB_ACCESS_KEY
storage_account_access_key = "f4hfs9ZwA9kfTBnSRsYF+gGJ7V658cOQhcAd830iPfW0VaT5sZr88sOSvqR64fRR+SqCejlhYYy/+ASttrBBTQ=="
spark = SparkSession.builder.appName('temps-demo-2').getOrCreate()
spark.conf.set("fs.azure.account.key." + storage_account_name +".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# Get stroage data location
ratingsLocation = "wasbs://" + container_name +"@" + storage_account_name + ".blob.core.windows.net/ratings.csv"
moviesLocation = "wasbs://" + container_name +"@" + storage_account_name +".blob.core.windows.net/movies.csv"
# Get ratings and movies data
ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)
movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)


# COMMAND ----------
# display(ratings)
ratings.show()

# COMMAND ----------

# display(movies)
movies.show()

# COMMAND ----------

# transform the timestamp data column to a date column
# first we cast the int column to Timestamp
ratingsTemp = ratings \
  .withColumn("ts", ratings.timestamp.cast("Timestamp")) 
  
# then, we cast Timestamp to Date
ratings = ratingsTemp \
  .withColumn("reviewDate", ratingsTemp.ts.cast("Date")) \
  .drop("ts", "timestamp")

# COMMAND ----------

# display(ratings)
ratings.show()

# COMMAND ----------

# use a Spark UDF(user-defined function) to get the year a movie was made, from the title

def titleToYear(title):
  try:
    return int(title[title.rfind("(")+1:title.rfind(")")])
  except:
    return None
# register the above Spark function as UDF
titleToYearUdf = udf(titleToYear, IntegerType())
# add the movieYear column
movies = movies.withColumn("movieYear", titleToYearUdf(movies.title))
# explode the 'movies'.'genres' values into separate rows
movies_denorm = movies.withColumn("genre", explode(split("genres", "\|"))).drop("genres")
# join movies and ratings datasets on movieId
ratings_denorm = ratings.alias('a').join(movies_denorm.alias('b'), 'movieId', 'inner')

# COMMAND ----------

# Show merged data table
# display(ratings_denorm)
ratings_denorm.show()

# COMMAND ----------
ratings_denorm.write.saveAsTable('ratings_denorm', format='parquet', mode='overwrite')

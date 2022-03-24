'''
  CSV to a relational database.
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from google.cloud import bigquery


current_dir = os.path.dirname(__file__)
relative_path = "../../../USER/PycharmProjects/spark/data/authors.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("CSV to DB").master("local").getOrCreate()
spark.conf.set("fs.gs.project.id", "test-bi")
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", "../../../USER/test/gcp_acces")

#  Step 1: Ingestion
#  ---------
#
#  Reads a CSV file with header, called authors.csv, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True, path=absolute_file_path)

# Step 2: Transform
# ---------
# Creates a new column called "name" as the concatenation of lname, a
# virtual column containing ", " and the fname column
df = spark.read.format('bigquery').option('project','test-bi').option('table','test.applications').load()
df.show()
# Step 3: Save

# Saving the data to BigQuery

# Good to stop SparkSession at the end of the application
spark.stop()
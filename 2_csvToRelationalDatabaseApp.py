from pyspark.sql import SparkSession


relative_path = "data/authors.csv"
# Creates a session on a local master
spark = SparkSession.builder.appName("CSV to DB").master("local"). \
    config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2"). \
    getOrCreate()

spark.conf.set("fs.gs.project.id", "p-bi")
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", "/Users/user/Documents/GCP/gcp_acces_zhukov")

#df = spark.read.csv(header=True, inferSchema=True, path=relative_path)

df = spark.read.format('bigquery').option('project','playgendary-bi').option('table','info.applications').load()
df.show()
#df.write.format("bigquery").option("temporaryGcsBucket","some-bucket").option("writeMethod", "direct").save("sandbox.author")


spark.stop()
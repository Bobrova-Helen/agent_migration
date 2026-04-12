import spark

df = spark.read.parquet("s3://raw/")
df.write.partitionBy("date").parquet("s3://processed/")

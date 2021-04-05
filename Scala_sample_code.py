3. Scala/pyspark/spark sample code

Business Objective: Extract data from Hive tables and store in S3 for other systems to access.

Technical Objective: 
Data is processed using Scala.
As per the requirement, processed data is saved in single csv file and also partioned by Year.

Pseudo Code: 

val df = spark.sql("select * from schema.table_name where condition")
val path ="s3://bucket_name/folder/"
df.coalesce(1).write.mode("overwrite").option("header", true).option("quoteMode", "ALL").option("sep", ",")csv(path)
df.write.mode("Overwrite").partitionBy(“Year”).parquet("Folder_path_in_s3")
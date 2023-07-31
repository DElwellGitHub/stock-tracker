from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

BOOTSTRAP_SERVERS='b-3.finalmskcluster.lsk05g.c14.kafka.us-west-2.amazonaws.com:9092,b-2.finalmskcluster.lsk05g.c14.kafka.us-west-2.amazonaws.com:9092,b-1.finalmskcluster.lsk05g.c14.kafka.us-west-2.amazonaws.com:9092'
if __name__ == "__main__":
   spark = SparkSession.builder.getOrCreate()

   schema = spark.read.option('multiline','True').json('s3://wcd-final/schema.json').schema

   df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
       .option("subscribe", "dbserver1.final.stocks") \
       .option("startingOffsets", "latest") \
       .load()

   transform_df = df.select(col("value").cast("string")).alias("value").withColumn("jsonData",from_json(col("value"),schema)).select("jsonData.payload.after.*")

   checkpoint_location = "s3://wcd-final/sparkjob"

   table_name = 'stocks'
   hudi_options = {
       'hoodie.table.name': table_name,
       "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
       'hoodie.datasource.write.recordkey.field': 'record_id',
       'hoodie.datasource.write.partitionpath.field': 'symbol',
       'hoodie.datasource.write.table.name': table_name,
       'hoodie.datasource.write.operation': 'upsert',
       'hoodie.datasource.write.precombine.field': 'event_time',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.partition_fields': 'true',
        'hoodie.datasource.hive_sync.database': "stocks",
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.table' : 'stocks_table',
        'hoodie.datasource.hive_sync.support_timestamp': 'true',
        'hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled': 'true',
        'hoodie.datasource.hive_sync.partition_fields' : 'symbol',
        'hoodie.datasource.hive_sync.partition_extractor_class' : 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
      'hoodie.upsert.shuffle.parallelism': 100,
       'hoodie.insert.shuffle.parallelism': 100
   }

   s3_path = "s3://wcd-final/stocks"

   def write_batch(batch_df, batch_id):
       batch_df.write.format("org.apache.hudi") \
       .options(**hudi_options) \
       .mode("append") \
       .save(s3_path)

   transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("wcd-stocks-streaming").foreachBatch(write_batch).start().awaitTermination()

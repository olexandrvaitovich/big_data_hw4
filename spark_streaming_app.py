import os
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_json, col, struct, to_json, window, udf, from_unixtime, arrays_overlap, array, lit, month, dayofmonth, minute, hour, to_timestamp, collect_set
from schema import data_schema
from states import states
    

def main():

    spark = SparkSession \
          .builder \
          .appName("spark_streaming_app") \
          .getOrCreate()
          
    df = (spark.
            readStream.
            format('kafka').
            option('kafka.bootstrap.servers', '104.248.248.196:9092,134.122.78.61:9092,134.209.225.2:9092').
            option('subscribe', 'stream_data').
            option('startingOffsets', 'earliest').
            load())
    
    df = df.selectExpr('CAST(value as STRING)')
    
    df = df.select(from_json(col('value'), data_schema).alias('df'))
    
    func1 = udf(lambda x: states[x.upper()], StringType())
    
    df = df.filter(col('df.group.group_country')=='us').select('df').withColumn('group_state', func1('df.group.group_state')).\
    withColumn('time', from_unixtime(col('df.event.time')/1000))
    
    df2 = df.select(struct(struct(
                                  col('df.event.event_name'),
                                  col('df.event.event_id'),
                                  col('time'),
                                 ).alias('event'),
                           col('df.group.group_city'),
                           col('df.group.group_country'),
                           col('df.group.group_id'),
                           col('df.group.group_name'),
                           col('group_state')
                           ).alias('value'))

    stream2 = df2.select(to_json('value').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",'104.248.248.196:9092,134.122.78.61:9092,134.209.225.2:9092') \
        .option("topic", "US-meetups") \
        .option("checkpointLocation", "US-metups-checkpoint")

    stream2 = stream2.start()

    df3 = df.withColumn('timestamp', to_timestamp('time')).\
             withWatermark('timestamp', "1 minute").groupBy(window('timestamp', '1 minute')).\
             agg(struct(month('window.end').alias('month'), dayofmonth('window.end').alias('day_of_the_month'), 
              hour('window.end').alias('hour'), minute('window.end').alias('minute'),collect_set('df.group.group_city').alias('cities')).alias('value')).\
             select('value')

    stream3 = df3.select(to_json('value').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",'104.248.248.196:9092,134.122.78.61:9092,134.209.225.2:9092') \
        .option("topic", "US-cities-every-minute") \
        .option("checkpointLocation", "US-cities-every-minute-checkpoint")

    stream3 = stream3.start()

    df4 = df.select(struct(struct(
                                  col('df.event.event_name'),
                                  col('df.event.event_id'),
                                  col('time'),
                                 ).alias('event'),
                           col('df.group.group_topics.topic_name'),
                           col('df.group.group_city'),
                           col('df.group.group_country'),
                           col('df.group.group_id'),
                           col('df.group.group_name'),
                           col('group_state')
                           ).alias('value')).filter(arrays_overlap('value.topic_name', array(lit("Computer programming"),
                                                                                               lit("Big Data"),
                                                                                               lit("Machine Learning"),
                                                                                                 lit("Python"),
                                                                                               lit("Java"),
                                                                                               lit("Web Development"))))
    
    stream4 = df4.select(to_json('value').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",'104.248.248.196:9092,134.122.78.61:9092,134.209.225.2:9092') \
        .option("topic", "Programming-meetups") \
        .option("checkpointLocation", "Programming-metups-checkpoint")

    stream4 = stream4.start()

    stream4.awaitTermination()

    spark.stop()


if __name__=='__main__':
    main()

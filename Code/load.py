import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json


class LoadData:
    def __init__(self):
        load_dotenv()
        self.config = {
            'bootstrap.server': 'localhost:9092',
            'auto.offset.reset': 'earliest',
            'topic': 'Gainers'
        }
        self.spark = (SparkSession.builder
                      .master('local[3]')
                      .appName('RealStockData')
                      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                                                     "org.postgresql:postgresql:42.2.5")
                      .getOrCreate()
                      )
        self.checkpnt = f"/Users/abhishekteli/Documents/Projects/StockDataAnalysis/checkpoint/"
        self.spark.sparkContext.setLogLevel('Error')

    def getSchema(self):
        schema = (StructType([
            StructField("symbol", StringType(), False),
            StructField("type", StringType(), True),
            StructField("name", StringType(), False),
            StructField("price", FloatType(), False),
            StructField("change", FloatType(), True),
            StructField("change_percent", FloatType(), True),
            StructField("previous_close", FloatType(), True),
            StructField("pre_or_post_market", FloatType(), True),
            StructField("pre_or_post_market_change", FloatType(), True),
            StructField("pre_or_pos_market_change_percent", FloatType(), True),
            StructField("last_update_utc", TimestampType(), False),
            StructField("currency", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("exchange_open", TimestampType(), False),
            StructField("exchange_close", TimestampType(), False),
            StructField("timezone", StringType(), True),
            StructField("utc_offset_sec", IntegerType(), True),
            StructField("country_code", StringType(), True),
            StructField("google_mid", StringType(), True)
        ]))

        return schema

    def readData(self):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{self.config['bootstrap.server']}")
            .option("subscribe", f"{self.config['topic']}")
            .option("startingOffsets", "latest")
            .load()
        )

    def getStockData(self, kafka_df):
        schema = self.getSchema()
        json_df = kafka_df.withColumn("json_array", from_json(col("value").cast("string"), ArrayType(schema)))
        exploded_df = json_df.withColumn("data", explode(col("json_array"))).select("data.*")
        return exploded_df

    def saveToDatabase(self, result_df, batch_id):
        url = 'jdbc:postgresql://localhost:5432/RealStockData'
        properties = {
            "user": os.getenv('USERNAME'),
            "password": os.getenv('DATABASE_PASSWORD'),
            "driver": 'org.postgresql.Driver'
        }
        try:
            result_df.write.jdbc(url=url, table='gainers', mode='append', properties=properties)
        except Exception as e:
            print(' ')

    def writeToDatabase(self, stockdata_df):
        sQuery = (
            stockdata_df.writeStream
            .format('console')
            .foreachBatch(self.saveToDatabase)
            .option("checkpointLocation", f"{self.checkpnt}")
            .outputMode('update')
            .start()
        )
        return sQuery

    def process(self):
        kafka_df = self.readData()
        parsed_df = self.getStockData(kafka_df)
        sQuery = self.writeToDatabase(parsed_df)
        return sQuery

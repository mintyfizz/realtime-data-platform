import logging
import os

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructField, StructType


logging.basicConfig(level=logging.INFO)


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
    )
    print("Keyspace created successfully or already exists")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name text,
            last_name text,
            gender text,
            address text,
            postcode text,
            email text,
            username text,
            dob text,
            registered_date text,
            phone text,
            picture text
        )
        """
    )
    print("Table created successfully or already exists")


def create_spark_connection():
    try:
        spark_connection = (
            SparkSession.builder.appName("KafkaSparkDataStreaming")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        spark_connection.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
        return spark_connection
    except Exception as error:
        logging.error("Error creating Spark connection: %s", error)
        return None


def connect_to_kafka(spark_connection):
    try:
        spark_df = (
            spark_connection.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "user_data")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Initial DataFrame created successfully from Kafka stream")
        return spark_df
    except Exception as error:
        logging.error("Error connecting to Kafka: %s", error)
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cassandra_session = cluster.connect()
        logging.info("Cassandra connection created successfully")
        return cassandra_session
    except Exception as error:
        logging.error("Error creating Cassandra connection: %s", error)
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("email", StringType(), True),
            StructField("username", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("registered_date", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("picture", StringType(), True),
        ]
    )

    selection_df = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"))
        .selectExpr("uuid() as id", "data.*")
    )
    return selection_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                .option("keyspace", "spark_streams")\
                .option("table", "created_users")\
                .queryName("user_data_to_cassandra")\
                .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT", "/tmp/spark_checkpoint_user_data"))\
                .start())

                streaming_query.awaitTermination()




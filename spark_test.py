aws_key = 'M5vvTFAJeMlP1Ojx'
aws_secret = 'CDt1TzPxkV2zRXht2pXtfGgKnpatM7Ft'
endpoint = 'localhost:9000'

import pyspark
from delta import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import DataFrame
from common_ops import get_from_s3


def build_spark():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", aws_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \

    return configure_spark_with_delta_pip(builder).getOrCreate()

def db_to_dataframe(spark, db_address):
    return spark.read.format('delta').load(db_address)

def file_to_dataframe(spark, file_path):
    return spark.read.format('json').option('inferSchema', 'true').load(file_path)

def show_table(spark, db_address = './table'):
    df =  spark.read.format('delta').load(db_address)
    df.show()

def make_table(data_address, table_name):
    spark = build_spark()
    df = file_to_dataframe(spark, data_address)
    df.write.format('delta').save(table_name)
    spark.stop()

def describe_table(spark, table_address, history = False):
    query = 'describe ' if not history else 'describe history '
    query += f'delta.`{table_address}`' 
    df = spark.sql(query)
    df.show()

def describe_dataframe(spark, frame):
    frame.describe().show()

def get_updates_cnt_on_merge(spark, data, table_address):
    data.createOrReplaceTempView('data')
    res = spark.sql(f"""
        SELECT COUNT(*)
        FROM data d
        LEFT OUTER JOIN delta.`{table_address}` t
        ON  t.id = d.id
        WHERE t.signature <> d.signature
    """).select('count(1)')
    return res[0]

def get_insert_cnt_on_merge(spark, data, table_address):
    data.createOrReplaceTempView('data')
    res = spark.sql(f"""
        SELECT COUNT(*)
        FROM data d
        LEFT OUTER JOIN delta.`{table_address}` t
        ON  t.id = d.id
        WHERE t.signature IS NULL
    """).select('count(1)')
    return res[0]

def get_delete_cnt_on_merge(spark, data, table_address):
    data.createOrReplaceTempView('data')
    res = spark.sql(f"""
        SELECT COUNT(*)
        FROM data d
        WHERE d.id NOT IN (
            SELECT id
            from delta.`{table_address}`
        )
    """).select('count(1)')
    return res[0]

def merge_data(data_address, table_address):
    spark = build_spark()
    df = file_to_dataframe(spark, data_address)
    df.createOrReplaceTempView('new_data')
    describe_table(spark, table_address)
    describe_dataframe(spark, df)
    
    spark.sql(f"""
        MERGE INTO delta.`{table_address}` AS TARGET
        USING new_data AS SOURCE
        ON (TARGET.id = SOURCE.id)
        WHEN MATCHED AND TARGET.signature <> SOURCE.signature
        THEN UPDATE 
            SET TARGET.price = SOURCE.price
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (date_posted, id, location, price, rent_or_sale, rooms, scraped_at, signature, size)
        VALUES (SOURCE.date_posted, SOURCE.id, SOURCE.location, SOURCE.price, SOURCE.rent_or_sale, SOURCE.rooms, SOURCE.scraped_at, SOURCE.signature, SOURCE.size)
        WHEN NOT MATCHED BY SOURCE 
        THEN DELETE 
    """)

def get_city_info(spark, table_address, city) -> DataFrame:
    df = spark.sql(f"""
    SELECT * 
    FROM delta.`{table_address}`
    WHERE location like '%{city}%'
    """)

    return df

def get_avg_price_by_city_area(spark, table_address, city):
    df = get_city_info(spark, table_address, city)
    return df.groupBy('location') \
        .agg({'price':'avg', "*":'count'}) \
        .select('location', 'avg(price)', 'count(1)') \
        .sort('avg(price)', ascending = False)


def get_versions(spark, table_address):
    df:DataFrame = spark.sql(f'describe history delta.`{table_address}`')
    return [int(elem[0]) for elem in df.select('version').collect()]
    

def get_all_ids_with_change(spark, table_address, version_cnt = -1):
    df_joined: DataFrame = None
    if version_cnt != -1:
        versions = [version for version in get_versions(spark, table_address) if (version < version_cnt)]
    else :
        versions = get_versions(spark, table_address)
    
    for version in versions:
        if df_joined is None:
            df_joined = spark.read.format('delta').option('versionAsOf', version).load(table_address)
        else:
            df_joined = df_joined.unionAll(spark.read.format('delta').option('versionAsOf', version).load(table_address))

    return df_joined.groupBy('id').agg({'*':'count'}).filter('count(1) > 1').select('id', 'count(1)').select('id')


    
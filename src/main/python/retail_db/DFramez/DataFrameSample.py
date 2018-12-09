from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('DataFrames Sample').getOrCreate()

# READING THE CSV FILE TO DATAFRAME AND GIVING SCHEMA FOR THAT DATA
ordersCSV = spark.read.csv('E:\\Spark2usingPython3\\data-master\\retail_db\\orders')\
    .toDF('order_id','order_date','order_customer_id','order_status')

from pyspark.sql.types import IntegerType

# AFTER READING THE DATA TO DATAFRAMES THE COLUMNS WILL BE OF TYPE STRING
# TO CHANGE THE COLUMNS TYPE USE WITHCOLUMN API
orders=ordersCSV.\
    withColumn('order_id',ordersCSV.order_id.cast(IntegerType())).\
    withColumn('order_customer_id',ordersCSV.order_customer_id.cast(IntegerType()))
orders.printSchema()
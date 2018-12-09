from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('DataFrames Sample').getOrCreate()

# READING THE CSV FILE TO DATA FRAME AND GIVING SCHEMA FOR THAT DATA
ordersCSV = spark.read.csv('E:\\Spark2usingPython3\\data-master\\retail_db\\orders')\
    .toDF('order_id','order_date','order_customer_id','order_status')

orderItemsCSV = spark.read.csv('/public/retail_db/order_items').toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',

       'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')
from pyspark.sql.types import IntegerType,FloatType

# AFTER READING THE DATA TO DATA FRAMES THE COLUMNS WILL BE OF TYPE STRING
# TO CHANGE THE COLUMNS TYPE USE WITHCOLUMN API
orders=ordersCSV.\
    withColumn('order_id',ordersCSV.order_id.cast(IntegerType())).\
    withColumn('order_customer_id',ordersCSV.order_customer_id.cast(IntegerType()))
orders.printSchema()

orderItems = orderItemsCSV.withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())).\
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())).\
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())).\
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())).\
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())).\
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))
orderItems.printSchema()

orderItems.show()
from pyspark import SparkConf,SparkContext
import configparser as cp
import sys

props = cp.RawConfigParser()
props.read("..\\..\\resources\\application.properties")
env = sys.argv[1]
config = SparkConf().\
    setAppName("Daily_Revenue").\
    setMaster(props.get(env,'executionMode')).\
    set("conf.ui.port","12902").\
    set('spark.executor.cores','1')
sc = SparkContext(conf = config)
sc.setLogLevel("WARN")
orders = sc.textFile("E:\\Spark2usingPython3\\data-master\\retail_db\\orders\\part-00000.txt")
print(orders.count())

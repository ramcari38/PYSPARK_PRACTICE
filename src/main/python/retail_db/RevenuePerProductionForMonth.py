import sys
import configparser as cp
try:
    from pyspark import SparkContext,SparkConf

    props = cp.RawConfigParser()
    props.read("..\\..\\resources\\application.properties")
    env = sys.argv[5]
    config = SparkConf(). \
        setAppName("Total Revenue Per Day"). \
        setMaster(props.get(env, 'executionMode'))
    sc = SparkContext(conf = config)
    inputPath=sys.argv[1]
    outputPath = sys.argv[2]
    month =sys.argv[3]
    localDir=sys.argv[4]
    Path=sc._gateway.jvm.org.apache.hadoop.fs.Path      #using JVM's APIs
    FileSystem=sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration=sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    fs=FileSystem.get(Configuration())

    #defining accumulator for products counter
    productCounter = sc.accumulator(0);
    if(fs.exists(Path(inputPath)) == False):
        print("Input Path doesnt exist...")
    else:
        if(fs.exists(Path(outputPath))):    #deleting output Dir if already exists
            print("outputPath already exists")
            fs.delete(Path(outputPath),True)
            print("outputPath deleted")
        if(fs.exists(Path("temp"))):
            fs.delete(Path("temp"),True)

        orders = inputPath + "/orders"
        # filtering records with required month and (orderid,1)
        ordersFiltered=sc.textFile(orders).filter(lambda order : month in order.split(",")[1]). \
                                            map(lambda order : (int(order.split(",")[0]),1))
        #print(ordersFiltered.take(5))

        # getting order_item id and subtotal for orderids
        orderItems = inputPath + "/order_items"
        revenueByProductID = sc.textFile(orderItems).\
            map(lambda orderitem: (int(orderitem.split(",")[1]),(int(orderitem.split(",")[2]),float(orderitem.split(",")[4]))))
        #print(revenueByProductID.take(5))

        joinedRDD=revenueByProductID.join(ordersFiltered)\
            .map(lambda l : l[1][0]).reduceByKey(lambda ele,tot : ele + tot)
        print(joinedRDD.take(4))

        #reading products data for mapping productid with product name
        
        def getProductNames(record):
            productCounter.add(1)
            return ((int(record.split(",")[0]),record.split(",")[2]))
        
        products = inputPath + "/products"
        productNamesID = sc.textFile(products).map(getProductNames)
        productNamesID.persist()
        productNamesID.saveAsTextFile("temp")
        #print(productNamesID.take(10))
        print(productCounter.value)

        #joining on productid of productNamesID with productid of joinedRDD
        finalResult = productNamesID.join(joinedRDD).map(lambda fr : str(fr[1][0]) + "\t" + str(fr[1][1]))
        print(finalResult.take(4))
        finalResult.saveAsTextFile(outputPath)
        productNamesID.unpersist()
except ImportError as IE:
    print("cannot import Spark Modules...",IE)

sys.exit(1)
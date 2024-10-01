# Spark version 3.2.0
# Name: pyspark Version: 3.2.0

# SparkContext: Serving as the entry point for Spark applications, SparkContext facilitates interaction between the application and the cluster. By enabling network-based communication between the driver and the workers, SparkContext manages crucial coordination. Notably, as of Spark 3.0, the new entry point is the SparkSession.
# Cluster manager: SparkContext establishes connectivity with a cluster manager, such as YARN, Mesos, or Kubernetes, securing essential resources required for the application’s execution.



# SparkContext: low level
# SparkContext is the entry point for low-level Spark APIs such as RDDs. It’s used to connect to a Spark cluster and create RDDs, accumulators, and broadcast variables on that cluster.

# SparkSession: hight level
# SparkSession is the entry point into all functionality in Spark. It’s the entry point for higher-level Spark APIs, such as DataFrames, SQL, and Structured Streaming, and provides a unified interface for working with various data sources, including Hive, Avro, Parquet, ORC, JSON, and JDBC. It’s the recommended entry point for most Spark applications because it provides a single interface for working with both batch and streaming data.
# DataFrames, SQL, and Structured Streaming, and unified interface for Hive, Avro, Parquet, ORC, JSON, and JDBC.

# Create SparkSession from builder
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkApp").master("local[5]").getOrCreate()
# print(f'Spark Version: {spark.version}')


from pyspark import SparkContext
sc = SparkContext("local", "Parallelize Example")

print("Create a Python list")
data = [1, 2, 3, 4, 5]

print("Create an RDD from the Python list")
rdd = sc.parallelize(data)

print("The type of object created")
print(type(rdd))
print("Return the output into the driver program")
print(rdd.collect())


print("Apply a filter transformation on the RDD")
rdd_filtered = rdd.filter(lambda x: x > 2)

print("The type of object created")
print(type(rdd_filtered))

print("Print the output of the filtered RDD")
print(rdd_filtered.collect())




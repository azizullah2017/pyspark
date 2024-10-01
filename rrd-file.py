from pyspark import SparkContext
sc = SparkContext("local", "Parallelize Example")


print("Create an RDD from external dataset")
rdd_file = sc.textFile("data.txt") 

print("The type of object created")
print(type(rdd_file))

print("Print the values of the RDD")
print(rdd_file.collect())

print("Create an RDD from a text file with 6 partitions")
rdd_file = sc.textFile("data.txt", minPartitions=6)
print(f"Number of partitions with minPartitions option: {rdd_file.getNumPartitions()}")

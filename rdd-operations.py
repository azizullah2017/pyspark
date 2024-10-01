from pyspark import SparkContext
sc = SparkContext("local", "RDD Operations Example")

print("Create a Python list")
data = [1, 2, 3, 4, 5]

print("Create an RDD from the Python list")
rdd = sc.parallelize(data)

print("Apply a map transformation to square each element in the RDD")
rdd2 = rdd.map(lambda x: x ** 2)
print(rdd2.collect())

print("Apply a reduce transformation to sum up all the elements in the rdd2 RDD")
result = rdd2.reduce(lambda x,y : x+y)

print(f'Print final result: {result}')


# Apply the map transformation to increment each element by 2
rdd_map = rdd.map(lambda x: x + 2)

# Collect and print the transformed RDD
print(f'Result after map transformation: {rdd_map.collect()}') 



# Apply the flatMap transformation on the original RDD
rdd_flatmap = rdd.flatMap(lambda x: [x, x*2, x*3])

# Collect and print the transformed RDD
print(f'result after flatmp transformation: {rdd_flatmap.collect()}') 

# Apply the filter transformation to return a list of even elements
rdd_filter = rdd.filter(lambda x: x % 2 == 0)

# Collect and print the transformed RDD
print(f'result after filter transformation: {rdd_filter.collect()}') 



print("Create a Python lists")
data1 = [1, 2, 3]
data2 = [2, 3, 4]

print("Create the first RDD from the first Python list")
rdd1 = sc.parallelize(data1)

print("Create the second RDD frorm the second Python list")
rdd2 = sc.parallelize(data2)

print("Apply the union transformation to create a new RDD that contains all the elements of the two RDDs")
rdd_union = rdd1.union(rdd2)
print(f'result after "Union" transformation: {rdd_union.collect()}') 

print("Apply the intersection transformation to create a new RDD that contains the common elements of the two RDDs")
rdd_intersect = rdd1.intersection(rdd2)
print(f'result after Intersection transformation: {rdd_intersect.collect()}') 


# RDD actions
# collect(): Returns all the elements of the RDD as an array to the driver program. Be careful using this action on large RDDs because it can cause the driver program to run out of memory.
# take(n): Returns the first n elements of the RDD as an array.
# first: Returns the first element of the RDD.
# count: Returns the number of elements in the RDD.


print("Collect and print the transformed RDD")
print(f'result after "colect" action: {rdd.collect()}') 

print("Apply the take action to print the first 3 element of the rdd")
print(f'result after "take" action: {rdd.take(3)}') 


print("Apply the first action to print the first element of the rdd")
print(f'result after "first" action: {rdd.first()}') 

print("Apply the count action")
print(f'result after "count" action: {rdd.count()}') 
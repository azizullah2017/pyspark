# https://medium.com/@akkidx/indexing-into-elasticsearch-using-spark-code-snippets-55eabc753272

# spark-submit --master local[4] --jars /opt/spark/jars//elasticsearch-hadoop-7.17.4.jar --packages org.elasticsearch:elasticsearch-spark-20_2.11:5.3.1 es_spark_test.py

# https://spark-packages.org/package/elastic/elasticsearch-hadoop
## download jar file find complitable list https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop
# https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop/7.17.4
## past the jar file to spark jar folder  /opt/spark/jars//elasticsearch-hadoop-7.17.4.jar

from pyspark import SparkContext
import json
sc = SparkContext()
rdd = sc.parallelize([{'num': i} for i in range(10)])
def remove__id(doc):
    # `_id` field needs to be removed from the document
    # to be indexed, else configure this in `conf` while
    # calling the `saveAsNewAPIHadoopFile` API
    doc.pop('_id', '')
    return doc

new_rdd = rdd.map(remove__id).map(json.dumps).map(lambda x: ('key', x))
new_rdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes" : 'localhost',
        "es.port" : '9200',
        "es.resource" : '%s/%s' % ('index_name', 'doc_type_name'),
        "es.input.json": 'true'
    }
)

# from pyspark import SparkConf
# from pyspark.sql import SQLContext
# from pyspark import SparkContext

# q ="""{
#   "query": {
#     "match_all": {}
#   }  
# }"""

# es_read_conf = {
#     "es.nodes" : "localhost",
#     "es.port" : "9200",
#     "es.resource" : "movies/movie",
#     "es.query" : q
# }
# sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
# es_rdd = sc.newAPIHadoopRDD(
#     inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable", 
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
#     conf=es_read_conf)

# sqlContext.createDataFrame(es_rdd).collect()



# from pyspark import SparkConf
# from pyspark.sql import SQLContext
# from pyspark import SparkContext

# conf = SparkConf().setAppName("ESTest")
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)

# q ="""{
#   "query": {
#     "filtered": {
#       "filter": {
#         "exists": {
#           "field": "label"
#         }
#       },
#       "query": {
#         "match_all": {}
#       }
#     }
#   }
# }"""

# es_read_conf = {
#     "es.nodes" : "localhost",
#     "es.port" : "9200",
#     "es.resource" : "titanic/passenger",
#     "es.query" : q
# }

# es_rdd = sc.newAPIHadoopRDD(
#     inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable", 
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
#     conf=es_read_conf)

# sqlContext.createDataFrame(es_rdd).collect()



# from pyspark import SparkContext, SparkConf

# if __name__ == "__main__":

#     conf = SparkConf().setAppName("ESTest")
#     sc = SparkContext(conf=conf)

#     es_read_conf = {
#         "es.nodes" : "localhost",
#         "es.port" : "9200",
#         "es.resource" : "titanic/passenger"
#     } 

#     es_write_conf = {
#         "es.nodes" : "localhost",
#         "es.port" : "9200",
#         "es.resource" : "titanic/value_counts"
#     } 
    
#     es_rdd = sc.newAPIHadoopRDD(
#         inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
#         keyClass="org.apache.hadoop.io.NullWritable", 
#         valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
#         conf=es_read_conf)

#     doc = es_rdd.first()[1]

#     for field in doc:

#         value_counts = es_rdd.map(lambda item: item[1][field])
#         value_counts = value_counts.map(lambda word: (word, 1))
#         value_counts = value_counts.reduceByKey(lambda a, b: a+b)
#         value_counts = value_counts.filter(lambda item: item[1] > 1)
#         value_counts = value_counts.map(lambda item: ('key', { 
#             'field': field, 
#             'val': item[0], 
#             'count': item[1] 
#         }))

#         value_counts.saveAsNewAPIHadoopFile(
#             path='-', 
#             outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#             keyClass="org.apache.hadoop.io.NullWritable", 
#             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
#             conf=es_write_conf)

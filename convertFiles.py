import sys
import logging
import threading
import ast
from pyspark import SparkSession
import pyspark.sql
from pyspark.sql.types import *

sc = SparkSession.builder \
    .appName("NDOD - Convert files") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


def createSchema (fields, dftext):
    tbfields = [StructField(field_name, StringType(), True) for field_name in fields]
    content = {}

    for l in open("C:\Users\oliveirahy\Documents\NDOD\NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt", "r"):
        value = ""
        for i in list(fields):
            sp = fields[i].split(",")[0]
            ep = fields[i].split(",")[1]
            value += l[int(sp):int(ep)] + ","
        content.append(value)

    for i in list(fields):
        value = []
        sp = fields[i].split(",")[0]
        ep = fields[i].split(",")[1]
        for l in open("hdfs://tmp/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt", "r"):
            value.append(l[int(sp):int(ep)])
        column_name = StructType([StructField(str(i), StringType(), True)])
        rdd = sc.parallelize(value).map(lambda x: Row(x))
        dfcol = sqlContext.createDataFrame(rdd, column_name)
        df = df.withColumn(lit(i), dfcol[str(i)])


"/tmp/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt"
def main ():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    props = {}

    #Read the argument file and segregate the properties.
    with open(sys.argv[1], "r") as f:
        for line in f:
            (key, val) = line.split("=")
            props[key] = str(val)

    # Use the field properties to read the input file
    fields = ast.literal_eval(props["fields"])

    #tbfields = [StructField(field_name, StringType(), True) for field_name in fields]
    schema = StructType([])
    df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
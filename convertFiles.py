import sys
import logging
import threading
import ast
from pyspark import SparkSession
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import Row

sc = SparkSession.builder \
    .appName("NDOD - Convert files") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


def createSchema (fields, dftext):
    content = []

    for l in open("ndod/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt", "r"):
        value = ""
        ct = 0
        for i in list(fields):
            ct += 1
            sp = fields[i].split(",")[0]
            ep = fields[i].split(",")[1]
            value += "'" + l[int(sp):int(ep)] + "'"
            if ct < len(list(fields)):
                value += ","
        content.append(value)


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

    schema = StructType([StructField(field_name, StringType(), True) for field_name in fields])
    #df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

    content = []

    for l in open("ndod/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt", "r"):
        value = ""
        ct = 0
        for i in list(fields):
            ct += 1
            sp = fields[i].split(",")[0]
            ep = fields[i].split(",")[1]
            value += "'" + l[int(sp):int(ep)] + "'"
            if ct < len(list(fields)):
                value += ","
        content.append(value)

    df = sqlContext.createDataFrame(list(map(lambda x: x.split(','), content)), schema=schema)

    df.registerTempTable("tmp_content")
    sc.sql(
        "INSERT OVERWRITE TABLE " + props['dest_table'] + " PARTITION(valueDate = " + str(
            partition) + ") SELECT * FROM tmp_content")

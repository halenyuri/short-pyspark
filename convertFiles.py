import sys
import logging
import threading
import ast
from pyspark import SparkSession
import pyspark.sql

sc = SparkSession.builder \
    .appName("NDOD - Convert files") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


def createSchema (fields, dftext):
    tbfields = [StructField(field_name, StringType(), True) for field_name in fields]
    content = []
    value = ""

    for l in open("/tmp/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt", "r"):
        content.append(value)
        value = ""
        for i in fields:
            sp = fields[i].split(",")[0]
            ep = fields[i].split(",")[1]
            value = l[sp:ep] + ","

"/tmp/NDOD_REST_TIERS_DELTA_30007_RCT_191204-101015.txt.txt"
def main ():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    #Read the argument file and segregate the properties.
    with open(sys.argv[1], "r") as f:
        for line in f:
            (key, val) = line.split("=")
            props[key] = str(val)

    # Use the field properties to read the input file
    fields = ast.literal_eval(props["fields"])

    df = sc.textFile(sys.argv[2])

    tbfields = [StructField(field_name, StringType(), True) for field_name in fields]
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


def main ():
    logger = logging.getLogger(os.path.basename(__file__))
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

    for i in fields:
        sp = fields[i].split(",")[0]
        ep = fields[i].split(",")[1]
        logger.info("Register " + i + " starts in " + sp + " position and finish in " + ep + " position")

    df = sc.read.textfile(sys.argv[2])
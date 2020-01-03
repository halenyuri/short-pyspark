import sys
import logging
import ast
from pyspark.sql import SparkSession
from pyspark.sql.types import *


sc = SparkSession.builder \
    .appName("NDOD - Convert files") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

def main ():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
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
    partition = str(props["partition"]).rstrip()

    schema = StructType([StructField(field_name, StringType(), True) for field_name in fields])
    #df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

    content = []
    #read the input data file and tranform it to fields separated by comma to create the dataframe
    for l in open(sys.argv[2], "r"):
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

    #create the dataframe from content
    df = sc.createDataFrame(list(map(lambda x: x.split(','), content[1:-1])), schema=schema)

    logger.info("###### Processing file " + sys.argv[2] + " with HEADER " + content[0])
    logger.info("###### Will be added " + str(df.count()) + "registers to table " + str(props["htable"]).rstrip() + " partitioned by " + partition)

    df.registerTempTable("tmp_content")
    partition_val = df.select(str(partition)).collect()
    logger.info("###### Partition value -> " + partition_val[0][0])
    #sc.sql("INSERT OVERWRITE TABLE " + props['htable'] + " PARTITION(" + str(partition) + "=" + partition_val[0][0] + ") SELECT * FROM tmp_content")
    df.write.mode("overwrite").insertInto(props["htable"])
    #sc.sql(
    #    "CREATE TABLE " + props['htable'] + "AS SELECT * FROM tmp_content")


main()


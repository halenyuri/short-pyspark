import sys
import datetime
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
from pyspark.sql.types import *

sc = SparkSession.builder \
    .appName("XML Converter to Fermat") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

props = {}
with open(sys.argv[1], "r") as f:
    for line in f:
        (key, val) = line.split(":")
        props[key] = str(val).rstrip()

def flatten_df(nested):
    flat_cols = [c[0] for c in nested.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested.dtypes if c[1][:6] == 'struct']
    flat_df = nested.select(flat_cols + [col(nc + '.' + c).alias(nc + '_' + c) for nc in nested_cols for c in nested.select(nc + '.*').columns])
    return flat_df

schema = StructType([StructField('ValueDate', LongType())])
value_date = sc.read.format('com.databricks.spark.xml') \
    .options(rowTag="root") \
    .load(props['source_path'], schema=schema)

partition = value_date.collect()[0][0]
#partition = datetime.datetime.now().strftime("%Y%m%d")

exposure = sc.read.format('com.databricks.spark.xml') \
    .options(rootTag='ArrayOfExposures') \
    .options(rowTag='Exposure') \
    .load(props['source_path'])
flat_exposure = flatten_df(exposure)

limit = sc.read.format('com.databricks.spark.xml') \
    .options(rootTag='ArrayOfLimits') \
    .options(rowTag='Limit') \
    .load(props['source_path'])
flat_limit = flatten_df(limit)

if props['target_type'] == "JSON":
    flat_exposure.coalesce(1).write.format("json").save(props['dest_path'] + "/exposure_" + str(partition))
    flat_limit.coalesce(1).write.format("json").save(props['dest_path'] + "/limit_" + str(partition))
elif props['target_type'] == "HIVE":
    flat_exposure.registerTempTable("tmp_exposure")
    sc.sql(
        "INSERT OVERWRITE TABLE " + props['dest_table'] + "_exposure PARTITION(valueDate = " + str(partition) + ") SELECT * FROM tmp_exposure")
    flat_limit.registerTempTable("tmp_limit")
    sc.sql(
        "INSERT OVERWRITE TABLE " + props[
            'dest_table'] + "_limit PARTITION(valueDate = " + str(partition) + ") SELECT * FROM tmp_limit")

else:
    print("Missing/Wrong target_type value (Ex.: HIVE or JSON)")

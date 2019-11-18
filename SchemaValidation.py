import sys
import logging
import threading
from pyspark import SparkSession
import pyspark.sql

sc = SparkSession.builder \
    .appName("sqlImport - Diff Schema") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

def getTableSchema (ctx, src_table):
    sch_table = ctx.table(src_table)
    schema = sch_table.printSchema()
    return [i.name for i in schema.fields]


def alterHiveTable (hive_table):
    tbl_source = hive_table

def execValidation (hostname, portnumber, dbtable, user, password, htable):

    logger = logging.getLogger(os.path.basename(__file__))
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    #sc.read.jdbc("CONNECTION_STRING", "TABLE", properties={"user":"repl","password":"password"})

    hive_table = htable
    db_table = dbtable
    sqldf = sc.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:"+user+"/"+password+"@//"+hostname+":"+portnumber+"/SID") \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()

    hivedf = sc.sql("SELECT + FROM " + hive_table + "LIMIT 1")

    hive_schema = getTableSchema(hivedf, hive_table)
    db_schema = getTableSchema(sqldf, db_table)

    logger.info('Comparing columns...')
    if hive_schema < db_schema:
        logger.warn('Table columns DO NOT match!')
        alterHiveTable(hive_table)

    logger.info('Hive schema validation finished!!')

def main():

    logger = logging.getLogger(os.path.basename(__file__))
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    tbl_args = {}

    with open(sys.argv[1], "r") as f:
        for line in f:
            (hostname, portnumber, user, dbtable, user, password, htable) = line.split(":")
            threading.Thread(target=execValidation, args=(hostname, portnumber, user, dbtable, user, password, htable)).start()
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql import Window
from pyspark.sql.functions import last
import sys


def load_trades(spark):
    data = [
        (10, 1546300800000, 37.50, 100.000),
        (10, 1546300801000, 37.51, 100.000),
        (20, 1546300804000, 12.67, 300.000),
        (10, 1546300807000, 37.50, 200.000),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("price", T.DoubleType()),
            T.StructField("quantity", T.DoubleType()),
        ]
    )
    
    return spark.createDataFrame(data, schema)


def load_prices(spark):
    data = [
        (10, 1546300799000, 37.50, 37.51),
        (10, 1546300802000, 37.51, 37.52),
        (10, 1546300806000, 37.50, 37.51),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("bid", T.DoubleType()),
            T.StructField("ask", T.DoubleType()),
        ]
    )
    
    return spark.createDataFrame(data, schema)


def fill(trades, prices):
    """
    Combine the sets of events and fill forward the value columns so that each
    row has the most recent non-null value for the corresponding id. For
    example, given the above input tables the expected output is:
    
    +---+-------------+-----+-----+-----+--------+
    | id|    timestamp|  bid|  ask|price|quantity|
    +---+-------------+-----+-----+-----+--------+
    | 10|1546300799000| 37.5|37.51| null|    null|
    | 10|1546300800000| 37.5|37.51| 37.5|   100.0|
    | 10|1546300801000| 37.5|37.51|37.51|   100.0|
    | 10|1546300802000|37.51|37.52|37.51|   100.0|
    | 20|1546300804000| null| null|12.67|   300.0|
    | 10|1546300806000| 37.5|37.51|37.51|   100.0|
    | 10|1546300807000| 37.5|37.51| 37.5|   200.0|
    +---+-------------+-----+-----+-----+--------+
    
    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and filled.
    """
    res =  trades.select('id','timestamp',lit(None).alias('bid'),lit(None).alias('ask'),'price','quantity').union(prices.select('id','timestamp','bid','ask',lit(None).alias('price'),lit(None).alias('quantity')))
    
    window = Window.partitionBy('id').orderBy('timestamp').rowsBetween(-sys.maxsize, 0)
    filled_bid = last(res['bid'], ignorenulls=True).over(window)
    filled_ask = last(res['ask'], ignorenulls=True).over(window)
    filled_price = last(res['price'], ignorenulls=True).over(window)
    filled_quantity = last(res['quantity'], ignorenulls=True).over(window)    
    
    res = res.withColumn('temp_bid', filled_bid).drop("bid").withColumnRenamed("temp_bid","bid")
    res = res.withColumn('temp_ask', filled_ask).drop("ask").withColumnRenamed("temp_ask","ask")
    res = res.withColumn('temp_price', filled_price).drop("price").withColumnRenamed("temp_price","price")
    res = res.withColumn('temp_quantity', filled_quantity).drop("quantity").withColumnRenamed("temp_quantity","quantity")    
    
    
    res = res.sort("timestamp",ascending=True)
    filled_bid = last(res['bid'], ignorenulls=True).over(window)
    filled_ask = last(res['ask'], ignorenulls=True).over(window)
    filled_price = last(res['price'], ignorenulls=True).over(window)
    filled_quantity = last(res['quantity'], ignorenulls=True).over(window)    
    
    res = res.withColumn('temp_bid', filled_bid).drop("bid").withColumnRenamed("temp_bid","bid")
    res = res.withColumn('temp_ask', filled_ask).drop("ask").withColumnRenamed("temp_ask","ask")
    res = res.withColumn('temp_price', filled_price).drop("price").withColumnRenamed("temp_price","price")
    res = res.withColumn('temp_quantity', filled_quantity).drop("quantity").withColumnRenamed("temp_quantity","quantity")    
    
    res = res.sort("timestamp",ascending=True)
    
    return res
    raise NotImplementedError()


def pivot(trades, prices):
    """
    Pivot and fill the columns on the event id so that each row contains a
    column for each id + column combination where the value is the most recent
    non-null value for that id. For example, given the above input tables the
    expected output is:

    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | id|    timestamp|  bid|  ask|price|quantity|10_bid|10_ask|10_price|10_quantity|20_bid|20_ask|20_price|20_quantity|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | 10|1546300799000| 37.5|37.51| null|    null|  37.5| 37.51|    null|       null|  null|  null|    null|       null|
    | 10|1546300800000| null| null| 37.5|   100.0|  37.5| 37.51|    37.5|      100.0|  null|  null|    null|       null|
    | 10|1546300801000| null| null|37.51|   100.0|  37.5| 37.51|   37.51|      100.0|  null|  null|    null|       null|
    | 10|1546300802000|37.51|37.52| null|    null| 37.51| 37.52|   37.51|      100.0|  null|  null|    null|       null|
    | 20|1546300804000| null| null|12.67|   300.0| 37.51| 37.52|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300806000| 37.5|37.51| null|    null|  37.5| 37.51|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300807000| null| null| 37.5|   200.0|  37.5| 37.51|    37.5|      200.0|  null|  null|   12.67|      300.0|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    
    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and pivoted columns.
    """
    suf = ["bid","ask","price","quantity"]
    i = 0
    base = fill(trades, prices)
    ids = base.select('id').distinct().collect()
    
    while i < int(len(ids)):
        for s in suf:
            colname = str(ids[int(i)][0]) + "_" + s
            base = base.withColumn(colname, F.col(s))
        i += 1
    
    return base
    raise NotImplementedError()


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    
    trades = load_trades(spark)
    trades.show()
    
    prices = load_prices(spark)
    prices.show()
    
    fill(trades, prices).show()
    
    pivot(trades, prices).show()

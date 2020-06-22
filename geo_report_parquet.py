from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

sqlContext = HiveContext(sc)

ttd_report_df = sqlContext.sql("select * from saket.ttd_geo_report_tmp")

ttd_report_df.repartition(2).write.partitionBy('advertiser_id','day_numeric').save('s3://dwh-reports-data/saket/ttd-reports/parquet/geo/', mode='overwrite',compression="snappy")

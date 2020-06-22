drop table if exists saket.ttd_system_report_final;
create external table saket.ttd_system_report_final(
  campaign_group_id string,
  insertion_order_id string,
  timezone string,
  geo_country string,
  browser_name string,
  creative_area string,
  creative_id string,
  device_make string,
  device_model string,
  device_type string,
  operating_system string,
  operating_system_family string,
  cost double,
  clicks bigint,
  impressions bigint,
  conversions bigint
)
partitioned by (
  advertiser_id string,
  day_numeric string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/parquet/system/';

alter table saket.ttd_system_report_final recover partitions;


drop table if exists saket.ttd_geo_report_final;
create external table saket.ttd_geo_report_final(
  campaign_group_id string,
  insertion_order_id string,
  city string,
  region string,
  geo_country string,
  cost double,
  clicks bigint,
  impressions bigint,
  conversions bigint
)
partitioned by (
  advertiser_id string,
  day_numeric string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/parquet/geo/';

alter table saket.ttd_geo_report_final recover partitions;


drop table if exists saket.ttd_sitedomain_report_final;
create external table saket.ttd_sitedomain_report_final(
  campaign_group_id string,
  insertion_order_id string,
  site_domain string,
  site_domain_category string,
  geo_country string,
  clicks bigint,
  impressions bigint,
  conversions bigint
)
partitioned by (
  advertiser_id string,
  day_numeric string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/parquet/site_domain/';

alter table saket.ttd_sitedomain_report_final recover partitions;


drop table if exists saket.ttd_segment_load_final;
create external table saket.ttd_segment_load_final(
  hourofday string,
  weekday string,
  segment_name string,
  total_loads bigint
)
partitioned by (
  advertiser_id string,
  day_numeric string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/parquet/segment_load/';

alter table saket.ttd_segment_load_final recover partitions;

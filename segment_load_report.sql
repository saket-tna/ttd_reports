drop table if exists saket.segment_ttd_conversions;
create external table saket.segment_ttd_conversions (
  logentrytime string,
  conversionid string,
  advertiserid string,
  conversiontype string,
  tdid string,
  ipaddress string,
  referrerurl string,
  monetaryvalue string,
  montaryvaluecurrency string,
  orderid string,
  td1 string,
  td2 string,
  td3 string,
  td4 string,
  td5 string,
  td6 string,
  td7 string,
  td8 string,
  td9 string,
  td10 string,
  processedtime string
)
partitioned by (
  year string,
  month string,
  day string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/thetradedesk-feed/conversions/';

alter table saket.segment_ttd_conversions add if not exists partition (year = '2020', month = '06', day = '14');


drop table if exists saket.ttd_conversions_temp;
create table saket.ttd_conversions_temp
as
select
  logentrytime,
  min(conversionid) as conversionid,
  advertiserid,
  conversiontype,
  tdid,
  ipaddress,
  referrerurl,
  monetaryvalue,
  montaryvaluecurrency,
  orderid,
  td1,
  td2,
  td3,
  td4,
  td5,
  td6,
  td7,
  td8,
  td9,
  td10,
  processedtime
from saket.segment_ttd_conversions
group by
  logentrytime,
  advertiserid,
  conversiontype,
  tdid,
  ipaddress,
  referrerurl,
  monetaryvalue,
  montaryvaluecurrency,
  orderid,
  td1,
  td2,
  td3,
  td4,
  td5,
  td6,
  td7,
  td8,
  td9,
  td10,
  processedtime;


drop table if exists saket.ttd_segment_load_report_tmp;
create table saket.ttd_segment_load_report_tmp (
  day_numeric string,
  hourofday string,
  weekday string,
  advertiser_id string,
  segment_name string,
  total_loads bigint
);


insert overwrite table saket.ttd_segment_load_report_tmp
select
  to_date(logentrytime),
  substr(logentrytime,12,2),
  date_format(logentrytime,'EEEE'),
  advertiserid,
  substr(conversiontype,7),
  count(distinct conversionid)
from saket.ttd_conversions_temp
group by
  to_date(logentrytime),
  substr(logentrytime,12,2),
  date_format(logentrytime,'EEEE'),
  advertiserid,
  substr(conversiontype,7);

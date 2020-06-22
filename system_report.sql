drop table if exists saket.ttd_system_report;
create external table saket.ttd_system_report(
  adgroup_id string,
  campaign_id string,
  `date` string,
  timezone string,
  advertiser_id string,
  country string,
  browser string,
  creative string,
  creative_id string,
  device_make string,
  device_model string,
  device_type string,
  operating_system string,
  operating_system_family string,
  advertiser_cost double,
  clicks bigint,
  impressions bigint,
  conversions bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/raw/system/'
TBLPROPERTIES (
  'skip.header.line.count'='1');


drop table if exists saket.ttd_system_report_tmp;
create table saket.ttd_system_report_tmp(
  campaign_group_id string,
  insertion_order_id string,
  day_numeric string,
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
  conversions bigint,
  advertiser_id string
);

insert overwrite table saket.ttd_system_report_tmp
select
  adgroup_id as campaign_group_id,
  campaign_id as insertion_order_id,
  to_date(`date`) as day_numeric,
  timezone,
  country as geo_country,
  browser as browser_name,
  creative as creative_area,
  creative_id,
  device_make,
  device_model,
  device_type,
  operating_system,
  operating_system_family,
  sum(advertiser_cost) as cost,
  sum(clicks) as clicks,
  sum(impressions) as impressions,
  sum(conversions) as conversions,
  advertiser_id
from saket.ttd_system_report
group by
  adgroup_id,
  campaign_id,
  to_date(`date`),
  timezone,
  country,
  browser,
  creative,
  creative_id,
  device_make,
  device_model,
  device_type,
  operating_system,
  operating_system_family,
  advertiser_id;

drop table if exists saket.ttd_geo_report;
create external table saket.ttd_geo_report(
  campaign_id string,
  adgroup_id string,
  advertiser_id string,
  city string,
  country string,
  region string,
  `date` string,
  metro_code int,
  advertiser_cost double,
  conversions bigint,
  clicks bigint,
  impressions bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dwh-reports-data/saket/ttd-reports/raw/geo/'
TBLPROPERTIES (
  'skip.header.line.count'='1');


drop table if exists saket.ttd_geo_report_tmp;
create table saket.ttd_geo_report_tmp(
  campaign_group_id string,
  insertion_order_id string,
  metro_code int,
  city string,
  region string,
  geo_country string,
  cost double,
  clicks bigint,
  impressions bigint,
  conversions bigint,
  advertiser_id string,
  day_numeric string
);

insert overwrite table saket.ttd_geo_report_tmp
select
  adgroup_id as campaign_group_id,
  campaign_id as insertion_order_id,
  metro_code,
  city,
  region,
  country as geo_country,
  sum(advertiser_cost) as cost,
  sum(clicks) as clicks,
  sum(impressions) as impressions,
  sum(conversions) as conversions,
  advertiser_id,
  to_date(`date`) as day_numeric
from saket.ttd_geo_report
group by
  advertiser_id,
  adgroup_id,
  campaign_id,
  metro_code,
  to_date(`date`),
  city,
  region,
  country;

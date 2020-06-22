drop table if exists saket.ttd_site_domain_report;
create external table saket.ttd_site_domain_report(
  `date` string,
  site string,
  adgroup_id string,
  advertiser_id string,
  campaign_id string,
  category_name string,
  country string,
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
  's3://dwh-reports-data/saket/ttd-reports/raw/site_domain/'
TBLPROPERTIES (
  'skip.header.line.count'='1');


drop table if exists saket.ttd_site_domain_report_tmp;
create table saket.ttd_site_domain_report_tmp(
  campaign_group_id string,
  insertion_order_id string,
  day_numeric string,
  site_domain string,
  site_domain_category string,
  geo_country string,
  clicks bigint,
  impressions bigint,
  conversions bigint,
  advertiser_id string
);

insert overwrite table saket.ttd_site_domain_report_tmp
select
  adgroup_id as campaign_group_id,
  campaign_id as insertion_order_id,
  to_date(`date`) as day_numeric,
  site as site_domain,
  category_name as site_domain_category,
  country as geo_country,
  sum(clicks) as clicks,
  sum(impressions) as impressions,
  sum(conversions) as conversions,
  advertiser_id
from saket.ttd_site_domain_report
group by
  advertiser_id,
  adgroup_id,
  campaign_id,
  to_date(`date`),
  site,
  category_name,
  country;

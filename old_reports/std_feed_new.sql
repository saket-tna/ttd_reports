use saket;

DROP TABLE if exists ttd_impressions;
CREATE EXTERNAL TABLE ttd_impressions(
  LogEntryTime string,
  ImpressionId string,
  PartnerId string,
  AdvertiserId string,
  CampaignId string,
  AdGroupId string,
  PrivateContractID string,
  AudienceID string,
  CreativeId string,
  AdFormat string,
  Frequency Int,
  SupplyVendor string,
  SupplyVendorPublisherID string,
  DealID string,
  Site string,
  ReferrerCategoriesList string,
  FoldPosition Int,
  UserHourOfWeek Int,
  UserAgent string,
  IPAddress string,
  TDID string,
  Country string,
  Region string,
  Metro string,
  City string,
  DeviceType Int,
  OSFamily Int,
  OS Int,
  Browser Int,
  Recency Int,
  LanguageCode string,
  MediaCost Double,
  FeeFeatureCost DOUBLE,
  DataUsageTotalCost DOUBLE,
  TTDCostInUSD DOUBLE,
  PartnerCostInUSD DOUBLE,
  AdvertiserCostInUSD DOUBLE,
  Latitude string,
  Longitude string,
  DeviceID string,
  ZipCode string,
  ProcessedTime string,
  DeviceMake string,
  DeviceModel string,
  RenderingContext string,
  CarrierID string,
  TemperatrueInCelsiusName DOUBLE,
  TemperatureBucketStartInCelsiusName Int,
  TemperatureBucketEndInCelsiusName Int,
  impressionplacementid string)
PARTITIONED BY (year string,month string,day string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/thetradedesk-feed/impressions/';


Drop table if exists ttd_clicks;
CREATE EXTERNAL TABLE ttd_clicks(
  logentrytime string,
  clickid string,
  ipaddress string,
  referrerurl string,
  redirecturl string,
  campaignid string,
  channelid string,
  advertiserid string,
  displayimpressionid string,
  keyword string,
  keywordid string,
  matchtype string,
  distributionnetwork string,
  tdid string,
  rawurl string,
  processedtime string,
  deviceid string)
PARTITIONED BY (YEAR string, MONTH string, DAY string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/thetradedesk-feed/clicks/';


Drop table if exists ttd_conversions;
CREATE EXTERNAL TABLE ttd_conversions(
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
  processedtime string)
PARTITIONED BY (YEAR string,MONTH string,DAY string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://dwh-reports-data/thetradedesk-feed/conversions/';


ALTER TABLE ttd_impressions add if not exists PARTITION(year='2020',month='04',day='25');
ALTER TABLE ttd_clicks add if not exists PARTITION(year='2020',month='04',day='25');
ALTER TABLE ttd_conversions add if not exists PARTITION(year='2020',month='04',day='25');


drop table if exists imp_feed_tmp;
create table imp_feed_tmp
as
select
  LogEntryTime,
  ImpressionId,
  PartnerId,
  AdvertiserId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  cast(MediaCost as double) as MediaCost,
  cast(AdvertiserCostInUSD as double) as AdvertiserCost,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid,
  1 as impressions,
  0 as conversions
from ttd_impressions;

drop table if exists click_feed_tmp;
create table click_feed_tmp
as
select
  displayimpressionid,
  count(displayimpressionid) as clicks
from ttd_clicks
group by
  displayimpressionid;


drop table if exists std_feed_tmp;
create table std_feed_tmp
as
select
  LogEntryTime,
  ImpressionId,
  AdvertiserId,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  MediaCost,
  AdvertiserCost,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid,
  impressions,
  conversions,
  b.clicks
from imp_feed_tmp a
  left join click_feed_tmp b on a.impressionid = b.displayimpressionid;


Drop table if exists ttd_clicks_report_temp;
CREATE TABLE ttd_clicks_report_temp (
  displayimpressionid string,
  conversionid string,
  conversiontype string,
  tdid string,
  conv_time string,
  advertiserid string,
  clicktime string);

Drop table if exists ttd_views_report_temp;
CREATE external TABLE ttd_views_report_temp(
  impressionid string,
  conversionid string,
  conversiontype string,
  tdid string,
  conv_time string,
  advertiserid string,
  imptime string);

INSERT overwrite TABLE ttd_clicks_report_temp
SELECT
  a.displayimpressionid,
  b.conversionid,
  b.conversiontype,
  a.tdid,
  b.logentrytime AS convtime,
  a.advertiserid,
  a.logentrytime AS clicktime
FROM (Select displayimpressionid,tdid,logentrytime,advertiserid from ttd_clicks where tdid != '00000000-0000-0000-0000-000000000000') as a
JOIN (Select conversionid,conversiontype,tdid,logentrytime,advertiserid from ttd_conversions where tdid != '00000000-0000-0000-0000-000000000000') as b
  ON a.advertiserid = b.advertiserid
    AND a.tdid = b.tdid
    AND a.logentrytime <= b.logentrytime
    AND DATEDIFF(b.logentrytime,a.logentrytime) <= 30;



INSERT OVERWRITE TABLE ttd_views_report_temp
SELECT
  a.impressionid,
  b.conversionid,
  b.conversiontype,
  a.tdid,
  b.logentrytime AS convtime,
  a.advertiserid,
  a.logentrytime AS imptime
FROM (Select conversionid,conversiontype,tdid,logentrytime,advertiserid from ttd_conversions where tdid != '00000000-0000-0000-0000-000000000000') as b
JOIN (Select impressionid,tdid,logentrytime,advertiserid from ttd_impressions where tdid != '00000000-0000-0000-0000-000000000000') as a
  ON a.advertiserid = b.advertiserid
    AND a.tdid = b.tdid
    AND a.logentrytime <= b.logentrytime
    AND DATEDIFF(b.logentrytime,a.logentrytime) <= 30;


Drop table if exists ttd_clicks_report_temp_2;
CREATE TABLE ttd_clicks_report_temp_2
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  MAX(clicktime) AS maxevent,
  'pc_conv' AS event
FROM ttd_clicks_report_temp
GROUP BY
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid;

Drop table if exists ttd_clicks_report_temp_3;
CREATE TABLE ttd_clicks_report_temp_3
AS
SELECT
  a.conversionid,
  a.conversiontype,
  a.tdid,
  a.conv_time,
  a.advertiserid,
  a.maxevent,
  a.event,
  b.displayimpressionid
FROM ttd_clicks_report_temp_2 a
LEFT JOIN ttd_clicks_report_temp b
  ON a.conversionid = b.conversionid
  AND a.maxevent = b.clicktime;

Drop table if exists ttd_clicks_report_temp_4;
CREATE TABLE ttd_clicks_report_temp_4
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  maxevent,
  event,
  displayimpressionid,
  ROW_NUMBER() OVER (PARTITION BY conversionid,tdid,conversiontype ORDER BY maxevent DESC) AS clickrank
FROM ttd_clicks_report_temp_3;

Drop table if exists ttd_clicks_report_temp_5;
CREATE TABLE ttd_clicks_report_temp_5
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  maxevent,
  event,
  displayimpressionid AS impressionid,
  REGEXP_REPLACE(TO_DATE(conv_time),'-','') AS dayserial_numeric
FROM ttd_clicks_report_temp_4
WHERE clickrank = 1;

Drop table if exists ttd_views_report_temp_2;
CREATE TABLE ttd_views_report_temp_2
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  MAX(imptime) AS maxevent,
  'pv_conv' AS event
FROM ttd_views_report_temp
GROUP BY
  conversiontype,
  conversionid,
  tdid,
  conv_time,
  advertiserid;

Drop table if exists ttd_views_report_temp_3;
CREATE TABLE ttd_views_report_temp_3
AS
SELECT
  a.conversionid,
  a.conversiontype,
  a.tdid,
  a.conv_time,
  a.advertiserid,
  a.maxevent,
  a.event,
  b.impressionid
FROM ttd_views_report_temp_2 a
LEFT JOIN ttd_views_report_temp b
  ON a.conversionid = b.conversionid
  AND a.maxevent = b.imptime;


Drop table if exists ttd_views_report_temp_4;
CREATE TABLE ttd_views_report_temp_4
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  maxevent,
  event,
  impressionid,
  ROW_NUMBER() OVER (PARTITION BY conversionid,tdid,conversiontype ORDER BY maxevent DESC) AS viewrank
FROM ttd_views_report_temp_3;


Drop table if exists ttd_views_report_temp_5;
CREATE TABLE ttd_views_report_temp_5
AS
SELECT
  conversionid,
  conversiontype,
  tdid,
  conv_time,
  advertiserid,
  maxevent,
  event,
  impressionid,
  REGEXP_REPLACE(TO_DATE(conv_time),'-','') AS dayserial_numeric
FROM ttd_views_report_temp_4
WHERE viewrank = 1;

SET hive.strict.checks.cartesian.product = FALSE;
SET hive.mapred.mode = nonstrict;


Drop table if exists ttd_views_report_temp_6;
CREATE TABLE ttd_views_report_temp_6
AS
SELECT
  a.conversionid,
  a.conversiontype,
  a.tdid,
  a.conv_time,
  a.advertiserid,
  a.maxevent,
  a.event,
  a.impressionid,
  a.dayserial_numeric
FROM ttd_views_report_temp_5 a
WHERE a.conversionid NOT IN (SELECT DISTINCT conversionid FROM ttd_clicks_report_temp_5);


Drop table if exists ttd_conv_report;
CREATE external TABLE ttd_conv_report(
  conversionid string,
  conversiontype string,
  tdid string,
  conv_time string,
  advertiserid string,
  maxevent string,
  event string,
  impressionid string,
  dayserial_numeric string);

INSERT overwrite TABLE ttd_conv_report
SELECT *
FROM ttd_clicks_report_temp_5
UNION ALL
SELECT *
FROM ttd_views_report_temp_6;

drop table if exists ttd_conv_imp_temp_final;
create table ttd_conv_imp_temp_final
as
SELECT
  a.conversionid,
  a.conversiontype,
  a.tdid,
  conv_time,
  a.advertiserid,
  maxevent,
  event,
  a.impressionid,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  Frequency,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  IPAddress,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  Recency,
  LanguageCode,
  MediaCost,
  AdvertiserCostInUSD,
  Latitude,
  Longitude,
  DeviceID,
  ZipCode,
  ProcessedTime,
  DeviceMake,
  DeviceModel,
  RenderingContext,
  CarrierID,
  TemperatrueInCelsiusName,
  TemperatureBucketStartInCelsiusName,
  TemperatureBucketEndInCelsiusName,
  impressionplacementid,
  dayserial_numeric
FROM ttd_conv_report a
  JOIN ttd_impressions b ON a.impressionid = b.impressionid;

insert into std_feed_tmp
select
  conv_time,
  impressionid,
  advertiserid,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  MediaCost,
  AdvertiserCostInUSD,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid,
  0,
  0,
  count(distinct conversionid)
from ttd_conv_imp_temp_final
group by
  conv_time,
  impressionid,
  advertiserid,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  MediaCost,
  AdvertiserCostInUSD,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid;

drop table if exists std_feed_final;
create table std_feed_final
as
select
  LogEntryTime,
  ImpressionId,
  AdvertiserId,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  MediaCost,
  AdvertiserCost,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid,
  sum(impressions) as impressions,
  sum(clicks) as clicks,
  sum(conversions) as conversions
from std_feed_tmp
group by
  LogEntryTime,
  ImpressionId,
  AdvertiserId,
  PartnerId,
  CampaignId,
  AdGroupId,
  PrivateContractID,
  AudienceID,
  CreativeId,
  AdFormat,
  SupplyVendor,
  SupplyVendorPublisherID,
  DealID,
  Site,
  ReferrerCategoriesList,
  FoldPosition,
  UserHourOfWeek,
  UserAgent,
  Country,
  Region,
  Metro,
  City,
  DeviceType,
  OSFamily,
  OS,
  Browser,
  MediaCost,
  AdvertiserCost,
  Latitude,
  Longitude,
  ZipCode,
  DeviceMake,
  DeviceModel,
  CarrierID,
  impressionplacementid;

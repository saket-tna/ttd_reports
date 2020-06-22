use saket;

SET hive.exec.compress.output = TRUE;
SET mapred.output.compression.type = BLOCK;
SET hive.auto.convert.join = TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET mapred.inmem.merge.threshold = 0;
SET mapred.job.reduce.input.buffer.percent = 1;
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;
set hive.execution.engine=tez;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
SET hive.exec.parallel = true;


DROP TABLE if exists attribution_ttd_impressions;

CREATE EXTERNAL TABLE attribution_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';


Drop table if exists attribution_ttd_clicks;
CREATE EXTERNAL TABLE attribution_ttd_clicks (logentrytime string,clickid string,ipaddress string,referrerurl string,redirecturl string,campaignid string,channelid string,advertiserid string,displayimpressionid string,keyword string,keywordid string,matchtype string,distributionnetwork string,tdid string,rawurl string,processedtime string,deviceid string) PARTITIONED BY (YEAR string,MONTH string,DAY string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/clicks/';


Drop table if exists attribution_ttd_conversions;
CREATE EXTERNAL TABLE attribution_ttd_conversions (logentrytime string,conversionid string,advertiserid string,conversiontype string,tdid string,ipaddress string,referrerurl string,monetaryvalue string,montaryvaluecurrency string,orderid string,td1 string,td2 string,td3 string,td4 string,td5 string,td6 string,td7 string,td8 string,td9 string,td10 string,processedtime string) PARTITIONED BY (YEAR string,MONTH string,DAY string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/conversions/';

ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='25');
ALTER TABLE attribution_ttd_conversions add if not exists PARTITION(year='2020',month='04',day='25');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='25');


Drop table if exists saurabh_ttd_clicks_report_temp;
CREATE TABLE saurabh_ttd_clicks_report_temp (displayimpressionid string,conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,clicktime string);

Drop table if exists saurabh_ttd_views_report_temp;
CREATE TABLE saurabh_ttd_views_report_temp (impressionid string,conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,imptime string);

INSERT overwrite TABLE saurabh_ttd_clicks_report_temp
SELECT a.displayimpressionid,
       b.conversionid,
       b.conversiontype,
       a.tdid,
       b.logentrytime AS convtime,
       a.advertiserid,
       a.logentrytime AS clicktime
FROM (Select displayimpressionid,tdid,logentrytime,advertiserid from attribution_ttd_clicks where tdid != '00000000-0000-0000-0000-000000000000') as a
  JOIN (Select conversionid,conversiontype,tdid,logentrytime,advertiserid from attribution_ttd_conversions where tdid != '00000000-0000-0000-0000-000000000000') as b
    ON a.advertiserid = b.advertiserid
   where a.tdid = b.tdid
   AND a.logentrytime <= b.logentrytime
   AND DATEDIFF(b.logentrytime,a.logentrytime) <= 30;



Drop table if exists abhishek_ttd_views_report_withoutcondition1;
Create temporary TABLE abhishek_ttd_views_report_withoutcondition1
as
SELECT a.impressionid,
       b.conversionid,
       b.conversiontype,
       a.tdid,
       b.logentrytime AS convtime,
       a.advertiserid as imp_advertiserid,
     b.advertiserid as conv_advertiserid,
       a.logentrytime AS imptime
FROM    (Select conversionid,conversiontype,tdid,logentrytime,advertiserid from attribution_ttd_conversions where tdid != '00000000-0000-0000-0000-000000000000') as b
  JOIN (Select impressionid,tdid,logentrytime,advertiserid from attribution_ttd_impressions where tdid != '00000000-0000-0000-0000-000000000000') as a
    ON a.tdid = b.tdid
  ;




Drop table if exists abhishek_ttd_views_report_withoutcondition;
Create  temporary  TABLE abhishek_ttd_views_report_withoutcondition
as
SELECT impressionid,
       conversionid,
       conversiontype,
       tdid,
       convtime,
     imp_advertiserid as advertiserid,
     imptime
      FROM  abhishek_ttd_views_report_withoutcondition1
  where imp_advertiserid = conv_advertiserid;


INSERT overwrite TABLE saurabh_ttd_views_report_temp
SELECT impressionid,
       conversionid,
       conversiontype,
       tdid,
       convtime,
       advertiserid,
       imptime
FROM abhishek_ttd_views_report_withoutcondition
where imptime <= convtime
   AND DATEDIFF(convtime,imptime) <= 30;


Drop table if exists saurabh_ttd_clicks_report_temp_2;
CREATE  temporary TABLE saurabh_ttd_clicks_report_temp_2
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       MAX(clicktime) AS maxevent,
       'pc_conv' AS event
FROM saurabh_ttd_clicks_report_temp
GROUP BY conversionid,
         conversiontype,
         tdid,
         conv_time,
         advertiserid;

Drop table if exists saurabh_ttd_clicks_report_temp_3;
CREATE  temporary TABLE saurabh_ttd_clicks_report_temp_3
AS
SELECT a.conversionid,
       a.conversiontype,
       a.tdid,
       a.conv_time,
       a.advertiserid,
       a.maxevent,
       a.event,
       b.displayimpressionid
FROM saurabh_ttd_clicks_report_temp_2 a
  LEFT JOIN saurabh_ttd_clicks_report_temp b
         ON a.conversionid = b.conversionid
        AND a.maxevent = b.clicktime;

Drop table if exists saurabh_ttd_clicks_report_temp_4;
CREATE  temporary TABLE saurabh_ttd_clicks_report_temp_4
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       displayimpressionid,
       ROW_NUMBER() OVER (PARTITION BY conversionid,tdid,conversiontype ORDER BY maxevent DESC) AS clickrank
FROM saurabh_ttd_clicks_report_temp_3;

Drop table if exists saurabh_ttd_clicks_report_temp_5;
CREATE  temporary TABLE saurabh_ttd_clicks_report_temp_5
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       displayimpressionid AS impressionid,
       REGEXP_REPLACE(TO_DATE(conv_time),'-','') AS dayserial_numeric
FROM saurabh_ttd_clicks_report_temp_4
WHERE clickrank = 1;

Drop table if exists saurabh_ttd_views_report_temp_2;
CREATE  temporary TABLE saurabh_ttd_views_report_temp_2
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       MAX(imptime) AS maxevent,
       'pv_conv' AS event
FROM saurabh_ttd_views_report_temp
GROUP BY conversiontype,
         conversionid,
         tdid,
         conv_time,
         advertiserid;

Drop table if exists saurabh_ttd_views_report_temp_3;
CREATE  temporary TABLE saurabh_ttd_views_report_temp_3
AS
SELECT a.conversionid,
       a.conversiontype,
       a.tdid,
       a.conv_time,
       a.advertiserid,
       a.maxevent,
       a.event,
       b.impressionid
FROM saurabh_ttd_views_report_temp_2 a
  LEFT JOIN saurabh_ttd_views_report_temp b
         ON a.conversionid = b.conversionid
        AND a.maxevent = b.imptime;


Drop table if exists saurabh_ttd_views_report_temp_4;
CREATE  temporary TABLE saurabh_ttd_views_report_temp_4
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       impressionid,
       ROW_NUMBER() OVER (PARTITION BY conversionid,tdid,conversiontype ORDER BY maxevent DESC) AS viewrank
FROM saurabh_ttd_views_report_temp_3;



Drop table if exists saurabh_ttd_views_report_temp_5;
CREATE  temporary TABLE saurabh_ttd_views_report_temp_5
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       impressionid,
       REGEXP_REPLACE(TO_DATE(conv_time),'-','') AS dayserial_numeric
FROM saurabh_ttd_views_report_temp_4
WHERE viewrank = 1;

SET hive.strict.checks.cartesian.product = FALSE;

SET hive.mapred.mode = nonstrict;


Drop table if exists saurabh_ttd_views_report_temp_6;
CREATE  temporary TABLE saurabh_ttd_views_report_temp_6
AS
SELECT a.conversionid,
       a.conversiontype,
       a.tdid,
       a.conv_time,
       a.advertiserid,
       a.maxevent,
       a.event,
       a.impressionid,
       a.dayserial_numeric
FROM saurabh_ttd_views_report_temp_5 a
WHERE a.conversionid NOT IN (SELECT DISTINCT conversionid
                           FROM saurabh_ttd_clicks_report_temp_5);



Drop table if exists ttd_conv_report;
CREATE TABLE ttd_conv_report (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,dayserial_numeric string);

INSERT overwrite TABLE ttd_conv_report
SELECT *
FROM saurabh_ttd_clicks_report_temp_5
UNION ALL
SELECT *
FROM saurabh_ttd_views_report_temp_6;

Drop table if exists saurabh_ttd_conv_imp_temp_final;
CREATE TABLE saurabh_ttd_conv_imp_temp_final (conversionid varchar(1024),conversiontype varchar(1024),tdid varchar(1024),conv_time varchar(1024),advertiserid varchar(1024),maxevent varchar(1024),event varchar(1024),impressionid varchar(1024),PartnerId varchar(1024),CampaignId varchar(1024),AdGroupId varchar(1024),PrivateContractID varchar(1024),AudienceID varchar(1024),CreativeId varchar(1024),AdFormat varchar(1024),Frequency varchar(1024),SupplyVendor varchar(1024),SupplyVendorPublisherID varchar(1024),DealID varchar(1024),Site varchar(1024),ReferrerCategoriesList varchar(1024),FoldPosition varchar(1024),UserHourOfWeek varchar(1024),UserAgent varchar(1024),IPAddress varchar(1024),Country varchar(1024),Region varchar(1024),Metro varchar(1024),City varchar(1024),DeviceType varchar(1024),OSFamily varchar(1024),OS varchar(1024),Browser varchar(1024),Recency varchar(1024),LanguageCode varchar(1024),AdvertiserCostInUSD varchar(1024),Latitude varchar(1024),Longitude varchar(1024),DeviceID varchar(1024),ZipCode varchar(1024),ProcessedTime varchar(1024),DeviceMake varchar(1024),DeviceModel varchar(1024),RenderingContext varchar(1024),CarrierID varchar(1024),TemperatrueInCelsiusName varchar(1024),TemperatureBucketStartInCelsiusName varchar(1024),TemperatureBucketEndInCelsiusName varchar(1024),impressionplacementid varchar(1024),dayserial_numeric varchar(1024));

INSERT overwrite TABLE saurabh_ttd_conv_imp_temp_final
SELECT a.conversionid,
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
  JOIN attribution_ttd_impressions b ON a.impressionid = b.impressionid;

Drop table if exists saurabh_ttd_conv_imp_temp_final2;
CREATE  temporary TABLE saurabh_ttd_conv_imp_temp_final2
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       impressionid,
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
       dayserial_numeric,
       ROW_NUMBER() OVER (PARTITION BY conversiontype,tdid,conv_time,advertiserid,maxevent,event,impressionid,dayserial_numeric,PartnerId,CampaignId,AdGroupId,PrivateContractID,AudienceID,CreativeId,AdFormat,Frequency,SupplyVendor,SupplyVendorPublisherID,DealID,Site,ReferrerCategoriesList,FoldPosition,UserHourOfWeek,UserAgent,IPAddress,Country,Region,Metro,City,DeviceType,OSFamily,OS,Browser,Recency,LanguageCode,AdvertiserCostInUSD,Latitude,Longitude,DeviceID,ZipCode,ProcessedTime,DeviceMake,DeviceModel,RenderingContext,CarrierID,TemperatrueInCelsiusName,TemperatureBucketStartInCelsiusName,TemperatureBucketEndInCelsiusName,impressionplacementid,dayserial_numeric ORDER BY conv_time DESC) AS RANK
FROM saurabh_ttd_conv_imp_temp_final;


Drop table if exists saurabh_ttd_conv_imp_temp_final3;
CREATE temporary  TABLE saurabh_ttd_conv_imp_temp_final3
AS
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       impressionid,
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
FROM saurabh_ttd_conv_imp_temp_final2
WHERE RANK = 1;

Drop table if exists saurabh_conv_imp_final_2;
CREATE external TABLE saurabh_conv_imp_final_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string, dayserial_numeric string);

INSERT overwrite table saurabh_conv_imp_final_2
SELECT conversionid,
       conversiontype,
       tdid,
       conv_time,
       advertiserid,
       maxevent,
       event,
       impressionid,
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
FROM saurabh_ttd_conv_imp_temp_final3;

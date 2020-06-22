---Workflow 

--Query 1

use abhishek_goyal;

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

ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='05');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='04');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='03');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='02');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='01');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='31');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='30');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='29');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='28');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='27');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='26');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='25');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='24');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='23');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='22');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='21');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='20');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='19');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='18');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='17');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='16');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='15');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='14');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='13');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='12');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='11');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='10');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='09');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='08');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='07');




ALTER TABLE attribution_ttd_conversions add if not exists PARTITION(year='2020',month='04',day='05');

ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='05');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='04');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='03');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='02');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='01');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='31');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='30');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='29');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='28');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='27');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='26');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='25');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='24');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='23');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='22');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='21');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='20');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='19');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='18');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='17');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='16');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='15');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='14');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='13');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='12');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='11');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='10');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='09');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='08');
ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='2020',month='03',day='07');






Drop table if exists saurabh_ttd_clicks_report_temp;
CREATE external TABLE saurabh_ttd_clicks_report_temp (displayimpressionid string,conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,clicktime string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/debug/convclicks/';

Drop table if exists saurabh_ttd_views_report_temp;
CREATE external TABLE saurabh_ttd_views_report_temp (impressionid string,conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,imptime string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/debug/convviews/';

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
CREATE external TABLE ttd_conv_report (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,dayserial_numeric string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/AbhishekGoyal/ttdreports/debug/dayconv/';

INSERT overwrite TABLE ttd_conv_report
SELECT *
FROM saurabh_ttd_clicks_report_temp_5
UNION ALL
SELECT *
FROM saurabh_ttd_views_report_temp_6;

Drop table if exists saurabh_ttd_conv_imp_temp_final;
CREATE external TABLE saurabh_ttd_conv_imp_temp_final (conversionid varchar(1024),conversiontype varchar(1024),tdid varchar(1024),conv_time varchar(1024),advertiserid varchar(1024),maxevent varchar(1024),event varchar(1024),impressionid varchar(1024),PartnerId varchar(1024),CampaignId varchar(1024),AdGroupId varchar(1024),PrivateContractID varchar(1024),AudienceID varchar(1024),CreativeId varchar(1024),AdFormat varchar(1024),Frequency varchar(1024),SupplyVendor varchar(1024),SupplyVendorPublisherID varchar(1024),DealID varchar(1024),Site varchar(1024),ReferrerCategoriesList varchar(1024),FoldPosition varchar(1024),UserHourOfWeek varchar(1024),UserAgent varchar(1024),IPAddress varchar(1024),Country varchar(1024),Region varchar(1024),Metro varchar(1024),City varchar(1024),DeviceType varchar(1024),OSFamily varchar(1024),OS varchar(1024),Browser varchar(1024),Recency varchar(1024),LanguageCode varchar(1024),AdvertiserCostInUSD varchar(1024),Latitude varchar(1024),Longitude varchar(1024),DeviceID varchar(1024),ZipCode varchar(1024),ProcessedTime varchar(1024),DeviceMake varchar(1024),DeviceModel varchar(1024),RenderingContext varchar(1024),CarrierID varchar(1024),TemperatrueInCelsiusName varchar(1024),TemperatureBucketStartInCelsiusName varchar(1024),TemperatureBucketEndInCelsiusName varchar(1024),impressionplacementid varchar(1024),dayserial_numeric varchar(1024)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/AbhishekGoyal/ttdreports/debug/convimpjoin/';

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
CREATE external TABLE saurabh_conv_imp_final_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string) partitioned BY (dayserial_numeric string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/convreport_deduped/';

INSERT INTO saurabh_conv_imp_final_2 PARTITION (dayserial_numeric)
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


Drop table if exists saurabh_ttd_clicks_report_temp_2;
Drop table if exists saurabh_ttd_clicks_report_temp_3;
Drop table if exists saurabh_ttd_clicks_report_temp_4;
Drop table if exists saurabh_ttd_clicks_report_temp_5;
Drop table if exists saurabh_ttd_views_report_temp_2;

Drop table if exists saurabh_ttd_views_report_temp_3;
Drop table if exists saurabh_ttd_views_report_temp_4;
Drop table if exists saurabh_ttd_views_report_temp_5;
Drop table if exists saurabh_ttd_views_report_temp_6;
Drop table if exists saurabh_ttd_conv_imp_temp_final2;
Drop table if exists saurabh_ttd_conv_imp_temp_final3;


Drop table if exists attribution_ttd_impressions;
Drop table if exists attribution_ttd_clicks;
Drop table if exists attribution_ttd_conversions;
Drop table if exists ttd_conv_report;
Drop table if exists saurabh_conv_imp_final_2;




----Query2


use abhishek_goyal;

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

Drop table if exists impclickjoin_ttd_clicks;
CREATE EXTERNAL TABLE impclickjoin_ttd_clicks (logentrytime string,clickid string,ipaddress string,referrerurl string,redirecturl string,campaignid string,channelid string,advertiserid string,displayimpressionid string,keyword string,keywordid string,matchtype string,distributionnetwork string,tdid string,rawurl string,processedtime string,deviceid string) PARTITIONED BY (YEAR string,MONTH string,DAY string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/clicks/';

ALTER TABLE impclickjoin_ttd_clicks add if not exists PARTITION(year='2020',month='04',day='05');


Drop table if exists impclickjoin_ttd_impressions; 
CREATE EXTERNAL TABLE impclickjoin_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';

ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='05');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='04');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='03');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='02');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='04',day='01');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='31');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='30');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='29');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='28');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='27');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='26');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='25');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='24');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='23');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='22');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='21');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='20');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='19');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='18');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='17');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='16');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='15');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='14');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='13');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='12');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='11');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='10');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='09');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='08');
ALTER TABLE impclickjoin_ttd_impressions add if not exists PARTITION(year='2020',month='03',day='07');


Drop table if exists ttd_temp_impression;
CREATE temporary  TABLE ttd_temp_impression (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string) ;

INSERT INTO ttd_temp_impression
  SELECT logentrytime,
  ImpressionId,
  PartnerId,
  AdvertiserId,
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
  TDID,
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
  impressionplacementid FROM impclickjoin_ttd_impressions;

Drop table if exists abhishek_ttd_temp_click;
CREATE temporary  TABLE abhishek_ttd_temp_click
AS
SELECT logentrytime,
       clickid,
       ipaddress,
       referrerurl,
       redirecturl,
       campaignid,
       channelid,
       advertiserid,
       displayimpressionid,
       keyword,
       keywordid,
       matchtype,
       distributionnetwork,
       tdid,
       rawurl,
       processedtime,
       deviceid
FROM impclickjoin_ttd_clicks;


Drop table if exists abhishek_ttd_imp_click;
CREATE EXTERNAL TABLE IF NOT EXISTS abhishek_ttd_imp_click (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string,cf_logentrytime string,cf_clickid string,cf_ipaddress string,cf_referrerurl string,cf_redirecturl string,cf_campaignid string,cf_channelid string,cf_advertiserid string,cf_displayimpressionid string,cf_keyword string,cf_keywordid string,cf_matchtype string,cf_distributionnetwork string,cf_tdid string,cf_rawurl string,cf_processedtime string,cf_deviceid string) PARTITIONED BY (dayserial_numeric string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/impresionsjoinedclicks/';

use abhishek_goyal;
SET hive.exec.dynamic.partition = TRUE;

INSERT INTO abhishek_ttd_imp_click PARTITION (dayserial_numeric)
SELECT a.*,
       b.logentrytime AS cf_logentrytime,
       b.clickid AS cf_clickid,
       b.ipaddress AS cf_ipaddress,
       b.referrerurl AS cf_referrerurl,
       b.redirecturl AS cf_redirecturl,
       b.campaignid AS cf_campaignid,
       b.channelid AS cf_channelid,
       b.advertiserid AS cf_advertiserid,
       b.displayimpressionid AS cf_displayimpressionid,
       b.keyword AS cf_keyword,
       b.keywordid AS cf_keywordid,
       b.matchtype AS cf_matchtype,
       b.distributionnetwork AS cf_distributionnetwork,
       b.tdid AS cf_tdid,
       b.rawurl AS cf_rawurl,
       b.processedtime AS cf_processedtime,
       b.deviceid AS cf_deviceid,
       SUBSTR(REGEXP_REPLACE(b.logentrytime,"-",""),1,8) AS dayserial_numeric
FROM ttd_temp_impression AS a
  JOIN abhishek_ttd_temp_click AS b
    ON a.impressionid = b.displayimpressionid
    where DATEDIFF(b.logentrytime,a.logentrytime) <= 30;
    
Drop table if exists impclickjoin_ttd_clicks;
Drop table if exists impclickjoin_ttd_impressions;
Drop table if exists ttd_temp_impression;
Drop table if exists abhishek_ttd_temp_click;
Drop table if exists abhishek_ttd_imp_click;




----Query 3


use abhishek_goyal;

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


Drop table if exists inventory_abhishek_ttd_imp_click;
CREATE EXTERNAL TABLE inventory_abhishek_ttd_imp_click (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string,cf_logentrytime string,cf_clickid string,cf_ipaddress string,cf_referrerurl string,cf_redirecturl string,cf_campaignid string,cf_channelid string,cf_advertiserid string,cf_displayimpressionid string,cf_keyword string,cf_keywordid string,cf_matchtype string,cf_distributionnetwork string,cf_tdid string,cf_rawurl string,cf_processedtime string,cf_deviceid string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/impresionsjoinedclicks/';


ALTER TABLE inventory_abhishek_ttd_imp_click ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');




Drop table if exists inventory_saurabh_conv_imp_final_2;
CREATE external TABLE inventory_saurabh_conv_imp_final_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string) partitioned BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/convreport_deduped/';


ALTER TABLE inventory_saurabh_conv_imp_final_2 ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');


Drop table if exists inventory_ttd_impressions;
CREATE EXTERNAL TABLE inventory_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';



ALTER TABLE inventory_ttd_impressions ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');

Drop table if exists abhishek_ttd_temp_inventoryreport_IMPRESSION;
CREATE  temporary TABLE abhishek_ttd_temp_inventoryreport_IMPRESSION
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
       SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_ttd_impressions
GROUP BY TO_DATE(LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
          RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
         SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8);

Drop table if exists abhishek_ttd_inventoryreport_IMPRESSION;
CREATE  temporary TABLE abhishek_ttd_inventoryreport_IMPRESSION
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       '0' AS CONVERSIONTYPE,
       IMPRESSIONS,
       ADVERTISERCOSTINUSD,
       MEDIACOST,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_IMPRESSION;

Drop table if exists abhishek_ttd_temp_inventoryreport_CLICKS;
CREATE  temporary TABLE abhishek_ttd_temp_inventoryreport_CLICKS
AS
SELECT TO_DATE(CF_LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
        RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       COUNT(DISTINCT CF_CLICKID) AS CLICKS,
       SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_abhishek_ttd_imp_click
GROUP BY TO_DATE(CF_LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
          RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
         SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8);


Drop table if exists abhishek_ttd_inventoryreport_CLICKS;
CREATE temporary  TABLE abhishek_ttd_inventoryreport_CLICKS
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
        RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       '0' AS CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_CLICKS;


Drop table if exists abhishek_ttd_temp_inventoryreport_Conversions;
CREATE temporary  TABLE abhishek_ttd_temp_inventoryreport_Conversions
AS
SELECT TO_DATE(CONV_TIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       conversiontype,
       COUNT(DISTINCT conversionid) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event IN ('pv_conv') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event IN ('pc_conv') THEN 1 ELSE 0 END) AS PC_CONV,
       SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_saurabh_conv_imp_final_2
GROUP BY TO_DATE(CONV_TIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
         RenderingContext,
         SupplyVendor,
         SupplyVendorPublisherId,
         ReferrerCategoriesList,
         Site,
         FoldPosition,
         AdFormat,
         ImpressionPlacementId,
         conversiontype,
         SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8);


Drop table if exists abhishek_ttd_inventoryreport_Conversions;
CREATE  temporary TABLE abhishek_ttd_inventoryreport_Conversions
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
       TOTALCONVERSIONS,
       PV_CONV,
       PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_Conversions;

Drop table if exists ttd_inventoryperformace_report;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_inventoryperformace_report (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,PrivateContractID string,RenderingContext string,SupplyVendor string,SupplyVendorPublisherId string,ReferrerCategoriesList string,Site string,FoldPosition string,AdFormat string,ImpressionPlacementId string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/inventoryperformance/';


INSERT INTO ttd_inventoryperformace_report PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
       SUM(report.IMPRESSIONS) AS IMPRESSIONS,
       SUM(report.ADVERTISERCOSTINUSD) AS ADVERTISERCOSTINUSD,
       SUM(report.MEDIACOST) AS MEDIACOST,
       SUM(report.TOTALCONVERSIONS) AS TOTALCONVERSIONS,
       SUM(report.PV_CONV) AS PV_CONV,
       SUM(report.PC_CONV) AS PC_CONV,
       SUM(report.CLICKS) AS CLICKS,
       report.DAYSERIAL_NUMERIC
FROM (SELECT *
      FROM abhishek_ttd_inventoryreport_IMPRESSION
      UNION ALL
      SELECT *
      FROM abhishek_ttd_inventoryreport_CLICKS
      UNION ALL
      SELECT *
      FROM abhishek_ttd_inventoryreport_Conversions) report
GROUP BY report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
         report.DAYSERIAL_NUMERIC;
     
Drop table if exists ttd_inventoryperformance_final_retain;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_inventoryperformance_final_retain (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,PrivateContractID string,RenderingContext string,SupplyVendor string,SupplyVendorPublisherId string,ReferrerCategoriesList string,Site string,FoldPosition string,AdFormat string,ImpressionPlacementId string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string,ADVERTISERNAME string,CAMPAIGNNAME string,ADGROUPNAME string,PRIVATECONTRACTNAME string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/lookedupinventoryperformance/';


Drop table if exists ttd_advertiser_lu;
CREATE external TABLE ttd_advertiser_lu (ADVERTISERID string,ADVERTISERNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');

Drop table if exists ttd_campaign_lu;
CREATE external TABLE if not exists ttd_campaign_lu (CAMPAIGNID string,CAMPAIGNNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Campaign/';

ALTER TABLE ttd_campaign_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


Drop table if exists ttd_adgroup_lu;
CREATE external TABLE if not exists ttd_adgroup_lu (ADGROUPID string,ADGROUPNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/AdGroup/';

ALTER TABLE ttd_adgroup_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


Drop table if exists ttd_privateContract_lu;
CREATE external TABLE if not exists ttd_privateContract_lu (PRIVATECONTRACTID string,PRIVATECONTRACTNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Contract/';

ALTER TABLE ttd_privateContract_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


INSERT INTO ttd_inventoryperformance_final_retain PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
       report.IMPRESSIONS,
       report.ADVERTISERCOSTINUSD,
       report.MEDIACOST,
       report.TOTALCONVERSIONS,
       report.PV_CONV,
       report.PC_CONV,
       report.CLICKS,
       b.ADVERTISERNAME,
       c.CAMPAIGNNAME,
       d.ADGROUPNAME,
       e.PRIVATECONTRACTNAME,
       report.DAYSERIAL_NUMERIC
FROM ttd_inventoryperformace_report AS report
  LEFT JOIN ttd_advertiser_lu AS b ON report.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON report.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON report.ADGROUPID = d.ADGROUPID
  LEFT JOIN ttd_privateContract_lu AS e ON report.PrivateContractID = e.PrivateContractID;   



Drop table if exists abhishek_ttd_temp_inventoryreport_IMPRESSION;
Drop table if exists abhishek_ttd_inventoryreport_IMPRESSION;
Drop table if exists abhishek_ttd_temp_inventoryreport_CLICKS;
Drop table if exists abhishek_ttd_inventoryreport_CLICKS;
Drop table if exists abhishek_ttd_temp_inventoryreport_Conversions;
Drop table if exists abhishek_ttd_inventoryreport_Conversions;
Drop table if exists inventory_saurabh_conv_imp_final_2;
Drop table if exists ttd_inventoryperformace_report;
Drop table if exists inventory_ttd_impressions;
Drop table if exists inventory_abhishek_ttd_imp_click;




----Query 4

use abhishek_goyal;

SET hive.exec.compress.output = TRUE;

SET mapred.output.compression.type = BLOCK;

SET hive.auto.convert.join = TRUE;

SET hive.vectorized.execution.enabled = TRUE;

SET mapred.inmem.merge.threshold = 0;

SET mapred.job.reduce.input.buffer.percent = 1;

SET hive.exec.dynamic.partition = TRUE;

SET hive.exec.dynamic.partition.mode = nonstrict;

SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;

Drop table if exists geo_abhishek_ttd_imp_click;
CREATE EXTERNAL TABLE geo_abhishek_ttd_imp_click (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string,cf_logentrytime string,cf_clickid string,cf_ipaddress string,cf_referrerurl string,cf_redirecturl string,cf_campaignid string,cf_channelid string,cf_advertiserid string,cf_displayimpressionid string,cf_keyword string,cf_keywordid string,cf_matchtype string,cf_distributionnetwork string,cf_tdid string,cf_rawurl string,cf_processedtime string,cf_deviceid string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/impresionsjoinedclicks/';

ALTER TABLE geo_abhishek_ttd_imp_click ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');

Drop table if exists geo_saurabh_conv_imp_final_2;
CREATE external TABLE geo_saurabh_conv_imp_final_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string) partitioned BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/convreport_deduped/';



ALTER TABLE geo_saurabh_conv_imp_final_2 ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');

Drop table if exists geo_ttd_impressions;
CREATE EXTERNAL TABLE geo_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';

ALTER TABLE geo_ttd_impressions ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');





Drop table if exists abhishek_ttd_temp_zipreport_IMPRESSION;
CREATE temporary  TABLE abhishek_ttd_temp_zipreport_IMPRESSION
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
       SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM geo_ttd_impressions
GROUP BY TO_DATE(LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
         SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8);

Drop table if exists abhishek_ttd_zipreport_IMPRESSION;
CREATE  temporary TABLE abhishek_ttd_zipreport_IMPRESSION
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       '0' AS CONVERSIONTYPE,
       IMPRESSIONS,
       ADVERTISERCOSTINUSD,
       MEDIACOST,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_IMPRESSION;


Drop table if exists abhishek_ttd_temp_zipreport_CLICKS;
CREATE  temporary TABLE abhishek_ttd_temp_zipreport_CLICKS
AS
SELECT TO_DATE(CF_LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       COUNT(DISTINCT CF_CLICKID) AS CLICKS,
       SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM geo_abhishek_ttd_imp_click
GROUP BY TO_DATE(CF_LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
         SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8);


Drop table if exists abhishek_ttd_zipreport_CLICKS;
CREATE  temporary TABLE abhishek_ttd_zipreport_CLICKS
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       '0' AS CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_CLICKS;



Drop table if exists abhishek_ttd_temp_zipreport_Conversions;
CREATE  temporary TABLE abhishek_ttd_temp_zipreport_Conversions
AS
SELECT TO_DATE(CONV_TIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       conversiontype,
       COUNT(DISTINCT conversionid) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event IN ('pv_conv') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event IN ('pc_conv') THEN 1 ELSE 0 END) AS PC_CONV,
       SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM geo_saurabh_conv_imp_final_2
GROUP BY TO_DATE(CONV_TIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
         conversiontype,
         SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8);


Drop table if exists abhishek_ttd_zipreport_Conversions;
CREATE  temporary TABLE abhishek_ttd_zipreport_Conversions
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
       TOTALCONVERSIONS,
       PV_CONV,
       PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_Conversions;


Drop table if exists ttd_geoperformace_report;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_geoperformace_report (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,COUNTRY string,REGION string,METRO string,CITY string,ZIPCODE string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/geoperformance/';

INSERT INTO ttd_geoperformace_report PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.COUNTRY,
       report.REGION,
       report.METRO,
       report.CITY,
       report.ZIPCODE,
       report.CONVERSIONTYPE,
       SUM(report.IMPRESSIONS) AS IMPRESSIONS,
       SUM(report.ADVERTISERCOSTINUSD) AS ADVERTISERCOSTINUSD,
       SUM(report.MEDIACOST) AS MEDIACOST,
       SUM(report.TOTALCONVERSIONS) AS TOTALCONVERSIONS,
       SUM(report.PV_CONV) AS PV_CONV,
       SUM(report.PC_CONV) AS PC_CONV,
       SUM(report.CLICKS) AS CLICKS,
       report.DAYSERIAL_NUMERIC
FROM (SELECT *
      FROM abhishek_ttd_zipreport_IMPRESSION
      UNION ALL
      SELECT *
      FROM abhishek_ttd_zipreport_CLICKS
      UNION ALL
      SELECT *
      FROM abhishek_ttd_zipreport_Conversions) report
GROUP BY report.DAY,
         report.ADVERTISERID,
         report.CAMPAIGNID,
         report.ADGROUPID,
         report.COUNTRY,
         report.REGION,
         report.METRO,
         report.CITY,
         report.ZIPCODE,
         report.CONVERSIONTYPE,
         report.DAYSERIAL_NUMERIC;
     
     
Drop table if exists ttd_geoperformance_final_retain;    
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_geoperformance_final_retain (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,COUNTRY string,REGION string,METRO string,CITY string,ZIPCODE string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string,ADVERTISERNAME string,CAMPAIGNNAME string,ADGROUPNAME string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/lookedupgeoperformance/';

Drop table if exists ttd_advertiser_lu;
CREATE external TABLE ttd_advertiser_lu (ADVERTISERID string,ADVERTISERNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');

Drop table if exists ttd_campaign_lu;
CREATE external TABLE if not exists ttd_campaign_lu (CAMPAIGNID string,CAMPAIGNNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Campaign/';

ALTER TABLE ttd_campaign_lu ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');


Drop table if exists ttd_adgroup_lu;
CREATE external TABLE if not exists ttd_adgroup_lu (ADGROUPID string,ADGROUPNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/AdGroup/';

ALTER TABLE ttd_adgroup_lu ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');


INSERT INTO ttd_geoperformance_final_retain PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.COUNTRY,
       report.REGION,
       report.METRO,
       report.CITY,
       report.ZIPCODE,
       report.CONVERSIONTYPE,
       report.IMPRESSIONS,
       report.ADVERTISERCOSTINUSD,
       report.MEDIACOST,
       report.TOTALCONVERSIONS,
       report.PV_CONV,
       report.PC_CONV,
       report.CLICKS,
       b.ADVERTISERNAME,
       c.CAMPAIGNNAME,
       d.ADGROUPNAME,
       report.DAYSERIAL_NUMERIC
FROM ttd_geoperformace_report AS report
  LEFT JOIN ttd_advertiser_lu AS b ON report.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON report.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON report.ADGROUPID = d.ADGROUPID;
     
     
use abhishek_goyal; 
Drop table if exists abhishek_ttd_temp_zipreport_IMPRESSION;
Drop table if exists abhishek_ttd_zipreport_IMPRESSION;
Drop table if exists abhishek_ttd_temp_zipreport_CLICKS;
Drop table if exists abhishek_ttd_zipreport_CLICKS;
Drop table if exists abhishek_ttd_temp_zipreport_Conversions;
Drop table if exists abhishek_ttd_zipreport_Conversions;
Drop table if exists geo_saurabh_conv_imp_final_2;
Drop table if exists ttd_geoperformace_report;
Drop table if exists geo_ttd_impressions;
Drop table if exists geo_abhishek_ttd_imp_click;



----Query 5


use abhishek_goyal;

SET hive.exec.compress.output = TRUE;
SET mapred.output.compression.type = BLOCK;
SET hive.auto.convert.join = TRUE;
SET hive.vectorized.execution.enabled = TRUE;
SET mapred.inmem.merge.threshold = 0;
SET mapred.job.reduce.input.buffer.percent = 1;
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapred.output.compression.codec = org.apache.hadoop.io.compress.GzipCodec;




-------------------
Drop table if exists geo_abhishek_ttd_imp_contract_click;
CREATE EXTERNAL TABLE geo_abhishek_ttd_imp_contract_click (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string,cf_logentrytime string,cf_clickid string,cf_ipaddress string,cf_referrerurl string,cf_redirecturl string,cf_campaignid string,cf_channelid string,cf_advertiserid string,cf_displayimpressionid string,cf_keyword string,cf_keywordid string,cf_matchtype string,cf_distributionnetwork string,cf_tdid string,cf_rawurl string,cf_processedtime string,cf_deviceid string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/impresionsjoinedclicks/';

ALTER TABLE geo_abhishek_ttd_imp_contract_click ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');


-----------------


Drop table if exists geo_saurabh_conv_imp_final_contract_2;
CREATE external TABLE geo_saurabh_conv_imp_final_contract_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string) partitioned BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/convreport_deduped/';

ALTER TABLE geo_saurabh_conv_imp_final_contract_2 ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');


-----------------



Drop table if exists geo_ttd_impressions_contract;
CREATE EXTERNAL TABLE geo_ttd_impressions_contract (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';

ALTER TABLE geo_ttd_impressions_contract ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');


-------------------------------


Drop table if exists abhishek_ttd_temp_zipreport_IMPRESSION_contract;
CREATE  temporary TABLE abhishek_ttd_temp_zipreport_IMPRESSION_contract
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
       SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
     SUM(PartnerCostInUSD*1.0000) AS PartnerCostInUSD,
       SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM geo_ttd_impressions_contract
GROUP BY TO_DATE(LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
     PrivateContractID,
         SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8);
     
     
----------------------     

Drop table if exists abhishek_ttd_zipreport_IMPRESSION_contract;
CREATE  temporary TABLE abhishek_ttd_zipreport_IMPRESSION_contract
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       '0' AS CONVERSIONTYPE,
       IMPRESSIONS,
       ADVERTISERCOSTINUSD,
       MEDIACOST,
     PartnerCostInUSD,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_IMPRESSION_contract;

--------------------


Drop table if exists abhishek_ttd_temp_zipreport_CLICKS_contract;
CREATE  temporary TABLE abhishek_ttd_temp_zipreport_CLICKS_contract
AS
SELECT TO_DATE(CF_LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       COUNT(DISTINCT CF_CLICKID) AS CLICKS,
       SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
     
FROM geo_abhishek_ttd_imp_contract_click
GROUP BY TO_DATE(CF_LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
     PrivateContractID,
         SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8);




------------------------


Drop table if exists abhishek_ttd_zipreport_CLICKS_contract;
CREATE  temporary TABLE abhishek_ttd_zipreport_CLICKS_contract
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       '0' AS CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
     0 as PartnerCostInUSD,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_CLICKS_contract;




-----------------------

Drop table if exists abhishek_ttd_temp_zipreport_Conversions_contract;
CREATE temporary  TABLE abhishek_ttd_temp_zipreport_Conversions_contract
AS
SELECT TO_DATE(CONV_TIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       conversiontype,
       COUNT(DISTINCT conversionid) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event IN ('pv_conv') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event IN ('pc_conv') THEN 1 ELSE 0 END) AS PC_CONV,
       SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM geo_saurabh_conv_imp_final_contract_2
GROUP BY TO_DATE(CONV_TIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         COUNTRY,
         REGION,
         METRO,
         CITY,
         ZIPCODE,
     PrivateContractID,
         conversiontype,
         SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8);


--------------------------



Drop table if exists abhishek_ttd_zipreport_Conversions_contract;
CREATE  temporary TABLE abhishek_ttd_zipreport_Conversions_contract
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
     PrivateContractID,
       CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
      0 as PartnerCostInUSD,
       TOTALCONVERSIONS,
       PV_CONV,
       PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_zipreport_Conversions_contract;



--------------------------



Drop table if exists ttd_geoperformace_report_contract;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_geoperformace_report_contract (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,COUNTRY string,REGION string,METRO string,CITY string,ZIPCODE string,PrivateContractID string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,PartnerCostInUSD string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/geoperformancecontract/';

INSERT INTO ttd_geoperformace_report_contract PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.COUNTRY,
       report.REGION,
       report.METRO,
       report.CITY,
       report.ZIPCODE,
     report.PrivateContractID,
       report.CONVERSIONTYPE,
       SUM(report.IMPRESSIONS) AS IMPRESSIONS,
       SUM(report.ADVERTISERCOSTINUSD) AS ADVERTISERCOSTINUSD,
       SUM(report.MEDIACOST) AS MEDIACOST,
     sum(report.PartnerCostInUSD) as PartnerCostInUSD,
       SUM(report.TOTALCONVERSIONS) AS TOTALCONVERSIONS,
       SUM(report.PV_CONV) AS PV_CONV,
       SUM(report.PC_CONV) AS PC_CONV,
       SUM(report.CLICKS) AS CLICKS,
       report.DAYSERIAL_NUMERIC
FROM (SELECT *
      FROM abhishek_ttd_zipreport_IMPRESSION_contract
      UNION ALL
      SELECT *
      FROM abhishek_ttd_zipreport_CLICKS_contract
      UNION ALL
      SELECT *
      FROM abhishek_ttd_zipreport_Conversions_contract) report
GROUP BY report.DAY,
         report.ADVERTISERID,
         report.CAMPAIGNID,
         report.ADGROUPID,
         report.COUNTRY,
         report.REGION,
         report.METRO,
         report.CITY,
         report.ZIPCODE,
     report.PrivateContractID,
         report.CONVERSIONTYPE,
         report.DAYSERIAL_NUMERIC;
     

----------------------------

     
Drop table if exists ttd_geoperformance_final_contract_retain;     
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_geoperformance_final_contract_retain (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,COUNTRY string,REGION string,METRO string,CITY string,ZIPCODE string,PrivateContractId string, CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,PartnerCostInUSD string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string,ADVERTISERNAME string,CAMPAIGNNAME string,ADGROUPNAME string,privatecontractname string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/lookedupgeoperformancecontract/';


----------------------------


Drop table if exists ttd_advertiser_lu_contract;
CREATE external TABLE ttd_advertiser_lu_contract (ADVERTISERID string,ADVERTISERNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu_contract ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


------------------



Drop table if exists ttd_campaign_lu_contract;
CREATE external TABLE if not exists ttd_campaign_lu_contract (CAMPAIGNID string,CAMPAIGNNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Campaign/';

ALTER TABLE ttd_campaign_lu_contract ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');



-----------------------



Drop table if exists ttd_contract_lu_contract;
CREATE external TABLE if not exists ttd_contract_lu_contract (PRIVATECONTRACTID string,PRIVATECONTRACTNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Contract/';

ALTER TABLE ttd_contract_lu_contract ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');



---------------------

Drop table if exists ttd_adgroup_lu_contract;
CREATE external TABLE if not exists ttd_adgroup_lu_contract (ADGROUPID string,ADGROUPNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/AdGroup/';

ALTER TABLE ttd_adgroup_lu_contract ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');



----------------------




INSERT INTO ttd_geoperformance_final_contract_retain PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.COUNTRY,
       report.REGION,
       report.METRO,
       report.CITY,
       report.ZIPCODE,
    report.PrivateContractID,
       report.CONVERSIONTYPE,
       report.IMPRESSIONS,
       report.ADVERTISERCOSTINUSD,
       report.MEDIACOST,
     report.PartnerCostInUSD,
       report.TOTALCONVERSIONS,
       report.PV_CONV,
       report.PC_CONV,
       report.CLICKS,
       b.ADVERTISERNAME,
       c.CAMPAIGNNAME,
       d.ADGROUPNAME,
      e.PrivateContractName,
       report.DAYSERIAL_NUMERIC
FROM ttd_geoperformace_report_contract AS report
  LEFT JOIN ttd_advertiser_lu_contract AS b ON report.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu_contract AS c ON report.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu_contract AS d ON report.ADGROUPID = d.ADGROUPID
  LEFT JOIN ttd_contract_lu_contract AS e ON report.PrivateContractID = e.PrivateContractID;   
  
  
  Drop table if exists ttd_geoperformance_contract_report;
  CREATE EXTERNAL TABLE `ttd_geoperformance_contract_report`(
  `day` date, 
  `advertiser_id` string, 
  `campaign_id` string, 
  `adgroup_id` string, 
  `country` string, 
  `region` string, 
  `metro` string, 
  `city` string, 
    `zipcode` string, 
  PrivateContractID string,
  `conversion_type` string, 
  `impressions` int, 
  `advertiser_cost_in_usd` double, 
  `media_cost` double, 
  PartnerCostInUSD double,
  `total_conversions` int, 
  `pv_conv` int, 
  `pc_conv` int, 
  `clicks` int, 
  `advertiser_name` string, 
  `campaign_name` string, 
  `adgroup_name` string,
  PrivateContractName string)
PARTITIONED BY ( 
  `dayserial_numeric` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://analyst-adhoc/ttdreports/lookedupgeoperformancecontract'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551358');

Alter table ttd_geoperformance_contract_report add if not exists PARTITION (dayserial_numeric=20200405);



------------------------------

     
use abhishek_goyal; 
Drop table if exists abhishek_ttd_temp_zipreport_IMPRESSION_contract;
Drop table if exists abhishek_ttd_zipreport_IMPRESSION_contract;
Drop table if exists abhishek_ttd_temp_zipreport_CLICKS_contract;
Drop table if exists abhishek_ttd_zipreport_CLICKS_contract;
Drop table if exists abhishek_ttd_temp_zipreport_Conversions_contract;
Drop table if exists abhishek_ttd_zipreport_Conversions_contract;
Drop table if exists geo_saurabh_conv_imp_final_contract_2;
Drop table if exists ttd_geoperformace_report_contract;
Drop table if exists geo_ttd_impressions_contract;
Drop table if exists geo_abhishek_ttd_imp_contract_click;



----Query 6

use abhishek_goyal;
-------final inventory report---- this has partner cost and private contract id------------------

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


Drop table if exists inventory_abhishek_ttd_imp_click;
CREATE EXTERNAL TABLE inventory_abhishek_ttd_imp_click (logentrytime string,impressionid string,partnerid string,advertiserid string,campaignid string,adgroupid string,privatecontractid string,audienceid string,creativeid string,adformat string,frequency string,supplyvendor string,supplyvendorpublisherid string,dealid string,site string,referrercategorieslist string,foldposition string,userhourofweek string,useragent string,ipaddress string,tdid string,country string,region string,metro string,city string,devicetype string,osfamily string,os string,browser string,recency string,languagecode string,latitude string,longitude string,deviceid string,zipcode string,processedtime string,devicemake string,devicemodel string,renderingcontext string,carrierid string,temperatrueincelsiusname string,temperaturebucketstartincelsiusname string,temperaturebucketendincelsiusname string,impressionplacementid string,cf_logentrytime string,cf_clickid string,cf_ipaddress string,cf_referrerurl string,cf_redirecturl string,cf_campaignid string,cf_channelid string,cf_advertiserid string,cf_displayimpressionid string,cf_keyword string,cf_keywordid string,cf_matchtype string,cf_distributionnetwork string,cf_tdid string,cf_rawurl string,cf_processedtime string,cf_deviceid string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/impresionsjoinedclicks/';


ALTER TABLE inventory_abhishek_ttd_imp_click ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');




Drop table if exists inventory_saurabh_conv_imp_final_2;
CREATE external TABLE inventory_saurabh_conv_imp_final_2 (conversionid string,conversiontype string,tdid string,conv_time string,advertiserid string,maxevent string,event string,impressionid string,PartnerId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency string,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition string,UserHourOfWeek string,UserAgent string,IPAddress string,Country string,Region string,Metro string,City string,DeviceType string,OSFamily string,OS string,Browser string,Recency string,LanguageCode string,AdvertiserCostInUSD string,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName string,TemperatureBucketStartInCelsiusName string,TemperatureBucketEndInCelsiusName string,impressionplacementid string) partitioned BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/convreport_deduped/';


ALTER TABLE inventory_saurabh_conv_imp_final_2 ADD IF NOT EXISTS PARTITION (DAYSERIAL_NUMERIC='20200405');


Drop table if exists inventory_ttd_impressions;
CREATE EXTERNAL TABLE inventory_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';



ALTER TABLE inventory_ttd_impressions ADD IF NOT EXISTS PARTITION (year='2020',month='04',day='05');

Drop table if exists abhishek_ttd_temp_inventoryreport_IMPRESSION;
CREATE temporary  TABLE abhishek_ttd_temp_inventoryreport_IMPRESSION
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
       SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUM(PartnerCostInUSD*1.000000) AS PartnerCostInUSD,
       SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_ttd_impressions
GROUP BY TO_DATE(LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
          RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
         SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8);

Drop table if exists abhishek_ttd_inventoryreport_IMPRESSION;
CREATE  temporary TABLE abhishek_ttd_inventoryreport_IMPRESSION
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       '0' AS CONVERSIONTYPE,
       IMPRESSIONS,
       ADVERTISERCOSTINUSD,
       MEDIACOST,
     PartnerCostInUSD,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_IMPRESSION;

Drop table if exists abhishek_ttd_temp_inventoryreport_CLICKS;
CREATE  temporary TABLE abhishek_ttd_temp_inventoryreport_CLICKS
AS
SELECT TO_DATE(CF_LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
        RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       COUNT(DISTINCT CF_CLICKID) AS CLICKS,
       SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_abhishek_ttd_imp_click
GROUP BY TO_DATE(CF_LOGENTRYTIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
          RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
         SUBSTR(REGEXP_REPLACE(CF_LOGENTRYTIME,"-",""),1,8);


Drop table if exists abhishek_ttd_inventoryreport_CLICKS;
CREATE temporary  TABLE abhishek_ttd_inventoryreport_CLICKS
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
        RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       '0' AS CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
     0 as PartnerCostInUSD,
       0 AS TOTALCONVERSIONS,
       0 AS PV_CONV,
       0 AS PC_CONV,
       CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_CLICKS;


Drop table if exists abhishek_ttd_temp_inventoryreport_Conversions;
CREATE temporary  TABLE abhishek_ttd_temp_inventoryreport_Conversions
AS
SELECT TO_DATE(CONV_TIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       conversiontype,
       COUNT(DISTINCT conversionid) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event IN ('pv_conv') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event IN ('pc_conv') THEN 1 ELSE 0 END) AS PC_CONV,
       SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM inventory_saurabh_conv_imp_final_2
GROUP BY TO_DATE(CONV_TIME),
         ADVERTISERID,
         CAMPAIGNID,
         ADGROUPID,
         PrivateContractID,
         RenderingContext,
         SupplyVendor,
         SupplyVendorPublisherId,
         ReferrerCategoriesList,
         Site,
         FoldPosition,
         AdFormat,
         ImpressionPlacementId,
         conversiontype,
         SUBSTR(REGEXP_REPLACE(CONV_TIME,"-",""),1,8);


Drop table if exists abhishek_ttd_inventoryreport_Conversions;
CREATE temporary  TABLE abhishek_ttd_inventoryreport_Conversions
AS
SELECT DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
       PrivateContractID,
       RenderingContext,
       SupplyVendor,
       SupplyVendorPublisherId,
       ReferrerCategoriesList,
       Site,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       CONVERSIONTYPE,
       0 AS IMPRESSIONS,
       0 AS ADVERTISERCOSTINUSD,
       0 AS MEDIACOST,
     0 as  PartnerCostInUSD,
       TOTALCONVERSIONS,
       PV_CONV,
       PC_CONV,
       0 AS CLICKS,
       DAYSERIAL_NUMERIC
FROM abhishek_ttd_temp_inventoryreport_Conversions;

Drop table if exists ttd_inventoryperformace_report;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_inventoryperformace_report (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,PrivateContractID string,RenderingContext string,SupplyVendor string,SupplyVendorPublisherId string,ReferrerCategoriesList string,Site string,FoldPosition string,AdFormat string,ImpressionPlacementId string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string,PartnerCostInUSD string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/finalinventoryperformance/';


INSERT INTO ttd_inventoryperformace_report PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
       SUM(report.IMPRESSIONS) AS IMPRESSIONS,
       SUM(report.ADVERTISERCOSTINUSD) AS ADVERTISERCOSTINUSD,
       SUM(report.MEDIACOST) AS MEDIACOST,
      sum(report.PartnerCostInUSD) as  PartnerCostInUSD,
       SUM(report.TOTALCONVERSIONS) AS TOTALCONVERSIONS,
       SUM(report.PV_CONV) AS PV_CONV,
       SUM(report.PC_CONV) AS PC_CONV,
       SUM(report.CLICKS) AS CLICKS,
       report.DAYSERIAL_NUMERIC
FROM (SELECT *
      FROM abhishek_ttd_inventoryreport_IMPRESSION
      UNION ALL
      SELECT *
      FROM abhishek_ttd_inventoryreport_CLICKS
      UNION ALL
      SELECT *
      FROM abhishek_ttd_inventoryreport_Conversions) report
GROUP BY report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
         report.DAYSERIAL_NUMERIC;
     
Drop table if exists ttd_inventoryperformance_final_retain;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_inventoryperformance_final_retain (DAY string,ADVERTISERID string,CAMPAIGNID string,ADGROUPID string,PrivateContractID string,RenderingContext string,SupplyVendor string,SupplyVendorPublisherId string,ReferrerCategoriesList string,Site string,FoldPosition string,AdFormat string,ImpressionPlacementId string,CONVERSIONTYPE string,IMPRESSIONS string,ADVERTISERCOSTINUSD STRING,MEDIACOST string, PartnerCostInUSD string,TOTALCONVERSIONS string,PV_CONV STRING,PC_CONV string,CLICKS string,ADVERTISERNAME string,CAMPAIGNNAME string,ADGROUPNAME string,PRIVATECONTRACTNAME string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/finalnventoryperformancelu/';


Drop table if exists ttd_advertiser_lu;
CREATE external TABLE ttd_advertiser_lu (ADVERTISERID string,ADVERTISERNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');

Drop table if exists ttd_campaign_lu;
CREATE external TABLE if not exists ttd_campaign_lu (CAMPAIGNID string,CAMPAIGNNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Campaign/';

ALTER TABLE ttd_campaign_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


Drop table if exists ttd_adgroup_lu;
CREATE external TABLE if not exists ttd_adgroup_lu (ADGROUPID string,ADGROUPNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/AdGroup/';

ALTER TABLE ttd_adgroup_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


Drop table if exists ttd_privateContract_lu;
CREATE external TABLE if not exists ttd_privateContract_lu (PRIVATECONTRACTID string,PRIVATECONTRACTNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Contract/';

ALTER TABLE ttd_privateContract_lu ADD IF NOT EXISTS PARTITION(year='2020',month='04',day='05');


INSERT INTO ttd_inventoryperformance_final_retain PARTITION (DAYSERIAL_NUMERIC)
SELECT report.DAY,
       report.ADVERTISERID,
       report.CAMPAIGNID,
       report.ADGROUPID,
       report.PrivateContractID,
       report.RenderingContext,
       report.SupplyVendor,
       report.SupplyVendorPublisherId,
       report.ReferrerCategoriesList,
       report.Site,
       report.FoldPosition,
       report.AdFormat,
       report.ImpressionPlacementId,
       report.CONVERSIONTYPE,
       report.IMPRESSIONS,
       report.ADVERTISERCOSTINUSD,
       report.MEDIACOST,
     report.PartnerCostInUSD,
       report.TOTALCONVERSIONS,
       report.PV_CONV,
       report.PC_CONV,
       report.CLICKS,
       b.ADVERTISERNAME,
       c.CAMPAIGNNAME,
       d.ADGROUPNAME,
       e.PRIVATECONTRACTNAME,
       report.DAYSERIAL_NUMERIC
FROM ttd_inventoryperformace_report AS report
  LEFT JOIN ttd_advertiser_lu AS b ON report.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON report.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON report.ADGROUPID = d.ADGROUPID
  LEFT JOIN ttd_privateContract_lu AS e ON report.PrivateContractID = e.PrivateContractID;

  
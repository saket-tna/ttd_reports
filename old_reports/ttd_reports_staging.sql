use staging_usba_ttdreports;

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


-----1.1 load impressions for 30 days-----
DROP TABLE if exists attribution_ttd_impressions;
CREATE EXTERNAL TABLE attribution_ttd_impressions (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';


ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEARX$',month='$MONTHX$',day='$DAYX$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR$',month='$MONTH$',day='$DAY$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR1$',month='$MONTH1$',day='$DAY1$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR2$',month='$MONTH2$',day='$DAY2$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR3$',month='$MONTH3$',day='$DAY3$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR4$',month='$MONTH4$',day='$DAY4$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR5$',month='$MONTH5$',day='$DAY5$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR6$',month='$MONTH6$',day='$DAY6$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR7$',month='$MONTH7$',day='$DAY7$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR8$',month='$MONTH8$',day='$DAY8$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR9$',month='$MONTH9$',day='$DAY9$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR10$',month='$MONTH10$',day='$DAY10$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR11$',month='$MONTH11$',day='$DAY11$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR12$',month='$MONTH12$',day='$DAY12$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR13$',month='$MONTH13$',day='$DAY13$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR14$',month='$MONTH14$',day='$DAY14$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR15$',month='$MONTH15$',day='$DAY15$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR16$',month='$MONTH16$',day='$DAY16$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR17$',month='$MONTH17$',day='$DAY17$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR18$',month='$MONTH18$',day='$DAY18$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR19$',month='$MONTH19$',day='$DAY19$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR20$',month='$MONTH20$',day='$DAY20$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR21$',month='$MONTH21$',day='$DAY21$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR22$',month='$MONTH22$',day='$DAY22$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR23$',month='$MONTH23$',day='$DAY23$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR24$',month='$MONTH24$',day='$DAY24$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR25$',month='$MONTH25$',day='$DAY25$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR26$',month='$MONTH26$',day='$DAY26$');
ALTER TABLE attribution_ttd_impressions add if not exists PARTITION(year='$YEAR27$',month='$MONTH27$',day='$DAY27$');





------1.2 load conversions 1 day-------
Drop table if exists attribution_ttd_conversions;
CREATE EXTERNAL TABLE attribution_ttd_conversions (logentrytime string,conversionid string,advertiserid string,conversiontype string,tdid string,ipaddress string,referrerurl string,monetaryvalue string,montaryvaluecurrency string,orderid string,td1 string,td2 string,td3 string,td4 string,td5 string,td6 string,td7 string,td8 string,td9 string,td10 string,processedtime string) PARTITIONED BY (YEAR string,MONTH string,DAY string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/conversions/';

ALTER TABLE attribution_ttd_conversions add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');


-------1.3 load attributed conversions 1 day-------
Drop table if exists attribution_ttd_miq_conversions;
CREATE EXTERNAL TABLE `attribution_ttd_miq_conversions` (`event_type` string,`event_cross_device_attribution_model` string,`event_time_utc` string, `conversion_id` string, `impressionid` string, `event_attribution_method` string, `conversion_monetary_value` string) PARTITIONED BY (YEAR string,MONTH string,DAY string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION 's3://thetradedesk-feed/attribution/' TBLPROPERTIES ('skip.header.line.count' = '1');


Alter table attribution_ttd_miq_conversions add if not exists PARTITION(year='$YEARZ$',month='$MONTHZ$',day='$DAYZ$');



--------1.5 load clicks for 1 day--------
Drop table if exists attribution_ttd_clicks;
CREATE EXTERNAL TABLE attribution_ttd_clicks (logentrytime string,clickid string,ipaddress string,referrerurl string,redirecturl string,campaignid string,channelid string,advertiserid string,displayimpressionid string,keyword string,keywordid string,matchtype string,distributionnetwork string,tdid string,rawurl string,processedtime string,deviceid string) PARTITIONED BY (YEAR string,MONTH string,DAY string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/clicks/';

ALTER TABLE attribution_ttd_clicks add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');




-----1.6 load impressions for 1 day-----
DROP TABLE if exists attribution_ttd_impressions_1day;
CREATE EXTERNAL TABLE attribution_ttd_impressions_1day (LogEntryTime string,ImpressionId string,PartnerId string,AdvertiserId string,CampaignId string,AdGroupId string,PrivateContractID string,AudienceID string,CreativeId string,AdFormat string,Frequency Int,SupplyVendor string,SupplyVendorPublisherID string,DealID string,Site string,ReferrerCategoriesList string,FoldPosition Int,UserHourOfWeek Int,UserAgent string,IPAddress string,TDID string,Country string,Region string,Metro string,City string,DeviceType Int,OSFamily Int,OS Int,Browser Int,Recency Int,LanguageCode string,MediaCost Double,FeeFeatureCost DOUBLE,DataUsageTotalCost DOUBLE,TTDCostInUSD DOUBLE,PartnerCostInUSD DOUBLE,AdvertiserCostInUSD DOUBLE,Latitude string,Longitude string,DeviceID string,ZipCode string,ProcessedTime string,DeviceMake string,DeviceModel string,RenderingContext string,CarrierID string,TemperatrueInCelsiusName DOUBLE,TemperatureBucketStartInCelsiusName Int,TemperatureBucketEndInCelsiusName Int,impressionplacementid string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/impressions/';


ALTER TABLE attribution_ttd_impressions_1day add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');



----------------3.2 LOADING LOOKUPS---------------


------------------advertiser lookup-------------
Drop table if exists ttd_advertiser_lu;
CREATE external TABLE ttd_advertiser_lu (ADVERTISERID string,ADVERTISERNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu ADD IF NOT EXISTS PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

---------------------campaign lookup---------------
Drop table if exists ttd_campaign_lu;
CREATE external TABLE if not exists ttd_campaign_lu (CAMPAIGNID string,CAMPAIGNNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Campaign/';

ALTER TABLE ttd_campaign_lu ADD IF NOT EXISTS PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

---------------------adgroup lookup---------------
Drop table if exists ttd_adgroup_lu;
CREATE external TABLE if not exists ttd_adgroup_lu (ADGROUPID string,ADGROUPNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/AdGroup/';

ALTER TABLE ttd_adgroup_lu ADD IF NOT EXISTS PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

---------------------private contract id  lookup---------------
Drop table if exists ttd_privateContract_lu;
CREATE external TABLE if not exists ttd_privateContract_lu (PRIVATECONTRACTID string,PRIVATECONTRACTNAME string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Contract/';

ALTER TABLE ttd_privateContract_lu ADD IF NOT EXISTS PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

---------------------creative id lookup---------------
Drop table if exists ttd_creative_lu;
CREATE external TABLE if not exists ttd_creative_lu (creativeid string,creativename string) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Creative/';

ALTER TABLE ttd_creative_lu ADD IF NOT EXISTS PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

---------------------foldposition static lookup---------------
Drop table if exists ttd_foldposition_lu;
CREATE external TABLE if not exists ttd_foldposition_lu (foldposition_id string,foldposition string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/v2/static_lookups/foldposition/';

---------------------jarvis office lookup---------------
Drop table if exists ttd_office_lu;
Create table ttd_office_lu
as
Select distinct z.dsp_id as campaignid,office,office_id
from
(Select a.id,a.dsp_id,b.campaign_id
from
 (Select * from jarvis.insertion_order_ttd where dsp = 'TTD') as a
join
(Select * from jarvis.campaign_io_association  where dsp = 'TTD') as b
on a.id=b.io_id) as z
join
jarvis.campaign_monthly_info as c
on z.campaign_id = c.id;

---------------------jarvis adv cat  lookup---------------
Drop table if exists ttd_Advertiser_Category_lookup;
create external TABLE ttd_Advertiser_Category_lookup
(iab_category_id	  string,
 iab_sub_category	  string,
 advertiser_name	 string,
 uid	string,
 category_name		string,
 sub_category_name	string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored
AS
textfile LOCATION 's3://analyst-reports/Lookup/Advertiser_category_lookup/';

Drop table if exists ttd_Advertiser_Category_final_lu;
Create table ttd_Advertiser_Category_final_lu
as
Select distinct SUBSTRING(uid,0,(locate('_',uid)-1)) as advertiserid,category_name,sub_category_name from ttd_Advertiser_Category_lookup;

-------------metro static lookup(dma)-------------

Drop table if exists ttd_metro_lookup;
Create external table ttd_metro_lookup
( metro string,
 metro_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/v2/static_lookups/metro/';



-------1.70 cleaning ttd attribution file and joining with reds conversions feed and creating srs att conversions report-------

Drop table if exists attribution_ttd_miq_conversions_final;
Create temporary table attribution_ttd_miq_conversions_final
as
Select event_attribution_method,
event_time_utc,
(case when event_cross_device_attribution_model in ('(null)') then 'deterministic'  else event_cross_device_attribution_model end) as event_cross_device_attribution_model,
conversion_id,
impressionid
from attribution_ttd_miq_conversions
where event_attribution_method in ('ViewThrough','LastClick');

--------------------1.71 creating report schema-------------
Drop table if exists attribution_ttd_attribution_report;
 CREATE EXTERNAL TABLE `attribution_ttd_attribution_report`(
   `event_attribution_method` string,
  `event_time_utc` string,
  `advertiserid` string,
  `event_cross_device_attribution_model` string,
  `conversion_id` string,
   `impressionid` string,
  `conversiontype` string,
  `tdid` string,
  `ipaddress` string,
  `referrerurl` string,
  `conversion_monetary_value` string,
  `conversion_monetary_value_currency` string,
  `orderid` string,
  `td1` string,
  `td2` string,
  `td3` string,
  `td4` string,
  `td5` string,
  `td6` string,
  `td7` string,
  `td8` string,
  `td9` string,
  `td10` string
)
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
  's3://analyst-reports/ttd_reports/v2/attributedconversions'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551654');


------------------1.72  temp report for attributed conversions--------------------




Drop table if exists attribution_ttd_attribution_report_temp;
Create temporary table attribution_ttd_attribution_report_temp
as
Select distinct a.event_attribution_method, a.event_time_utc, b.advertiserid,a.event_cross_device_attribution_model, a.conversion_id,a.impressionid,
b.conversiontype,b.tdid,b.ipaddress,b.referrerurl,b.monetaryvalue as conversion_monetary_value,b.montaryvaluecurrency as conversion_monetary_value_currency,b.orderid,b.td1 ,b.td2 ,b.td3 ,b.td4 ,b.td5 ,b.td6 ,b.td7 ,b.td8 ,b.td9 ,b.td10 ,REGEXP_REPLACE(to_date(a.event_time_utc),"-","") as dayserial_numeric
from attribution_ttd_miq_conversions_final as a
join attribution_ttd_conversions as b
on a.conversion_id=b.conversionid;



------------------1.73 writing attributed conversions report---------
Insert overwrite table attribution_ttd_attribution_report  partition (dayserial_numeric)
Select  event_attribution_method, event_time_utc, advertiserid,event_cross_device_attribution_model, conversion_id,impressionid,
conversiontype,tdid,ipaddress,referrerurl,conversion_monetary_value,conversion_monetary_value_currency,orderid,td1 ,td2 ,td3 ,td4 ,td5 ,td6 ,td7 ,td8 ,td9 ,td10,dayserial_numeric
from attribution_ttd_attribution_report_temp;




-------1.8 reformatting 1 day of impressions in prep for flat file---------------

Drop table if exists ttd_impression_forflatfile;

CREATE  temporary TABLE ttd_impression_forflatfile
AS
SELECT a.logentrytime,
       a.impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       a.mediacost,
       a.feefeaturecost,
       a.datausagetotalcost,
       a.ttdcostinusd,
       a.partnercostinusd,
       a.advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
       'imp' as event_type,
       '0' as event_cross_device_attribution_model,
       '0' as conversion_type,
       '0' as conversion_monetary_value_currency,
       cast(NULL as string) as conversion_id,
	    0 as conversion_monetary_value,
	   '0' as conversion_orderid,
	   cast(NULL as string) as clickid,
	   cast(NULL as string) as conversion_impression_id,
	   cast(NULL as string) as click_impression_id
from
attribution_ttd_impressions_1day as a;











-------1.9 joining 30 days of impression with attributed conversions----------
Drop table if exists ttd_impression30_conversions;

CREATE  temporary  TABLE ttd_impression30_conversions
AS
SELECT b.event_time_utc AS logentrytime,
       cast(NULL as string) as impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       0 AS mediacost,
       0 AS feefeaturecost,
       0 AS datausagetotalcost,
       0 AS ttdcostinusd,
       0 AS partnercostinusd,
       0 AS advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
       b.event_attribution_method as event_type,
       b.event_cross_device_attribution_model,
       b.conversiontype as conversion_type,
       b.conversion_monetary_value_currency,
	   b.conversion_id,
       b.conversion_monetary_value,
       b.orderid as conversion_orderid,
	   cast(NULL as string) as clickid,
	   b.impressionid as conversion_impression_id,
	   cast(NULL as string) as click_impression_id

from
attribution_ttd_attribution_report_temp as b
join attribution_ttd_impressions as a
on
b.advertiserid = a.advertiserid
and b.impressionid=a.impressionid;


-------1.10joining 30 days of impressions with clicks------


Drop table if exists ttd_impression30_clicks;

CREATE  temporary  TABLE ttd_impression30_clicks
AS
SELECT b.logentrytime AS logentrytime,
       cast(NULL as string) as  impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       0 AS mediacost,
       0 AS feefeaturecost,
       0 AS datausagetotalcost,
       0 AS ttdcostinusd,
       0 AS partnercostinusd,
       0 AS advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
       'click' as event_type,
       '0' as event_cross_device_attribution_model,
       '0' as conversion_type,
       '0' as conversion_monetary_value_currency,
       cast(NULL as string) as conversion_id,
	    0 as conversion_monetary_value,
	   '0' as conversion_orderid,
	    b.clickid,
		cast(NULL as string) as conversion_impression_id,
	   b.displayimpressionid as click_impression_id
from
attribution_ttd_clicks as b
join attribution_ttd_impressions as a
on
b.advertiserid = a.advertiserid
and b.campaignid = a.campaignid
and b.displayimpressionid=a.impressionid;


-------1.11 creating flat file--------------

Drop table if exists ttd_flatfile;
CREATE EXTERNAL TABLE `ttd_flatfile`(
  `logentrytime` string,
  `impressionid` string,
  `partnerid` string,
  `advertiserid` string,
  `campaignid` string,
  `adgroupid` string,
  `privatecontractid` string,
  `audienceid` string,
  `creativeid` string,
  `adformat` string,
  `frequency` int,
  `supplyvendor` string,
  `supplyvendorpublisherid` string,
  `dealid` string,
  `site` string,
  `referrercategorieslist` string,
  `foldposition` int,
  `userhourofweek` int,
  `useragent` string,
  `ipaddress` string,
  `tdid` string,
  `country` string,
  `region` string,
  `metro` string,
  `city` string,
  `devicetype` int,
  `osfamily` int,
  `os` int,
  `browser` int,
  `recency` int,
  `languagecode` string,
  `mediacost` double,
  `feefeaturecost` double,
  `datausagetotalcost` double,
  `ttdcostinusd` double,
  `partnercostinusd` double,
  `advertisercostinusd` double,
  `latitude` string,
  `longitude` string,
  `deviceid` string,
  `zipcode` string,
  `processedtime` string,
  `devicemake` string,
  `devicemodel` string,
  `renderingcontext` string,
  `carrierid` string,
  `impressionplacementid` string,
  `event_type` string,
  `event_cross_device_attribution_model` string,
  `conversion_type` string,
  `conversion_monetary_value_currency` string,
  `conversion_id` string,
  `conversion_monetary_value` double,
  `conversion_orderid` string,
  `clickid` string,
  `conversion_impression_id` string,
  `click_impression_id` string)
PARTITIONED BY (
  `dayserial_numeric` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://analyst-adhoc/ttdreports/v2/flatfile'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574422556');



Insert overwrite table ttd_flatfile partition (dayserial_numeric)
Select a.* from
(Select x.*,REGEXP_REPLACE(to_date(x.logentrytime),"-","") as dayserial_numeric from ttd_impression_forflatfile as x
Union All
Select y.*,REGEXP_REPLACE(to_date(y.logentrytime),"-","") as dayserial_numeric from ttd_impression30_conversions as y
Union All
Select z.*,REGEXP_REPLACE(to_date(z.logentrytime),"-","") as dayserial_numeric from ttd_impression30_clicks as z) as a;




------------2.1 addidng media type to flat file--------------


Drop table if exists ttd_AdFormat_To_MediaType_lookup;
create external TABLE ttd_AdFormat_To_MediaType_lookup
(adformatid	  string,
 widthinpixels	  string,
 heightinpixels	 string,
 mediatypeid	string,
 MediaType		string,
 AdFormat		string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored
AS
textfile LOCATION 's3://analyst-reports/Lookup/AdFormat_To_MediaType/';


Drop table if exists ttd_flatfile_mediatype;
Create temporary table ttd_flatfile_mediatype
as
Select a.*,b.mediatypeid,b.mediatype
from
ttd_AdFormat_To_MediaType_lookup as b
right join
ttd_flatfile as a
on
a.adformat=b.adformat;

------------3.1 creating inventory report--------------

Drop table if exists ttd_inventoryperformace_report_temp;
CREATE temporary TABLE ttd_inventoryperformace_report_temp
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
       CreativeId,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   mediatype,
	   COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
	   SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUM(feefeaturecost*1.000000) AS feefeaturecost,
       SUM(datausagetotalcost*1.000000) AS datausagetotalcost,
       SUM(partnercostinusd*1.000000) AS partnercostinusd,
	   SUM(ttdcostinusd*1.000000) AS ttdcostinusd,
       COUNT(DISTINCT conversion_id) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event_type  IN ('ViewThrough') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event_type  IN ('LastClick') THEN 1 ELSE 0 END) AS PC_CONV,
       COUNT(DISTINCT clickid) AS CLICKS,
       SUM(conversion_monetary_value) AS conversion_monetary_value,
       REGEXP_REPLACE(to_date(logentrytime),"-","") as dayserial_numeric,
	   country
FROM ttd_flatfile_mediatype
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
       CreativeId,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   mediatype,
	   REGEXP_REPLACE(to_date(logentrytime),"-",""),
	   country;



-------------------3.3 CREATING FINAL INVENTORY REPORT----------------------

 Drop table if exists ttd_inventory_performance_report;
 CREATE EXTERNAL TABLE `ttd_inventory_performance_report`(
       DAY date ,
       ADVERTISERID string,
       CAMPAIGNID string,
       ADGROUPID string,
       PrivateContractID string,
       RenderingContext string,
       SupplyVendor string,
       SupplyVendorPublisherId  string,
       ReferrerCategoriesList  string,
       Site string,
       CreativeId string,
       FoldPosition int,
       AdFormat  string,
       ImpressionPlacementId  string,
       event_cross_device_attribution_model  string,
       conversion_type  string,
       conversion_monetary_value_currency  string,
	   mediatype  string,
	   IMPRESSIONS int,
	   ADVERTISERCOSTINUSD double,
       MEDIACOST double,
       feefeaturecost double,
       datausagetotalcost double,
       partnercostinusd double,
	   ttdcostinusd double,
       TOTALCONVERSIONS int,
       PV_CONV int,
       PC_CONV int,
       CLICKS int,
       conversion_monetary_value double,
       ADVERTISERNAME string,
       CAMPAIGNNAME string,
       ADGROUPNAME string,
       PrivateContractNAME string,
       CREATIVENAME STRING,
        FOLDPOSITIONNAME STRING,
       OFFICE STRING,
       AdvertiserCategory string,
 AdvertiserSubcategory string,
 country string)
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
  's3://analyst-reports/ttd_reports/v2/inventoryperformance'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551654');

  ---------------------3.4 inserting data to S3------------------
  Insert overwrite table ttd_inventory_performance_report PARTITION (dayserial_numeric)
  Select z.DAY,
       z.ADVERTISERID,
       z.CAMPAIGNID,
       z.ADGROUPID,
       z.PrivateContractID,
       z.RenderingContext,
       z.SupplyVendor,
       z.SupplyVendorPublisherId,
       z.ReferrerCategoriesList,
       z.Site,
       z.CreativeId,
       z.FoldPosition,
       z.AdFormat,
       z.ImpressionPlacementId,
       z.event_cross_device_attribution_model,
       z.conversion_type,
       z.conversion_monetary_value_currency,
	   z.mediatype,
	   z.IMPRESSIONS,
	   z.ADVERTISERCOSTINUSD,
       z.MEDIACOST,
       z.feefeaturecost,
       z.datausagetotalcost,
       z.partnercostinusd,
	   z.ttdcostinusd,
       z.TOTALCONVERSIONS,
       z.PV_CONV,
       z.PC_CONV,
       z.CLICKS,
       z.conversion_monetary_value,
       b.ADVERTISERNAME,c.CAMPAIGNNAME,d.ADGROUPNAME,e.PrivateContractNAME,f.CREATIVENAME,g.foldposition as FOLDPOSITIONNAME,h.OFFICE,i.category_name as AdvertiserCategory, i.sub_category_name as AdvertiserSubcategory,z.country,
	   z.dayserial_numeric
 from
 ttd_inventoryperformace_report_temp AS Z
  LEFT JOIN ttd_advertiser_lu AS b ON Z.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON Z.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON Z.ADGROUPID = d.ADGROUPID
  LEFT JOIN ttd_privateContract_lu AS e ON Z.PrivateContractID = e.PrivateContractID
  left join ttd_creative_lu AS f on z.CreativeId = f.creativeid
  left join ttd_foldposition_lu as g on z.foldposition = g.foldposition_id
  left join ttd_office_lu as h on z.campaignid = h.campaignid
  left join ttd_Advertiser_Category_final_lu as i on i.advertiserid = z.advertiserid ;


 -----------------------4.0 Creating Geo Report-------------------------

  -------------------4.1 CREATING TEMP GEO Report----------------------
Drop table if exists ttd_geo_performace_report_temp;
CREATE temporary  TABLE ttd_geo_performace_report_temp
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
	   PrivateContractID,
	   COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   mediatype,
	   COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
	   SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUM(feefeaturecost*1.000000) AS feefeaturecost,
       SUM(datausagetotalcost*1.000000) AS datausagetotalcost,
       SUM(partnercostinusd*1.000000) AS partnercostinusd,
	   SUM(ttdcostinusd*1.000000) AS ttdcostinusd,
       COUNT(DISTINCT conversion_id) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event_type  IN ('ViewThrough') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event_type  IN ('LastClick') THEN 1 ELSE 0 END) AS PC_CONV,
       COUNT(DISTINCT clickid) AS CLICKS,
       SUM(conversion_monetary_value) AS conversion_monetary_value,
       REGEXP_REPLACE(to_date(logentrytime),"-","") as dayserial_numeric
FROM ttd_flatfile_mediatype
GROUP BY TO_DATE(LOGENTRYTIME),
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
	     PrivateContractID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   mediatype,
	   REGEXP_REPLACE(to_date(logentrytime),"-","");

---------4.2 creating final geo report --------------

Drop table if exists ttd_geo_performance_report;
 CREATE EXTERNAL TABLE `ttd_geo_performance_report`(
       DAY date ,
       ADVERTISERID string,
       CAMPAIGNID string,
       ADGROUPID string,
     PrivateContractID string,
       COUNTRY string,
       REGION string,
       METRO string,
       CITY string,
       ZIPCODE string,
       event_cross_device_attribution_model  string,
       conversion_type  string,
       conversion_monetary_value_currency  string,
	   mediatype  string,
	   IMPRESSIONS int,
	   ADVERTISERCOSTINUSD double,
       MEDIACOST double,
       feefeaturecost double,
       datausagetotalcost double,
       partnercostinusd double,
	   ttdcostinusd double,
       TOTALCONVERSIONS int,
       PV_CONV int,
       PC_CONV int,
       CLICKS int,
       conversion_monetary_value double,
       ADVERTISERNAME string,
       CAMPAIGNNAME string,
       ADGROUPNAME string,
       privatecontractname string,
       OFFICE STRING,
       AdvertiserCategory string,
       AdvertiserSubcategory string,
       Metro_name string)
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
  's3://analyst-reports/ttd_reports/v2/geoperformance/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551654');

-------------4.3 inserting data into final table----------

Insert overwrite table ttd_geo_performance_report PARTITION (dayserial_numeric)
  Select
       z.DAY,
       z.ADVERTISERID,
       z.CAMPAIGNID,
       z.ADGROUPID,
	     z.PrivateContractID,
	   z.COUNTRY,
       z.REGION,
       z.METRO,
       z.CITY,
       z.ZIPCODE,
       z.event_cross_device_attribution_model,
       z.conversion_type,
       z.conversion_monetary_value_currency,
	   z.mediatype,
	   z.IMPRESSIONS,
	   z.ADVERTISERCOSTINUSD,
       z.MEDIACOST,
       z.feefeaturecost,
       z.datausagetotalcost,
       z.partnercostinusd,
	   z.ttdcostinusd,
       z.TOTALCONVERSIONS,
       z.PV_CONV,
       z.PC_CONV,
       z.CLICKS,
       z.conversion_monetary_value,
       b.ADVERTISERNAME,
	   c.CAMPAIGNNAME,
	   d.ADGROUPNAME,
	   e.privatecontractname,
	   h.OFFICE,
	   i.category_name as AdvertiserCategory,
	   i.sub_category_name as AdvertiserSubcategory,
	   j.metro_name,
	   z.dayserial_numeric
 from
 ttd_geo_performace_report_temp AS Z
  LEFT JOIN ttd_advertiser_lu AS b ON Z.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON Z.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON Z.ADGROUPID = d.ADGROUPID
 LEFT JOIN ttd_privateContract_lu AS e ON Z.PrivateContractID = e.PrivateContractID
  left join ttd_office_lu as h on z.campaignid = h.campaignid
  left join ttd_Advertiser_Category_final_lu as i on i.advertiserid = z.advertiserid
  left join ttd_metro_lookup as j on j.metro=z.metro;

------------5.0 Video Flat File------------
-------------5.1 loading video 30 days------------
SET hive.vectorized.execution.enabled = FALSE;
DROP TABLE IF EXISTS ttd_video_feed_30day;

CREATE external TABLE ttd_video_feed_30day (logentrytime string,impressionid string,videoeventcreativeview int,videoeventstart int,videoeventfirstquarter int,videoeventmidpoint int,videoeventthirdquarter int,videoeventcomplete int,videoeventmuted int,videoeventunmuted int,creativeistrackable string,creativewasviewable string,videoplaytimeinseconds string,videoviewabletimeinseconds string,videoeventcompanioncreativeview string,processedtime string) PARTITIONED BY (year bigint,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/video/';
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEARX$',month='$MONTHX$',day='$DAYX$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR$',month='$MONTH$',day='$DAY$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR1$',month='$MONTH1$',day='$DAY1$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR2$',month='$MONTH2$',day='$DAY2$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR3$',month='$MONTH3$',day='$DAY3$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR4$',month='$MONTH4$',day='$DAY4$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR5$',month='$MONTH5$',day='$DAY5$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR6$',month='$MONTH6$',day='$DAY6$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR7$',month='$MONTH7$',day='$DAY7$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR8$',month='$MONTH8$',day='$DAY8$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR9$',month='$MONTH9$',day='$DAY9$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR10$',month='$MONTH10$',day='$DAY10$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR11$',month='$MONTH11$',day='$DAY11$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR12$',month='$MONTH12$',day='$DAY12$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR13$',month='$MONTH13$',day='$DAY13$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR14$',month='$MONTH14$',day='$DAY14$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR15$',month='$MONTH15$',day='$DAY15$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR16$',month='$MONTH16$',day='$DAY16$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR17$',month='$MONTH17$',day='$DAY17$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR18$',month='$MONTH18$',day='$DAY18$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR19$',month='$MONTH19$',day='$DAY19$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR20$',month='$MONTH20$',day='$DAY20$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR21$',month='$MONTH21$',day='$DAY21$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR22$',month='$MONTH22$',day='$DAY22$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR23$',month='$MONTH23$',day='$DAY23$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR24$',month='$MONTH24$',day='$DAY24$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR25$',month='$MONTH25$',day='$DAY25$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR26$',month='$MONTH26$',day='$DAY26$');
ALTER TABLE ttd_video_feed_30day add if not exists PARTITION(year='$YEAR27$',month='$MONTH27$',day='$DAY27$');





-------5.2 --loading video 1 day-------

DROP TABLE IF EXISTS ttd_video_feed_1day;

CREATE external TABLE ttd_video_feed_1day (logentrytime string,impressionid string,videoeventcreativeview int,videoeventstart int,videoeventfirstquarter int,videoeventmidpoint int,videoeventthirdquarter int,videoeventcomplete int,videoeventmuted int,videoeventunmuted int,creativeistrackable string,creativewasviewable string,videoplaytimeinseconds string,videoviewabletimeinseconds string,videoeventcompanioncreativeview string,processedtime string) PARTITIONED BY (year bigint,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/video/';

ALTER TABLE ttd_video_feed_1day add if not exists PARTITION(year='$YEARY$',month='$MONTHY$',day='$DAYY$');

-------5.3 simplifying and cleaning video feed 1 day------
Drop table if exists ttd_video_feed_simplified;
Create temporary table ttd_video_feed_simplified
as
Select min(a.logentrytime) as logentrytime,
       a.impressionid,
       max(a.videoeventcreativeview) as videoeventcreativeview,
	   max(a.videoeventstart) as videoeventstart,
       max(a.videoeventfirstquarter) as videoeventfirstquarter,
       max(a.videoeventmidpoint) as videoeventmidpoint,
       max(a.videoeventthirdquarter) as videoeventthirdquarter,
       max(a.videoeventcomplete) as videoeventcomplete,
       max(a.creativeistrackable) as creativeistrackable,
       max(a.creativewasviewable) as creativewasviewable,
       max(a.videoplaytimeinseconds) as videoplaytimeinseconds,
       max(a.videoviewabletimeinseconds) as videoviewabletimeinseconds,
       max(a.videoeventcompanioncreativeview) as videoeventcompanioncreativeview,
       sum(a.videoeventmuted) as videoeventmuted,
       sum(a.videoeventunmuted) as videoeventunmuted
	from ttd_video_feed_1day as a
group by
a.impressionid ;

------------- 5.4 joining imp 1 day and vid 1 day---------
Drop table if exists ttd_imp_video_1day;
Create temporary table ttd_imp_video_1day
as
Select a.logentrytime,
       a.impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       a.mediacost,
       a.feefeaturecost,
       a.datausagetotalcost,
       a.ttdcostinusd,
       a.partnercostinusd,
       a.advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
	   b.videoeventcreativeview,
       b.videoeventstart,
       b.videoeventfirstquarter,
       b.videoeventmidpoint,
       b.videoeventthirdquarter,
       b.videoeventcomplete,
       b.videoeventmuted,
       b.videoeventunmuted,
       b.creativeistrackable,
       b.creativewasviewable,
       b.videoplaytimeinseconds,
       b.videoviewabletimeinseconds,
       b.videoeventcompanioncreativeview
from ttd_video_feed_simplified b
join attribution_ttd_impressions_1day a on a.impressionid = b.impressionid;

-------------5.5   -preparing video imp 1 day join for flat file append ----------
Drop table if exists ttd_video_feed_aggregated_1day_forflatfile;
Create  temporary table ttd_video_feed_aggregated_1day_forflatfile
as
Select a.logentrytime,
       a.impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       a.mediacost,
       a.feefeaturecost,
       a.datausagetotalcost,
       a.ttdcostinusd,
       a.partnercostinusd,
       a.advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
	   'imp' as event_type,
       '0' as event_cross_device_attribution_model,
       '0' as conversion_type,
       '0' as conversion_monetary_value_currency,
       cast(NULL as string) as conversion_id,
	    0 as conversion_monetary_value,
	   '0' as conversion_orderid,
	   cast(NULL as string) as clickid,
	   cast(NULL as string) as conversion_impression_id,
	   cast(NULL as string) as click_impression_id,
	   a.videoeventcreativeview,
	   a.videoeventstart,
      a.videoeventfirstquarter,
      a.videoeventmidpoint,
       a.videoeventthirdquarter,
       a.videoeventcomplete,
       a.creativeistrackable,
       a.creativewasviewable,
       a.videoplaytimeinseconds,
       a.videoviewabletimeinseconds,
       a.videoeventcompanioncreativeview,
       a.videoeventmuted,
       a.videoeventunmuted
from ttd_imp_video_1day as a;


------------5.6   30 days of video ----join ----attribution_ttd_miq_conversions_final(this is the enriched conversion sample)------

Drop table if exists ttd_impression30_video30_conversions;
Create  temporary table ttd_impression30_video30_conversions
as
Select distinct        a.logentrytime,
       a.impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       a.mediacost,
       a.feefeaturecost,
       a.datausagetotalcost,
       a.ttdcostinusd,
       a.partnercostinusd,
       a.advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
       a.event_type,
       a.event_cross_device_attribution_model,
       a.conversion_type,
       a.conversion_monetary_value_currency,
	     a.conversion_id,
       a.conversion_monetary_value,
       a.conversion_orderid,
	     a.clickid,
	     a.conversion_impression_id,
	     a.click_impression_id,
  0 as   videoeventcreativeview,
 0 as  videoeventstart,
    0 as   videoeventfirstquarter,
  0 as   videoeventmidpoint,
    0 as   videoeventthirdquarter,
      0 as   videoeventcomplete,
       0 as   creativeistrackable,
      0 as   creativewasviewable,
     0 as   videoplaytimeinseconds,
     0 as   videoviewabletimeinseconds,
      0 as  videoeventcompanioncreativeview,
     0 as   videoeventmuted,
      0 as   videoeventunmuted
from ttd_impression30_conversions as a
where  a.conversion_impression_id in (Select distinct impressionid from  ttd_video_feed_30day where impressionid is not NULL);

------------5.7   30 days of video ----join with click file of display attributed clicks---------
Drop table if exists ttd_impression30_video30_clicks;
Create  temporary table ttd_impression30_video30_clicks
as
Select distinct        a.logentrytime,
       a.impressionid,
       a.partnerid,
       a.advertiserid,
       a.campaignid,
       a.adgroupid,
       a.privatecontractid,
       a.audienceid,
       a.creativeid,
       a.adformat,
       a.frequency,
       a.supplyvendor,
       a.supplyvendorpublisherid,
       a.dealid,
       a.site,
       a.referrercategorieslist,
       a.foldposition,
       a.userhourofweek,
       a.useragent,
       a.ipaddress,
       a.tdid,
       a.country,
       a.region,
       a.metro,
       a.city,
       a.devicetype,
       a.osfamily,
       a.os,
       a.browser,
       a.recency,
       a.languagecode,
       a.mediacost,
       a.feefeaturecost,
       a.datausagetotalcost,
       a.ttdcostinusd,
       a.partnercostinusd,
       a.advertisercostinusd,
       a.latitude,
       a.longitude,
       a.deviceid,
       a.zipcode,
       a.processedtime,
       a.devicemake,
       a.devicemodel,
       a.renderingcontext,
       a.carrierid,
       a.impressionplacementid,
       a.event_type,
       a.event_cross_device_attribution_model,
       a.conversion_type,
       a.conversion_monetary_value_currency,
	     a.conversion_id,
       a.conversion_monetary_value,
       a.conversion_orderid,
	     a.clickid,
	     a.conversion_impression_id,
	     a.click_impression_id,
  0 as   videoeventcreativeview,
 0 as  videoeventstart,
    0 as   videoeventfirstquarter,
  0 as   videoeventmidpoint,
    0 as   videoeventthirdquarter,
      0 as   videoeventcomplete,
       0 as   creativeistrackable,
      0 as   creativewasviewable,
     0 as   videoplaytimeinseconds,
     0 as   videoviewabletimeinseconds,
      0 as  videoeventcompanioncreativeview,
     0 as   videoeventmuted,
      0 as   videoeventunmuted
	  from ttd_impression30_clicks as a
	  where a.click_impression_id in (Select distinct impressionid from  ttd_video_feed_30day  where impressionid is not NULL);

--------5.8 Creating table schema-----------

Drop table if exists ttd_video_flatfile;
CREATE EXTERNAL TABLE ttd_video_flatfile(
      logentrytime string,
      impressionid string,
      partnerid string,
      advertiserid string,
      campaignid string,
      adgroupid string,
      privatecontractid string,
      audienceid string,
      creativeid string,
      adformat string,
      frequency int,
      supplyvendor string,
      supplyvendorpublisherid string,
      dealid string,
      site string,
      referrercategorieslist string,
      foldposition int,
      userhourofweek int,
      useragent string,
      ipaddress string,
      tdid string,
      country string,
      region string,
      metro string,
      city string,
      devicetype int,
      osfamily int,
      os int,
      browser int,
      recency int,
      languagecode string,
      mediacost double,
      feefeaturecost double,
      datausagetotalcost double,
      ttdcostinusd double,
      partnercostinusd double,
      advertisercostinusd double,
      latitude string,
      longitude string,
      deviceid string,
      zipcode string,
      processedtime string,
      devicemake string,
      devicemodel string,
      renderingcontext string,
      carrierid string,
      impressionplacementid string,
      event_type string,
      event_cross_device_attribution_model string,
      conversion_type string,
      conversion_monetary_value_currency string,
      conversion_id string,
       conversion_monetary_value double,
       conversion_orderid string,
       clickid string,
       conversion_impression_id string,
       click_impression_id string,
        videoeventcreativeview  string,
videoeventstart  string,
     videoeventfirstquarter  string,
    videoeventmidpoint  string,
    videoeventthirdquarter string,
    videoeventcomplete string,
      creativeistrackable string,
      creativewasviewable string,
     videoplaytimeinseconds string,
  videoviewabletimeinseconds string,
    videoeventcompanioncreativeview string,
      videoeventmuted string,
       videoeventunmuted  string)
  PARTITIONED BY (
 dayserial_numeric string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
 'field.delim'='\t',
 'serialization.format'='\t')
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 's3://analyst-adhoc/ttdreports/v2/video_flatfile/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1573549335');


  ------------5.9 Inserting to S3---------------------
  Insert overwrite table ttd_video_flatfile partition (dayserial_numeric)
Select a.* from
(Select x.*,REGEXP_REPLACE(to_date(x.logentrytime),"-","") as dayserial_numeric from ttd_video_feed_aggregated_1day_forflatfile as x
Union All
Select y.*,REGEXP_REPLACE(to_date(y.logentrytime),"-","") as dayserial_numeric from ttd_impression30_video30_conversions as y
Union All
Select z.*,REGEXP_REPLACE(to_date(z.logentrytime),"-","") as dayserial_numeric from ttd_impression30_video30_clicks as z) as a;



-----------------6.0---creating video inventory report--------------
---------------6.1 report schema-----------
Drop table if exists ttd_video_inventory_performance_report;
 CREATE EXTERNAL TABLE `ttd_video_inventory_performance_report`(
       DAY date ,
       ADVERTISERID string,
       CAMPAIGNID string,
       ADGROUPID string,
       PrivateContractID string,
       RenderingContext string,
       SupplyVendor string,
       SupplyVendorPublisherId  string,
       ReferrerCategoriesList  string,
       Site string,
       CreativeId string,
       FoldPosition int,
       AdFormat  string,
       ImpressionPlacementId  string,
       event_cross_device_attribution_model  string,
       conversion_type  string,
       conversion_monetary_value_currency  string,
	   mediatype  string,
	   IMPRESSIONS int,
	   ADVERTISERCOSTINUSD double,
       MEDIACOST double,
       feefeaturecost double,
       datausagetotalcost double,
       partnercostinusd double,
	   ttdcostinusd double,
       TOTALCONVERSIONS int,
       PV_CONV int,
       PC_CONV int,
       CLICKS int,
       conversion_monetary_value double,
       ADVERTISERNAME string,
       CAMPAIGNNAME string,
       ADGROUPNAME string,
       PrivateContractNAME string,
       CREATIVENAME STRING,
        FOLDPOSITIONNAME STRING,
       OFFICE STRING,
       AdvertiserCategory string,
 AdvertiserSubcategory string,
 Video_started int,
 Q1_Completed int,
 Q2_Completed int,
 Q3_Completed int,
 Video_Completed int,
 total_time float,
 country string)
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
  's3://analyst-reports/ttd_reports/v2/video_inventoryperformance'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551654');

  -----------6.2-------creating temp table---------


  Drop table if exists ttd_video_inventory_performance_report_temp;
CREATE temporary TABLE ttd_video_inventory_performance_report_temp
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
       CreativeId,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
	   SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUM(feefeaturecost*1.000000) AS feefeaturecost,
       SUM(datausagetotalcost*1.000000) AS datausagetotalcost,
       SUM(partnercostinusd*1.000000) AS partnercostinusd,
	   SUM(ttdcostinusd*1.000000) AS ttdcostinusd,
       COUNT(DISTINCT conversion_id) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event_type  IN ('ViewThrough') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event_type  IN ('LastClick') THEN 1 ELSE 0 END) AS PC_CONV,
       COUNT(DISTINCT clickid) AS CLICKS,
       SUM(conversion_monetary_value) AS conversion_monetary_value,
	    sum(videoeventstart) as  Video_started ,
 sum(videoeventfirstquarter) as Q1_Completed,
 sum(videoeventmidpoint) as Q2_Completed ,
 sum(videoeventthirdquarter) as Q3_Completed,
 sum(videoeventcomplete) as Video_Completed,
 sum(videoplaytimeinseconds) as total_time ,
       REGEXP_REPLACE(to_date(logentrytime),"-","") as dayserial_numeric,
	   country
FROM ttd_video_flatfile
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
       CreativeId,
       FoldPosition,
       AdFormat,
       ImpressionPlacementId,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   REGEXP_REPLACE(to_date(logentrytime),"-",""),
	   country;

  ---------------------6.3 inserting data to S3------------------
  Insert overwrite table ttd_video_inventory_performance_report PARTITION (dayserial_numeric)
  Select z.DAY,
       z.ADVERTISERID,
       z.CAMPAIGNID,
       z.ADGROUPID,
       z.PrivateContractID,
       z.RenderingContext,
       z.SupplyVendor,
       z.SupplyVendorPublisherId,
       z.ReferrerCategoriesList,
       z.Site,
       z.CreativeId,
       z.FoldPosition,
       z.AdFormat,
       z.ImpressionPlacementId,
       z.event_cross_device_attribution_model,
       z.conversion_type,
       z.conversion_monetary_value_currency,
	   j.mediatype,
	   z.IMPRESSIONS,
	   z.ADVERTISERCOSTINUSD,
       z.MEDIACOST,
       z.feefeaturecost,
       z.datausagetotalcost,
       z.partnercostinusd,
	   z.ttdcostinusd,
       z.TOTALCONVERSIONS,
       z.PV_CONV,
       z.PC_CONV,
       z.CLICKS,
       z.conversion_monetary_value,
       b.ADVERTISERNAME,c.CAMPAIGNNAME,d.ADGROUPNAME,e.PrivateContractNAME,f.CREATIVENAME,g.foldposition as FOLDPOSITIONNAME,h.OFFICE,i.category_name as AdvertiserCategory, i.sub_category_name as AdvertiserSubcategory,
	   z.Video_started,
 z.Q1_Completed,
 z.Q2_Completed,
 z.Q3_Completed,
 z.Video_Completed,
 z.total_time,
 z.country,
	   z.dayserial_numeric
 from
 ttd_video_inventory_performance_report_temp AS Z
  LEFT JOIN ttd_advertiser_lu AS b ON Z.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON Z.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON Z.ADGROUPID = d.ADGROUPID
  LEFT JOIN ttd_privateContract_lu AS e ON Z.PrivateContractID = e.PrivateContractID
  left join ttd_creative_lu AS f on z.CreativeId = f.creativeid
  left join ttd_foldposition_lu as g on z.foldposition = g.foldposition_id
  left join ttd_office_lu as h on z.campaignid = h.campaignid
  left join ttd_Advertiser_Category_final_lu as i on i.advertiserid = z.advertiserid
  left join ttd_AdFormat_To_MediaType_lookup as j on z.adformat=j.adformat;


-----------------7.0----creating video geo report--------------------------
-------------------7.1 CREATING TEMP GEO Report----------------------
Drop table if exists ttd_video_geo_performace_report_temp;
CREATE Temporary TABLE ttd_video_geo_performace_report_temp
AS
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
	   PrivateContractID,
	   COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   k.mediatype,
	   COUNT(DISTINCT IMPRESSIONID) AS IMPRESSIONS,
	   SUM(ADVERTISERCOSTINUSD*1.000000) AS ADVERTISERCOSTINUSD,
       SUM(MEDIACOST*1.000000) AS MEDIACOST,
       SUM(feefeaturecost*1.000000) AS feefeaturecost,
       SUM(datausagetotalcost*1.000000) AS datausagetotalcost,
       SUM(partnercostinusd*1.000000) AS partnercostinusd,
	   SUM(ttdcostinusd*1.000000) AS ttdcostinusd,
       COUNT(DISTINCT conversion_id) AS TOTALCONVERSIONS,
       SUM(CASE WHEN event_type  IN ('ViewThrough') THEN 1 ELSE 0 END) AS PV_CONV,
       SUM(CASE WHEN event_type  IN ('LastClick') THEN 1 ELSE 0 END) AS PC_CONV,
       COUNT(DISTINCT clickid) AS CLICKS,
       SUM(conversion_monetary_value) AS conversion_monetary_value,
	   sum(videoeventstart) as  Video_started ,
 sum(videoeventfirstquarter) as Q1_Completed,
 sum(videoeventmidpoint) as Q2_Completed ,
 sum(videoeventthirdquarter) as Q3_Completed,
 sum(videoeventcomplete) as Video_Completed,
 sum(videoplaytimeinseconds) as total_time ,
       REGEXP_REPLACE(to_date(logentrytime),"-","") as dayserial_numeric
FROM ttd_video_flatfile
 left join ttd_AdFormat_To_MediaType_lookup as k on ttd_video_flatfile.adformat=k.adformat
GROUP BY TO_DATE(LOGENTRYTIME),
       ADVERTISERID,
       CAMPAIGNID,
       ADGROUPID,
	   PrivateContractID,
       COUNTRY,
       REGION,
       METRO,
       CITY,
       ZIPCODE,
       event_cross_device_attribution_model,
       conversion_type,
       conversion_monetary_value_currency,
	   mediatype,
	   REGEXP_REPLACE(to_date(logentrytime),"-","");

---------7.2 creating final geo report --------------

 Drop table if exists ttd_video_geo_performance_report;
 CREATE EXTERNAL TABLE `ttd_video_geo_performance_report`(
       DAY date ,
       ADVERTISERID string,
       CAMPAIGNID string,
       ADGROUPID string,
   PrivateContractID string,
       COUNTRY string,
       REGION string,
       METRO string,
       CITY string,
       ZIPCODE string,
       event_cross_device_attribution_model  string,
       conversion_type  string,
       conversion_monetary_value_currency  string,
	   mediatype  string,
	   IMPRESSIONS int,
	   ADVERTISERCOSTINUSD double,
       MEDIACOST double,
       feefeaturecost double,
       datausagetotalcost double,
       partnercostinusd double,
	   ttdcostinusd double,
       TOTALCONVERSIONS int,
       PV_CONV int,
       PC_CONV int,
       CLICKS int,
       conversion_monetary_value double,
       ADVERTISERNAME string,
       CAMPAIGNNAME string,
       ADGROUPNAME string,
   privatecontractname string,
       OFFICE STRING,
       AdvertiserCategory string,
       AdvertiserSubcategory string,
       Metro_name string,
 Video_started int,
 Q1_Completed int,
 Q2_Completed int,
 Q3_Completed int,
 Video_Completed int,
 total_time float)
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
  's3://analyst-reports/ttd_reports/v2/video_geoperformance/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1559551654');

-------------7.3 inserting data into final table----------

Insert overwrite table ttd_video_geo_performance_report PARTITION (dayserial_numeric)
  Select
       z.DAY,
       z.ADVERTISERID,
       z.CAMPAIGNID,
       z.ADGROUPID,
	   z.privatecontractid,
	   z.COUNTRY,
       z.REGION,
       z.METRO,
       z.CITY,
       z.ZIPCODE,
       z.event_cross_device_attribution_model,
       z.conversion_type,
       z.conversion_monetary_value_currency,
	   z.mediatype,
	   z.IMPRESSIONS,
	   z.ADVERTISERCOSTINUSD,
       z.MEDIACOST,
       z.feefeaturecost,
       z.datausagetotalcost,
       z.partnercostinusd,
	   z.ttdcostinusd,
       z.TOTALCONVERSIONS,
       z.PV_CONV,
       z.PC_CONV,
       z.CLICKS,
       z.conversion_monetary_value,
       b.ADVERTISERNAME,
	   c.CAMPAIGNNAME,
	   d.ADGROUPNAME,
	   e.privatecontractname,
	   h.OFFICE,
	   i.category_name as AdvertiserCategory,
	   i.sub_category_name as AdvertiserSubcategory,
	   j.metro_name,
	    z.Video_started,
 z.Q1_Completed,
 z.Q2_Completed,
 z.Q3_Completed,
 z.Video_Completed,
 z.total_time,
	   z.dayserial_numeric
 from
 ttd_video_geo_performace_report_temp AS Z
  LEFT JOIN ttd_advertiser_lu AS b ON Z.ADVERTISERID = b.ADVERTISERID
  LEFT JOIN ttd_campaign_lu AS c ON Z.CAMPAIGNID = c.CAMPAIGNID
  LEFT JOIN ttd_adgroup_lu AS d ON Z.ADGROUPID = d.ADGROUPID
   LEFT JOIN ttd_privateContract_lu AS e ON Z.PrivateContractID = e.PrivateContractID
  left join ttd_office_lu as h on z.campaignid = h.campaignid
  left join ttd_Advertiser_Category_final_lu as i on i.advertiserid = z.advertiserid
  left join ttd_metro_lookup as j on j.metro=z.metro;

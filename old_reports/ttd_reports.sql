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

Drop table if exists segment_ttd_conversions;
CREATE EXTERNAL TABLE segment_ttd_conversions (logentrytime string,conversionid string,advertiserid string,conversiontype string,tdid string,ipaddress string,referrerurl string,monetaryvalue string,montaryvaluecurrency string,orderid string,td1 string,td2 string,td3 string,td4 string,td5 string,td6 string,td7 string,td8 string,td9 string,td10 string,processedtime string) PARTITIONED BY (year string,month string,day string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED
AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://dwh-reports-data/thetradedesk-feed/conversions/';

ALTER TABLE segment_ttd_conversions ADD IF NOT EXISTS PARTITION (YEAR = '2020',MONTH = '04',DAY = '25');

Drop table if exists ttd_conversions_temp;
CREATE TABLE ttd_conversions_temp
AS
SELECT logentrytime,
       MIN(conversionid) AS conversionid,
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
FROM segment_ttd_conversions
GROUP BY logentrytime,
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

DROP TABLE if EXISTS ttd_segmentload_report;
CREATE EXTERNAL TABLE ttd_segmentload_report (DAY string,HOUROFDAY string,ADVERTISERID string,CONVERSIONTYPE string,TOTALLOADS string) PARTITIONED BY (DAYSERIAL_NUMERIC string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://analyst-adhoc/ttdreports/segmentloads/';



INSERT OVERWRITE TABLE ttd_segmentload_report  PARTITION (DAYSERIAL_NUMERIC)
SELECT TO_DATE(LOGENTRYTIME) AS DAY,
       SUBSTR(LOGENTRYTIME,12,2) AS HOUROFDAY,
       ADVERTISERID,
       CONVERSIONTYPE,
       COUNT(DISTINCT CONVERSIONID) AS TOTALLOADS,
       SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8) AS DAYSERIAL_NUMERIC
FROM ttd_conversions_temp
GROUP BY TO_DATE(LOGENTRYTIME),
         SUBSTR(LOGENTRYTIME,12,2),
         ADVERTISERID,
         CONVERSIONTYPE,
         SUBSTR(REGEXP_REPLACE(LOGENTRYTIME,"-",""),1,8);


Drop table if exists ttd_conversions_temp;

Drop table if exists ttd_advertiser_lu;
Create external table ttd_advertiser_lu
(ADVERTISERID  string,
ADVERTISERNAME string
) PARTITIONED BY (year string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE LOCATION 's3://thetradedesk-feed/id-name/Advertiser/';

ALTER TABLE ttd_advertiser_lu ADD IF NOT EXISTS PARTITION (YEAR = '2020',MONTH = '04',DAY = '25');

Drop table if exists ttd_segment_load_report_tmp;
CREATE EXTERNAL TABLE ttd_segment_load_report_tmp (day date,hour_of_day string,advertiser_id string,conversion_type string,total_loads int, advertiser_name string,dayserial_numeric int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED
AS
TEXTFILE;


INSERT Overwrite TABLE ttd_segment_load_report_tmp
SELECT a.DAY,
       a.HOUROFDAY,
       a.ADVERTISERID,
       a.CONVERSIONTYPE,
       a.TOTALLOADS,
       b.ADVERTISERNAME,
       a.DAYSERIAL_NUMERIC
FROM ttd_segmentload_report AS a
left join ttd_advertiser_lu as b
on a.advertiserid=b.advertiserid;

Drop table if exists ttd_segmentload_report;
Drop table if exists ttd_conversions_temp;
Drop table if exists segment_ttd_conversions;

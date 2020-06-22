USE rithav;

Drop table if exists ttd_console_global_creative;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_creative
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
creative	string,
creative_id	string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_creative/';

Alter table ttd_console_global_creative recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

USE rithav;

Drop table if exists ttd_console_global_device_player;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_device_player
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
device_make string,
device_model string,
device_type string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_device_player/';

Alter table ttd_console_global_device_player recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

USE rithav;

Drop table if exists ttd_console_global_os_browser;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_os_browser
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
browser string,
operating_system string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_os_browser/';

Alter table ttd_console_global_os_browser recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

USE rithav;

Drop table if exists ttd_console_global_io_li;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_io_li
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
clicks	string,
impressions	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_io_li/';

Alter table ttd_console_global_io_li recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------


USE rithav;

Drop table if exists ttd_console_global_geo;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_geo
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
city string,
region string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_geo/';

Alter table ttd_console_global_geo recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------


USE rithav;

Drop table if exists ttd_console_global_environment;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_environment
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
carrier_name string,
site string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_environment/';

Alter table ttd_console_global_environment recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

USE rithav;

Drop table if exists ttd_console_global_page_category;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_page_category
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
category_name string,
app string,
carrier_id string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_page_category/';

Alter table ttd_console_global_page_category recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------

USE rithav;

Drop table if exists ttd_console_global_day_time;
CREATE EXTERNAL TABLE IF NOT EXISTS ttd_console_global_day_time
(
dt	string,
advertiser	string,
advertiser_id	string,
campaign	string,
campaign_id	string,
ad_group	string,
ad_group_id string,
user_hour_of_day string,
user_day_of_week string,
impressions	string,
clicks	string,
player_25_complete	string,
player_50_complete	string,
player_75_complete	string,
player_completed_views	string,
all_last_click_view_conversions string
)

PARTITIONED BY (DAYSERIAL_NUMERIC bigint)
Row format delimited fields terminated by '\t'
STORED AS textfile location 's3://thetradedesk-feed/ttd_reports/report_files/ttd_console_global_day_time/';

Alter table ttd_console_global_day_time recover partitions;

---------------------------------------------------------------------------------------------------------------------------------------------------------------
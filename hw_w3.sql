CREATE OR REPLACE EXTERNAL TABLE `booming-post-375009.trips_data_all.external_fhw`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_booming-post-375009/data_fhw/fhv_tripdata_*.parquet']
);

CREATE OR REPLACE TABLE `booming-post-375009.trips_data_all.external_fhw_non_part` AS
SELECT 
dispatching_base_num,
pickup_datetime,		
dropOff_datetime,			
SR_Flag,	
Affiliated_base_number	
FROM `booming-post-375009.trips_data_all.external_fhw`;

select count(1) FROM `booming-post-375009.trips_data_all.external_fhw_non_part`;
select count(1) FROM `booming-post-375009.trips_data_all.external_fhw`;

select COUNT(DISTINCT Affiliated_base_number) FROM `booming-post-375009.trips_data_all.external_fhw_non_part`; --317.94mb
select COUNT(DISTINCT Affiliated_base_number) FROM `booming-post-375009.trips_data_all.external_fhw`; --0

select count(1) FROM `booming-post-375009.trips_data_all.external_fhw` where 
PUlocationID is null and DOlocationID is null;

CREATE OR REPLACE TABLE `booming-post-375009.trips_data_all.external_fhw_clus`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT dispatching_base_num, pickup_datetime, dropOff_datetime, Affiliated_base_number	FROM `booming-post-375009.trips_data_all.external_fhw_non_part`;



select count(distinct Affiliated_base_number)
from `booming-post-375009.trips_data_all.external_fhw_clus` 
where pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'; 
--1591

select count(distinct Affiliated_base_number)
from `booming-post-375009.trips_data_all.external_fhw_non_part` 
where pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'; --1591
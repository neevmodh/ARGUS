-- ARGUS Hive tables over HDFS (external: dropping a table never deletes the data).
CREATE DATABASE IF NOT EXISTS argus;
USE argus;

-- Raw CSV exactly as uploaded. OpenCSVSerde handles the quoted Description column;
-- it reads every column as STRING, so cast in queries.
DROP TABLE IF EXISTS accidents_raw;
CREATE EXTERNAL TABLE accidents_raw (
  id STRING, source STRING, severity STRING, start_time STRING, end_time STRING,
  start_lat STRING, start_lng STRING, end_lat STRING, end_lng STRING, distance_mi STRING,
  description STRING, street STRING, city STRING, county STRING, state STRING,
  zipcode STRING, country STRING, timezone STRING, airport_code STRING, weather_timestamp STRING,
  temperature_f STRING, wind_chill_f STRING, humidity STRING, pressure_in STRING, visibility_mi STRING,
  wind_direction STRING, wind_speed_mph STRING, precipitation_in STRING, weather_condition STRING,
  amenity STRING, bump STRING, crossing STRING, give_way STRING, junction STRING, no_exit STRING,
  railway STRING, roundabout STRING, station STRING, stop STRING, traffic_calming STRING,
  traffic_signal STRING, turning_loop STRING, sunrise_sunset STRING, civil_twilight STRING,
  nautical_twilight STRING, astronomical_twilight STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"")
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/argus/raw/accidents'
TBLPROPERTIES ("skip.header.line.count" = "1");

-- Curated Parquet written by Spark (jobs/batch/ingest_clean.py), partitioned by state/year.
DROP TABLE IF EXISTS accidents;
CREATE EXTERNAL TABLE accidents (
  id STRING, severity INT, start_time TIMESTAMP, end_time TIMESTAMP, lat DOUBLE, lng DOUBLE,
  distance_mi DOUBLE, street STRING, city STRING, county STRING, timezone STRING,
  temperature_f DOUBLE, humidity DOUBLE, pressure_in DOUBLE, visibility_mi DOUBLE,
  wind_speed_mph DOUBLE, precipitation_in DOUBLE, weather_condition STRING, weather_bucket STRING,
  is_night INT, amenity INT, bump INT, crossing INT, give_way INT, junction INT, no_exit INT,
  railway INT, roundabout INT, station INT, stop INT, traffic_calming INT, traffic_signal INT,
  month INT, hour INT, day_of_week INT, `date` DATE, duration_min DOUBLE
)
PARTITIONED BY (state STRING, year INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/argus/curated/accidents';

MSCK REPAIR TABLE accidents;
SHOW PARTITIONS accidents;

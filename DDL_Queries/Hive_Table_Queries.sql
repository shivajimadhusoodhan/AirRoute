

Welcome1234
-------------------------------------------------------------------------------------------------------------------------------------------------
----QUERIES TO MOVE DATA FROM HDUSER TO ROOT USER------------------------------------------------------------------------------------------------

scp -P 2222 /home/hduser26x100/AirRoute/airports.dat root@localhost:/root/users/airroute
scp -P 2222 /home/hduser26x100/AirRoute/airlines.dat root@localhost:/root/users/airroute
scp -P 2222 /home/hduser26x100/AirRoute/routes.dat root@localhost:/root/users/airroute
scp -P 2222 /home/hduser26x100/AirRoute/Air_Traffic_Cargo_Statistics.csv root@localhost:/root/users/airroute
scp -P 2222 /home/hduser26x100/AirRoute/Air_Traffic_Landings_Statistics.csv root@localhost:/root/users/airroute
scp -P 2222 /home/hduser26x100/AirRoute/Air_Traffic_Passenger_Statistics.csv root@localhost:/root/users/airroute


-------------------------------------------------------------------------------------------------------------------------------------------------
----QUERIES FOR LOADING DATA FILE TO HDFS--------------------------------------------------------------------------------------------------------

hdfs dfs -copyFromLocal /root/users/airroute/airports.dat  /user/airroute
hdfs dfs -copyFromLocal /root/users/airroute/airlines.dat  /user/airroute
hdfs dfs -copyFromLocal /root/users/airroute/routes.dat  /user/airroute
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Cargo_Statistics.csv  /user/airroute
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Landings_Statistics.csv  /user/airroute
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Passenger_Statistics.csv  /user/airroute


-------------------------------------------------------------------------------------------------------------------------------------------------
-----AIRLINES QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS AIR_ROUTE_DB;
USE AIR_ROUTE_DB;

CREATE TABLE IF NOT EXISTS AIRLINES ( airline_id STRING,airline_name STRING,alias STRING, iata_code STRING,icao_code STRING,callsign STRING,country STRING,active STRING)
COMMENT 'AIRLINES DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE;
LOAD DATA INPATH '/user/airroute/airlines.dat' overwrite into table AIRLINES;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_AIRLINES;
CREATE TABLE IF NOT EXISTS PARTITIONED_AIRLINES ( airline_id STRING,airline_name STRING,alias STRING, iata_code STRING,icao_code STRING,callsign STRING,active STRING)
COMMENT 'partitioned AIRLINES DETAILS'
PARTITIONED BY (country STRING)
CLUSTERED BY (airline_id) INTO 32 BUCKETS
STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE PARTITIONED_AIRLINES PARTITION (country) SELECT  airline_id,airline_name,alias, iata_code ,icao_code ,callsign,active,country FROM AIRLINES;

-------------------------------------------------------------------------------------------------------------------------------------------------
-----AIRPORT  QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AIRPORTS ( airport_id INT,airport_name STRING,city STRING, country STRING,iata_code STRING,icao_code STRING,latitude DECIMAL(8,6), longititude DECIMAL(8,6), altitude INT, timezone DECIMAL(4,2), dst STRING, database_timezone STRING,airport_type STRING,data_source STRING)
COMMENT 'AIRPORTS DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE;
LOAD DATA INPATH '/user/airroute/airports.dat' overwrite into table AIRPORTS;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_AIRPORTS;
CREATE TABLE IF NOT EXISTS PARTITIONED_AIRPORTS (airport_id INT,airport_name STRING,city STRING, iata_code STRING,icao_code STRING,latitude DECIMAL(8,6), longititude DECIMAL(8,6), altitude INT, timezone DECIMAL(4,2), dst STRING, database_timezone STRING,airport_type STRING,data_source STRING)
COMMENT 'partitioned AIRPORTS DETAILS'
PARTITIONED BY (country STRING)
CLUSTERED BY (city) INTO 32 BUCKETS
STORED AS SEQUENCEFILE;

set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE PARTITIONED_AIRPORTS PARTITION (country) SELECT  airport_id , airport_name, city,iata_code,icao_code,latitude,longititude,altitude,timezone,dst,database_timezone,airport_type,data_source,country FROM AIRPORTS;
	   
	   
-------------------------------------------------------------------------------------------------------------------------------------------------
-----ROUTES  QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ROUTES ( 
airline_code STRING,
airline_id STRING,
source_airport STRING, 
source_airport_id INT,
destination_airport STRING,
destination_airport_id INT,
codeshare STRING,stops INT, 
equipement STRING)
COMMENT 'ROUTE DETAILS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
LOAD DATA INPATH '/user/airroute/routes.dat' overwrite into table ROUTES;


-------------------------------------------------------------------------------------------------------------------------------------------------
-----CARGO STATISTICS QUERIES----------------------------------------------------------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS CARGO_STATISTICS (
Activity_Period INT, 
Operating_Airline STRING,
Operating_Airline_IATA_Code CHAR(2),
Published_Airline STRING,
Published_Airline_IATA_Code CHAR(2),
GEO_Summary STRING,
GEO_Region STRING,
Activity_Type_Code STRING,
Cargo_Type_Code STRING,
Cargo_Aircraft_Type STRING,
Cargo_Weight_LBS BIGINT,
CargoMetric_TONS DECIMAL(11,7))
COMMENT 'ROUTE DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/airroute/Air_Traffic_Cargo_Statistics.csv' overwrite into table CARGO_STATISTICS;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_CARGO_STATISTICS;
CREATE TABLE IF NOT EXISTS PARTITIONED_CARGO_STATISTICS (Operating_Airline STRING,
Operating_Airline_IATA_Code CHAR(2),
Published_Airline STRING,
Published_Airline_IATA_Code CHAR(2),
GEO_Summary STRING,
GEO_Region STRING,
Activity_Type_Code STRING,
Cargo_Type_Code STRING,
Cargo_Aircraft_Type STRING,
Cargo_Weight_LBS BIGINT,
CargoMetric_TONS DECIMAL(11,7))
COMMENT 'partitioned cargo DETAILS'
PARTITIONED BY (YEAR INT, MONTH INT)
CLUSTERED BY (GEO_Region) INTO 32 BUCKETS
STORED AS SEQUENCEFILE;
set hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE PARTITIONED_CARGO_STATISTICS PARTITION (Year, Month) SELECT  
Operating_Airline,
Operating_Airline_IATA_Code,
Published_Airline ,
Published_Airline_IATA_Code ,
GEO_Summary ,
GEO_Region ,
Activity_Type_Code ,
Cargo_Type_Code ,
Cargo_Aircraft_Type ,
Cargo_Weight_LBS ,
CargoMetric_TONS,
SUBSTR(Activity_Period,1,4),
SUBSTR(Activity_Period, 5,7) FROM CARGO_STATISTICS;


-------------------------------------------------------------------------------------------------------------------------------------------------
-----LANDINGS STATISTICS QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS LANDINGS_STATISTICS (Activity_Period INT, Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Landing_Aircraft_type STRING,Aircraft_Body_Type STRING,Aircraft_Manufacturer  STRING,Aircraft_Model STRING, Aircraft_Version INT,Landing_Count INT,Total_Landed_Weight BIGINT)
COMMENT 'ROUTE DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/airroute/Air_Traffic_Landings_Statistics.csv' overwrite into table LANDINGS_STATISTICS;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_LANDINGS_STATISTICS;
CREATE TABLE IF NOT EXISTS PARTITIONED_LANDINGS_STATISTICS (Operating_Airline STRING,
Operating_Airline_IATA_Code CHAR(2),
Published_Airline STRING,
Published_Airline_IATA_Code CHAR(2),
GEO_Summary STRING,
GEO_Region STRING, 
Landing_Aircraft_type STRING,
Aircraft_Body_Type STRING,
Aircraft_Manufacturer STRING,
Aircraft_Model STRING, 
Aircraft_Version INT,
Landing_Count INT,
Total_Landed_Weight BIGINT)
COMMENT 'partitioned landing DETAILS'
PARTITIONED BY (YEAR INT, MONTH INT)
CLUSTERED BY (GEO_Region) INTO 32 BUCKETS
STORED AS SEQUENCEFILE;
set hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE PARTITIONED_LANDINGS_STATISTICS PARTITION (Year, Month) SELECT  
Operating_Airline,
Operating_Airline_IATA_Code,
Published_Airline ,
Published_Airline_IATA_Code ,
GEO_Summary ,
GEO_Region ,
Landing_Aircraft_type,
Aircraft_Body_Type,
Aircraft_Manufacturer,
Aircraft_Model, 
Aircraft_Version,
Landing_Count,
Total_Landed_Weight,
SUBSTR(Activity_Period,1,4),
SUBSTR(Activity_Period, 5,7) FROM LANDINGS_STATISTICS;

-------------------------------------------------------------------------------------------------------------------------------------------------
-----PASSENGER STATISTICS QUERIES----------------------------------------------------------------------------------------------------------------------------

Drop table PASSENGER_STATISTICS;
CREATE TABLE IF NOT EXISTS PASSENGER_STATISTICS (Activity_Period INT, Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Activity_Type_Code STRING,Price_Category_Code STRING,Terminal STRING, Boarding_Area CHAR(1), Passenger_Count
BIGINT)
COMMENT 'PASSENGER_STATISTICS DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/airroute/Air_Traffic_Passenger_Statistics.csv' overwrite into table PASSENGER_STATISTICS;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_PASSENGER_STATISTICS;
CREATE TABLE IF NOT EXISTS PARTITIONED_PASSENGER_STATISTICS (Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Activity_Type_Code STRING,Price_Category_Code STRING,Terminal STRING, Boarding_Area CHAR(1), Passenger_Count BIGINT)
COMMENT 'partitioned passenger DETAILS'
PARTITIONED BY (YEAR INT, MONTH INT)
CLUSTERED BY (GEO_Region) INTO 32 BUCKETS
STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE PARTITIONED_PASSENGER_STATISTICS PARTITION (Year, Month) SELECT  
Operating_Airline,
Operating_Airline_IATA_Code,
Published_Airline ,
Published_Airline_IATA_Code ,
GEO_Summary ,
GEO_Region ,
Activity_Type_Code ,
Price_Category_Code,
Terminal, 
Boarding_Area, 
Passenger_Count,
SUBSTR(Activity_Period,1,4),
SUBSTR(Activity_Period, 5,7)  FROM PASSENGER_STATISTICS;





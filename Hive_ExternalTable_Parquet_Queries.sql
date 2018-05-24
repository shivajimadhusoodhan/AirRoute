
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

hdfs dfs -copyFromLocal /root/users/airroute/airports.dat  /user/airroute/tables/airports
hdfs dfs -copyFromLocal /root/users/airroute/airlines.dat  /user/airroute/tables/airline
hdfs dfs -copyFromLocal /root/users/airroute/routes.dat  /user/airroute/tables/routes
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Cargo_Statistics.csv  /user/airroute/tables/CargoStatistics
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Landings_Statistics.csv  /user/airroute/tables/LandingsStatistics
hdfs dfs -copyFromLocal /root/users/airroute/Air_Traffic_Passenger_Statistics.csv  /user/airroute/tables/PassengerStatistics



-------------------------------------------------------------------------------------------------------------------------------------------------
-----CREATING DATABASE IN HIVE ------------------------------------------------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS AIR_ROUTE_DATABASE;
USE AIR_ROUTE_DATABASE;

-------------------------------------------------------------------------------------------------------------------------------------------------
-----QUERIES TO MAKE HIVE SUPPPORT PARTITION AND BUCKETING --------------------------------------------------------------------------------------

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

-------------------------------------------------------------------------------------------------------------------------------------------------
-----AIRLINES QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS AIRLINES ( 
	airline_id STRING,
	airline_name STRING,
	alias STRING, 
	iata_code STRING,
	icao_code STRING,
	callsign STRING,
	country STRING,
	active STRING)
COMMENT 'AIRLINES DETAILS'
COMMENT 'AIRLINES DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar"     = "\""
	)
STORED AS TEXTFILE
LOCATION '/user/airroute/tables/airline';

CREATE TABLE IF NOT EXISTS PARTITIONED_AIRLINES ( 
	airline_id STRING,
	airline_name STRING,
	alias STRING, 
	iata_code STRING,
	icao_code STRING,
	callsign STRING,
	active STRING)
COMMENT 'Partitioned AIRLINES DETAILS'
PARTITIONED BY (country STRING)
CLUSTERED BY (airline_id) INTO 32 BUCKETS
STORED AS PARQUET;

TRUNCATE TABLE  PARTITIONED_AIRLINES;

INSERT  INTO
TABLE PARTITIONED_AIRLINES 
PARTITION (country) 
SELECT  airline_id,
	airline_name,
	alias, 
	iata_code ,
	icao_code ,
	callsign,
	active,
	country 
FROM AIRLINES;

--FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. Cannot validate serde: parquet.hive.serde.ParquetHiveSerDe

-------------------------------------------------------------------------------------------------------------------------------------------------
-----AIRPORT  QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS AIRPORTS ( 
	airport_id INT,
	airport_name STRING,
	city STRING, 
	country STRING,
	iata_code STRING,
	icao_code STRING,
	latitude DECIMAL(8,6), 
	longititude DECIMAL(8,6), 
	laltitude INT, 
	timezone DECIMAL(4,2), 
	dst STRING, database_timezone STRING,airport_type STRING,data_source STRING)
COMMENT 'AIRPORTS DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar"     = "\""
	)
STORED AS TEXTFILE
LOCATION '/user/airroute/tables/airports';


CREATE EXTERNAL TABLE IF NOT EXISTS PARTITIONED_AIRPORTS (
	airport_id INT,
	airport_name STRING,
	city STRING, 
	iata_code STRING,
	icao_code STRING,
	latitude DECIMAL(8,6), 
	longititude DECIMAL(8,6), 
	altitude INT, 
	timezone DECIMAL(4,2), 
	dst STRING, 
	database_timezone STRING,
	airport_type STRING,
	data_source STRING)
COMMENT 'partitioned AIRPORTS DETAILS'
PARTITIONED BY (country STRING)
CLUSTERED BY (city) INTO 32 BUCKETS
STORED AS PARQUET;

TRUNCATE TABLE  PARTITIONED_AIRPORTS;

INSERT  INTO
TABLE PARTITIONED_AIRPORTS 
PARTITION (country) 
SELECT  airport_id , 
		airport_name, 
		city,iata_code,
		icao_code,
		latitude,
		longititude,
		altitude,
		timezone,
		dst,
		database_timezone,
		airport_type,
		data_source,
		country 
FROM AIRPORTS;
	   
	   
-------------------------------------------------------------------------------------------------------------------------------------------------
-----ROUTES  QUERIES----------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ROUTES ( 
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
STORED AS TEXTFILE
LOCATION '/user/airroute/tables/routes';
LOAD DATA INPATH '/user/airroute/routes.dat' 
OVERWRITE INTO TABLE ROUTES;


-------------------------------------------------------------------------------------------------------------------------------------------------
-----CARGO STATISTICS QUERIES----------------------------------------------------------------------------------------------------------------------------


CREATE EXTERNAL TABLE IF NOT EXISTS CARGO_STATISTICS (
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
LOCATION '/user/airroute/tables/cargo_statistics'
tblproperties("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS PARTITIONED_CARGO_STATISTICS (Operating_Airline STRING,
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
STORED AS SEQUENCEFILE
LOCATION '/user/airroute/tables/partitioned_cargo_statistics';


INSERT 
TABLE PARTITIONED_CARGO_STATISTICS 
PARTITION (Year, Month) 
SELECT  
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
	SUBSTR(Activity_Period, 5,7) 
FROM CARGO_STATISTICS;


-------------------------------------------------------------------------------------------------------------------------------------------------
-----LANDINGS STATISTICS QUERIES------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS LANDINGS_STATISTICS (Activity_Period INT, Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Landing_Aircraft_type STRING,Aircraft_Body_Type STRING,Aircraft_Manufacturer  STRING,Aircraft_Model STRING, Aircraft_Version INT,Landing_Count INT,Total_Landed_Weight BIGINT)
COMMENT 'ROUTE DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
	WITH SERDEPROPERTIES (
		"separatorChar" = ",",
		"quoteChar"     = "\""
	)
STORED AS TEXTFILE
LOCATION '/user/airroute/tables/landings_statistics'
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/airroute/Air_Traffic_Landings_Statistics.csv' overwrite into table LANDINGS_STATISTICS;

DROP TABLE IF EXISTS PARTITIONED_LANDINGS_STATISTICS;
CREATE EXTERNAL TABLE IF NOT EXISTS PARTITIONED_LANDINGS_STATISTICS (Operating_Airline STRING,
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
STORED AS SEQUENCEFILE
LOCATION '/user/airroute/tables/partitioned_landings_statistics';
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
CREATE EXTERNAL TABLE IF NOT EXISTS PASSENGER_STATISTICS (Activity_Period INT, Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Activity_Type_Code STRING,Price_Category_Code STRING,Terminal STRING, Boarding_Area CHAR(1), Passenger_Count
BIGINT)
COMMENT 'PASSENGER_STATISTICS DETAILS'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) STORED AS TEXTFILE
LOCATION '/user/airroute/tables/passenger_statistics'
tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '/user/airroute/Air_Traffic_Passenger_Statistics.csv' overwrite into table PASSENGER_STATISTICS;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.enforce.bucketing = true;

DROP TABLE IF EXISTS PARTITIONED_PASSENGER_STATISTICS;
CREATE EXTERNAL TABLE IF NOT EXISTS PARTITIONED_PASSENGER_STATISTICS (Operating_Airline STRING,Operating_Airline_IATA_Code CHAR(2), Published_Airline STRING,Published_Airline_IATA_Code CHAR(2),GEO_Summary STRING,GEO_Region STRING,Activity_Type_Code STRING,Price_Category_Code STRING,Terminal STRING, Boarding_Area CHAR(1), Passenger_Count BIGINT)
COMMENT 'partitioned passenger DETAILS'
PARTITIONED BY (YEAR INT, MONTH INT)
CLUSTERED BY (GEO_Region) INTO 32 BUCKETS
STORED AS SEQUENCEFILE
LOCATION '/user/airroute/tables/partitioned_passenger_statistics';
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
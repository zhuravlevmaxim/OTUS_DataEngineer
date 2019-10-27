--0. Create external table
DROP TABLE IF EXISTS CRIMES;

CREATE EXTERNAL TABLE CRIMES (
    District varchar(255),
    Crimes_total int,
    Crimes_monthly int,
    Frequent_crime_types varchar(255),
    Lat decimal,
    Lng decimal
) as copy from '/tmp/data/Crimes.parquet' parquet;

SELECT * FROM CRIMES;

--1.0 Create ETL file metadata table
CREATE SCHEMA IF NOT EXISTS SBX;

CREATE TABLE IF NOT EXISTS SBX.ETL_FILE_LOAD (
	FILE_ID AUTO_INCREMENT(1, 1, 1) NOT NULL,
	SOURCE VARCHAR(128) NOT NULL,
	FILE_NAME VARCHAR(64) NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	PRIMARY KEY (FILE_ID, SOURCE, FILE_NAME) ENABLED
)
ORDER BY SOURCE, FILE_NAME
UNSEGMENTED ALL NODES;

--1.1 Create stage table
CREATE TABLE IF NOT EXISTS SBX.STG_CRIME (
    District varchar(255) NOT NULL,
    Crimes_total int NOT NULL ,
    Crimes_monthly int NOT NULL ,
    Frequent_crime_types varchar(255),
    Lat decimal,
    Lng decimal,
	PRIMARY KEY (District)
)
ORDER BY
    District
SEGMENTED BY HASH(District) ALL NODES;

--1.2 Populate stage table
INSERT INTO SBX.STG_CRIME(
    District,
    Crimes_total,
    Crimes_monthly,
    Frequent_crime_types,
    Lat,
    Lng
)SELECT
    District,
    Crimes_total,
    Crimes_monthly,
    Frequent_crime_types,
    Lat,
    Lng
FROM CRIMES;

SELECT COUNT(*) FROM SBX.STG_CRIME;

--1.3 Populate file metadata
INSERT INTO SBX.ETL_FILE_LOAD (
	SOURCE ,
	FILE_NAME ,
	LOAD_TS
)
SELECT	DISTINCT
	'ERP' AS SOURCE ,
	'STG_OPER' AS FILE_NAME ,
	GETDATE() as LOAD_TS
FROM SBX.STG_CRIME stg
	LEFT JOIN SBX.ETL_FILE_LOAD fl
		ON fl.SOURCE = 'ERP'
			AND fl.FILE_NAME = 'STG_OPER'
WHERE fl.FILE_ID IS NULL;

SELECT * FROM SBX.ETL_FILE_LOAD;

--2.1 Create ODS table
CREATE TABLE IF NOT EXISTS SBX.ODS_CRIME (
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
    District varchar(255) NOT NULL,
    Crimes_total int NOT NULL ,
    Crimes_monthly int NOT NULL ,
    Frequent_crime_types varchar(255),
    Lat decimal,
    Lng decimal,
    PRIMARY KEY (FILE_ID, District) ENABLED
)
ORDER BY
	FILE_ID,
	District
SEGMENTED BY HASH(District) ALL NODES;

--2.2 Create view to populate ODS
CREATE OR REPLACE VIEW SBX.V_STG_CRIME_ODS_CRIME AS
SELECT
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.District ,
    src.Crimes_total,
	src.Crimes_monthly ,
    src.Frequent_crime_types,
    src.Lat,
    src.Lng
FROM SBX.STG_CRIME src
INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
ON fl.SOURCE = 'ERP' AND
	fl.FILE_NAME = 'STG_OPER'
LEFT JOIN SBX.ODS_CRIME trg
ON fl.FILE_ID = trg.FILE_ID AND
	src.District = trg.District
WHERE trg.FILE_ID IS NULL;

SELECT COUNT(*) FROM SBX.V_STG_CRIME_ODS_CRIME;

--2.3 Populate ODS table
INSERT INTO SBX.ODS_CRIME (
	FILE_ID ,
	LOAD_TS ,
	District ,
	Crimes_total,
	Crimes_monthly,
	Frequent_crime_types,
	Lat,
    Lng
)
SELECT
	src.FILE_ID ,
	src.LOAD_TS ,
	src.District ,
	src.Crimes_total,
	src.Crimes_monthly,
	src.Frequent_crime_types,
	src.Lat,
    src.Lng
FROM SBX.V_STG_CRIME_ODS_CRIME src;

SELECT * FROM SBX.ODS_CRIME;

--3.1 Create DDS HUB table
CREATE TABLE IF NOT EXISTS SBX.DDS_HUB_DISTRICT (
	HK_DISTRICT VARCHAR(32) NOT NULL,
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	DISTRICT VARCHAR(255) NOT NULL,
	PRIMARY KEY (HK_DISTRICT) ENABLED
)
ORDER BY HK_DISTRICT
SEGMENTED BY HASH(HK_DISTRICT) ALL NODES;

--3.2 Create view to populate HUB table
CREATE OR REPLACE VIEW SBX.V_STG_OPER_DDS_HUB_DISTRICT AS
SELECT DISTINCT
	MD5(src.District) AS HK_DISTRICT ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.District
FROM SBX.STG_CRIME src
INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
ON fl.SOURCE = 'ERP' AND fl.FILE_NAME = 'STG_OPER'
LEFT JOIN SBX.DDS_HUB_DISTRICT trg
ON MD5(src.District) = trg.HK_DISTRICT
WHERE trg.HK_DISTRICT IS NULL;

--3.3 Populate HUB table
INSERT INTO SBX.DDS_HUB_DISTRICT (
	HK_DISTRICT ,
	FILE_ID ,
	LOAD_TS ,
	DISTRICT
)
SELECT
	src.HK_DISTRICT ,
	src.FILE_ID ,
	src.LOAD_TS ,
	src.District
FROM SBX.V_STG_OPER_DDS_HUB_DISTRICT src;

SELECT * FROM SBX.DDS_HUB_DISTRICT;

--4.1 Create DDS SATELLITE table
CREATE TABLE IF NOT EXISTS SBX.DDS_ST_DISTRICT_METRICS (
	HK_DISTRICT VARCHAR(32) NOT NULL,
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	HASHDIFF VARCHAR(32) NOT NULL,
	CRIMES_TOTAL INT NOT NULL,
	CRIMES_MONTHLY INT NOT NULL,
    FREQUENT_CRIME_TYPES VARCHAR(255),
    LAT DECIMAL,
    LNG DECIMAL,
	PRIMARY KEY (HK_DISTRICT, HASHDIFF) ENABLED
)
ORDER BY
	HK_DISTRICT ,
	LOAD_TS
SEGMENTED BY HASH(HK_DISTRICT) ALL NODES;

--4.2 Create view to populate SATELLITE table
CREATE OR REPLACE VIEW SBX.V_STG_OPER_DDS_ST_DISTRICT_METRICS AS
SELECT
	MD5(src.District) AS HK_DISTRICT ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	MD5(isnull(src.Crimes_total::INT,0)||
	    isnull(src.Crimes_monthly::INT,0)||
	    isnull(src.Frequent_crime_types::VARCHAR(255),'NULL')||
	    isnull(src.Lat::DECIMAL,0.0)||
	    isnull(src.Lng::DECIMAL,0.0)) AS HASHDIFF ,
	src.Crimes_total,
	src.Crimes_monthly,
	src.Frequent_crime_types,
    src.Lat,
    src.Lng
FROM SBX.STG_CRIME src
	INNER JOIN (
		SELECT
			FILE_ID ,
			LOAD_TS ,
			SOURCE ,
			FILE_NAME
		FROM SBX.ETL_FILE_LOAD
		LIMIT 1 OVER (PARTITION BY SOURCE, FILE_NAME ORDER BY LOAD_TS DESC)
		) fl
			ON fl.SOURCE = 'ERP'
				AND fl.FILE_NAME = 'STG_OPER'
	LEFT JOIN SBX.DDS_ST_DISTRICT_METRICS trg
		ON MD5(src.District) = trg.HK_DISTRICT
			AND MD5(isnull(src.Crimes_total::INT,0)||
	    isnull(src.Crimes_monthly::INT,0)||
	    isnull(src.Frequent_crime_types::VARCHAR(255),'NULL')||
	    isnull(src.Lat::DECIMAL,0.0)||
	    isnull(src.Lng::DECIMAL,0.0)) = trg.HASHDIFF
WHERE trg.HK_DISTRICT IS NULL;

--4.3 Populate SATELLITE table
INSERT INTO SBX.DDS_ST_DISTRICT_METRICS (
	HK_DISTRICT ,
	FILE_ID ,
	LOAD_TS ,
	HASHDIFF ,
	CRIMES_TOTAL,
	CRIMES_MONTHLY,
	FREQUENT_CRIME_TYPES,
    LAT,
    LNG
)
SELECT
	src.HK_DISTRICT ,
	src.FILE_ID ,
	src.LOAD_TS ,
	src.HASHDIFF ,
	src.Crimes_total ,
	src.Crimes_monthly ,
	src.Frequent_crime_types,
    src.Lat,
    src.Lng
FROM SBX.V_STG_OPER_DDS_ST_DISTRICT_METRICS src;

SELECT * FROM SBX.DDS_ST_DISTRICT_METRICS;

-- 5.0 view to get actual data
CREATE OR REPLACE VIEW SBX.V_DDS_OPER AS
SELECT
	DISTRICT ,
	CRIMES_TOTAL,
	CRIMES_MONTHLY ,
	FREQUENT_CRIME_TYPES,
    LAT,
    LNG
FROM SBX.DDS_HUB_DISTRICT hub
INNER JOIN SBX.DDS_ST_DISTRICT_METRICS st
ON hub.HK_DISTRICT = st.HK_DISTRICT
LIMIT 1 OVER (PARTITION BY DISTRICT ORDER BY st.LOAD_TS DESC);

--6.0 Sample MART on actual data
SELECT * FROM SBX.V_DDS_OPER;

-- This file will create an ORC table with bird data

-- DROP TABLE IF EXISTS smcveigh_birds_csv;

-- First, map the CSV data we downloaded in Hive
create external table smcveigh_birds_csv(
  gbifID integer,
  datasetKey string,
  occurrenceID string,
  kingdom string,
  phylum string,
  class string,
  `order` string, --backticks because order is a reserved word
  family string,
  genus string,
  species string,
  infraspecificEpithet string, 
  taxonRank string, 
  scientificName string,
  verbatimScientificName string,
  verbatimScientificNameAuthorship string,
  countryCode string,
  locality string,
  stateProvince string,
  occurrenceStatus string,
  individualCount integer,
  publishingOrgKey string,
  decimalLatitude float,
  decimalLongitude float,
  coordinateUncertaintyInMeters float,
  coordinatePrecision float,
  elevation float, 
  elevationAccuracy float,
  depth float,
  depthAccuracy float,
  eventDate string,
  day smallint,
  month smallint,
  year smallint,
  taxonKey integer,
  speciesKey integer,
  basisOfRecord string,
  institutionCode string,
  collectionCode string,
  catalogNumber string,
  recordNumber string,
  identifiedBy string,
  dateIdentified string,
  license string,
  rightsHolder string,
  recordedBy string,
  typeStatus string,
  establishmentMeans string,
  lastInterpreted string,
  mediaType string,
  issue string)
  -- row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' -- SerDe not necessary for tab-separated csv with clean file

--WITH SERDEPROPERTIES (
--   "separatorChar" = ","
--)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' -- tab-delimited csv
STORED AS TEXTFILE
  location '/user/hadoop/smcveigh/inputs' -- This should be the HDFS location (my project location is at /home/hadoop/smcveigh and hadoop is /smcveigh)
  TBLPROPERTIES ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select * from smcveigh_birds_csv limit 5;
select species, decimalLatitude, decimalLongitude FROM smcveigh_birds_csv limit 100;

-- Create an ORC table for ontime data (Note "stored as ORC" at the end)
create table smcveigh_birds(
  gbifID integer,
  datasetKey string,
  occurrenceID string,
  kingdom string,
  phylum string,
  class string,
  `order` string,
  family string,
  genus string,
  species string,
  infraspecificEpithet string, 
  taxonRank string, 
  scientificName string,
  verbatimScientificName string,
  verbatimScientificNameAuthorship string,
  countryCode string,
  locality string,
  stateProvince string,
  occurrenceStatus string,
  individualCount integer,
  publishingOrgKey string,
  decimalLatitude float,
  decimalLongitude float,
  roundedLatitude float, -- use this and the next row to map to biomes
  roundedLongitude float,
  coordinateUncertaintyInMeters float,
  coordinatePrecision float,
  elevation float, 
  elevationAccuracy float,
  depth float,
  depthAccuracy float,
  eventDate string,
  day smallint,
  month smallint,
  year smallint,
  taxonKey integer,
  speciesKey integer,
  basisOfRecord string,
  institutionCode string,
  collectionCode string,
  catalogNumber string,
  recordNumber string,
  identifiedBy string,
  dateIdentified string,
  license string,
  rightsHolder string,
  recordedBy string,
  typeStatus string,
  establishmentMeans string,
  lastInterpreted string,
  mediaType string,
  issue string)
  stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table smcveigh_birds
select 
  gbifID,
  datasetKey,
  occurrenceID,
  kingdom,
  phylum,
  class,
  `order`, 
  family,
  genus,
  species,
  infraspecificEpithet, 
  taxonRank, 
  scientificName,
  verbatimScientificName,
  verbatimScientificNameAuthorship,
  countryCode,
  locality,
  stateProvince,
  occurrenceStatus,
  individualCount,
  publishingOrgKey,
  decimalLatitude,
  decimalLongitude,
  ROUND(decimalLatitude, 2) as roundedLatitude, -- use this and the next row to map to biomes (check syntax)
  ROUND(decimalLongitude, 2) as roundedLongitude,
  coordinateUncertaintyInMeters,
  coordinatePrecision,
  elevation, 
  elevationAccuracy,
  depth,
  depthAccuracy,
  eventDate,
  day,
  month,
  year,
  taxonKey,
  speciesKey,
  basisOfRecord,
  institutionCode,
  collectionCode,
  catalogNumber,
  recordNumber,
  identifiedBy,
  dateIdentified,
  license,
  rightsHolder,
  recordedBy,
  typeStatus,
  establishmentMeans,
  lastInterpreted,
  mediaType,
  issue
from smcveigh_birds_csv;

select species, stateProvince, decimalLatitude, decimalLongitude, roundedLatitude, roundedLongitude, eventDate from smcveigh_birds limit 100;
select species, stateProvince, decimalLatitude, decimalLongitude, roundedLatitude, roundedLongitude, eventDate from smcveigh_birds WHERE roundedLatitude = 52.29 AND roundedLongitude = -113.83 limit 100;


--select * from smcveigh_birds_csv;
-- where origin is not null and dest is not null
-- and depdelay is not null and arrdelay is not null
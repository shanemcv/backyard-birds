-- this will join birds to biomes table

create table smcveigh_birds_and_biomes(
  -- Birds
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
  issue string,
  -- Biomes 
  biomeCode string,
  biomeName string)
  stored as orc;

-- Perform join
insert overwrite table smcveigh_birds_and_biomes
select
-- Birds
  b.gbifID,
  b.datasetKey,
  b.occurrenceID,
  b.kingdom,
  b.phylum,
  b.class,
  b.`order`,
  b.family,
  b.genus,
  b.species,
  b.infraspecificEpithet, 
  b.taxonRank, 
  b.scientificName,
  b.verbatimScientificName,
  b.verbatimScientificNameAuthorship,
  b.countryCode,
  b.locality,
  b.stateProvince,
  b.occurrenceStatus,
  b.individualCount,
  b.publishingOrgKey,
  b.decimalLatitude,
  b.decimalLongitude,
  b.roundedLatitude, -- use this and the next row to map to biomes
  b.roundedLongitude,
  b.coordinateUncertaintyInMeters,
  b.coordinatePrecision,
  b.elevation, 
  b.elevationAccuracy,
  b.depth,
  b.depthAccuracy,
  b.eventDate,
  b.day,
  b.month,
  b.year,
  b.taxonKey,
  b.speciesKey,
  b.basisOfRecord,
  b.institutionCode,
  b.collectionCode,
  b.catalogNumber,
  b.recordNumber,
  b.identifiedBy,
  b.dateIdentified,
  b.license,
  b.rightsHolder,
  b.recordedBy,
  b.typeStatus,
  b.establishmentMeans,
  b.lastInterpreted,
  b.mediaType,
  b.issue,
  -- biome columns
  COALESCE(bi.NA_L3CODE, 'Unknown') as biomeCode,
  COALESCE(bi.NA_L3NAME, 'Unknown') as biomeName
from smcveigh_birds b
left join smcveigh_biomes bi
  on b.roundedLatitude = bi.lat
  and b.roundedLongitude = bi.lon;

-- Test queries
select species, stateProvince, decimalLatitude, decimalLongitude, roundedLatitude, roundedLongitude, eventDate, biomeCode, biomeName from smcveigh_birds_and_biomes WHERE roundedLatitude = 52.29 AND roundedLongitude = -113.83 limit 100;

select species, stateProvince, roundedLatitude, roundedLongitude, eventDate, biomeCode, biomeName from smcveigh_birds_and_biomes WHERE biomeCode = '8.2.3' limit 100;
-- First create the standard table, then the hbase table

--------------------------------------------------------
------------- (1) Bird Sightings By State --------------
--------------------------------------------------------

-- Standard Table
-- drop table if exists smcveigh_birds_by_state;
-- Baseline
create external table smcveigh_birds_by_state (
    stateProvince string,
    species string,
    sightings integer)
stored as orc
location '/user/hadoop/smcveigh/birds_by_state';

-- Populate
insert overwrite table smcveigh_birds_by_state
select
  stateProvince,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where stateProvince is not null and species is not null
group by stateProvince, species;

-- Test Query
select species, sightings
from smcveigh_birds_by_state
where stateProvince = 'Illinois'
order by sightings desc
limit 10;

+------------------------+------------+
|        species         | sightings  |
+------------------------+------------+
| Cardinalis cardinalis  | 12796      |
| Junco hyemalis         | 11339      |
| Zenaida macroura       | 11106      |
| Passer domesticus      | 11079      |
| Dryobates pubescens    | 9147       |
| Poecile atricapillus   | 8502       |
| Spinus tristis         | 7812       |
| Haemorhous mexicanus   | 7693       |
| Sturnus vulgaris       | 7624       |
| Cyanocitta cristata    | 6213       |
+------------------------+------------+

select species, sightings
from smcveigh_birds_by_state
where stateProvince = 'Illinois' and sightings > 0
order by sightings asc
limit 10;

+------------------------+------------+
|        species         | sightings  |
+------------------------+------------+
| Bucephala islandica    | 1          |
| Pipilo maculatus       | 1          |
| Ammospiza leconteii    | 1          |
| Leiothlypis celata     | 1          |
| Gallinago gallinago    | 1          |
| Melospiza lincolnii    | 1          |
| Perdix perdix          | 1          |
| Progne subis           | 1          |
| Pandion haliaetus      | 1          |
| Nycticorax nycticorax  | 1          |
+------------------------+------------+

select stateProvince, sightings
from smcveigh_birds_by_state
where species = 'Bubo virginianus'-- great horned owl
order by sightings desc
limit 60; -- should get all

+----------------------------+------------+
|       stateprovince        | sightings  |
+----------------------------+------------+
| California                 | 751        |
| Pennsylvania               | 499        |
| Texas                      | 429        |
| Florida                    | 389        |
| Wisconsin                  | 307        |
| Washington                 | 289        |
| Illinois                   | 261        |
| Arizona                    | 247        |
| Georgia                    | 245        |
| New York                   | 245        |
| Colorado                   | 239        |
| Ohio                       | 226        |
| Montana                    | 220        |
-------------------------------------------

-- HBASE TABLE
-- First, need to create the hbase table in hbase shell
-- hbase shell
-- create 'smcveigh_birds_by_state_hbase', 'info', 'stats'
-- exit
create external table smcveigh_birds_by_state_hbase (
    state_species string,
    stateProvince string,
    species string,
    sightings integer)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:stateProvince,info:species,stats:sightings')
TBLPROPERTIES ('hbase.table.name' = 'smcveigh_birds_by_state_hbase');

insert overwrite table smcveigh_birds_by_state_hbase
select
  concat(stateProvince, '_', species) as state_species,
  stateProvince,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where stateProvince is not null and species is not null
group by stateProvince, species;

-- Test Query
select sightings
from smcveigh_birds_by_state_hbase
where state_species = 'Illinois_Junco hyemalis';

+------------+
| sightings  |
+------------+
| 11339      |
+------------+

--------------------------------------------------------
------------- (2) Bird Sightings By Biome --------------
--------------------------------------------------------

-- Standard Table
-- drop table if exists smcveigh_birds_by_biome;
-- Baseline
create external table smcveigh_birds_by_biome (
    biomeName string,
    species string,
    sightings integer)
stored as orc
location '/user/hadoop/smcveigh/birds_by_biome';

-- Populate
insert overwrite table smcveigh_birds_by_biome
select
  biomeName,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where biomeName is not null and species is not null
group by biomeName, species;

-- Test Query (Top 10 biomes)
select biomeName, sightings
from smcveigh_birds_by_biome
where species = 'Cardinalis cardinalis'
order by sightings desc
limit 10;

+--------------------------------+------------+
|           biomename            | sightings  |
+--------------------------------+------------+
| Piedmont                       | 42513      |
| Northern Piedmont              | 27540      |
| Southeastern Plains            | 21103      |
| Northeastern Coastal Zone      | 19635      |
| Southern Coastal Plain         | 17996      |
| Ridge and Valley               | 17406      |
| Eastern Great Lakes Lowlands   | 15161      |
| Eastern Corn Belt Plains       | 14676      |
| Middle Atlantic Coastal Plain  | 12779      |
| Interior Plateau               | 12324      |
+--------------------------------+------------+

-- Test Query (Top birds by biome)
select species, sightings
from smcveigh_birds_by_biome
where biomeName = 'Piedmont'
order by sightings desc
limit 10;

+---------------------------+------------+
|          species          | sightings  |
+---------------------------+------------+
| Cardinalis cardinalis     | 42513      |
| Poecile carolinensis      | 38677      |
| Baeolophus bicolor        | 36799      |
| Zenaida macroura          | 35358      |
| Thryothorus ludovicianus  | 29378      |
| Spinus tristis            | 28513      |
| Junco hyemalis            | 26239      |
| Haemorhous mexicanus      | 25738      |
| Melanerpes carolinus      | 25077      |
| Cyanocitta cristata       | 23700      |
+---------------------------+------------+

-- HBASE TABLE
-- First, need to create the hbase table in hbase shell
-- hbase shell
-- create 'smcveigh_birds_by_biome_hbase', 'info', 'stats'
-- exit
create external table smcveigh_birds_by_biome_hbase (
    biome_species string,
    biomeName string,
    species string,
    sightings integer)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:biomeName,info:species,stats:sightings')
TBLPROPERTIES ('hbase.table.name' = 'smcveigh_birds_by_biome_hbase');

insert overwrite table smcveigh_birds_by_biome_hbase
select
  concat(biomeName, '_', species) as state_species,
  biomeName,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where biomeName is not null and species is not null
group by biomeName, species;

-- Test Query
select sightings
from smcveigh_birds_by_biome_hbase
where biome_species = 'Piedmont_Cardinalis cardinalis';

+------------+
| sightings  |
+------------+
| 42513      |
+------------+

--------------------------------------------------------
-------- (3) Bird Sightings By State and Month ---------
--------------------------------------------------------

create external table smcveigh_birds_by_state_month (
    stateProvince string,
    month string,
    species string,
    sightings integer)
stored as orc
location '/user/hadoop/smcveigh/birds_by_state_month';

insert overwrite table smcveigh_birds_by_state_month
select
  stateProvince,
  cast(month as string) as month,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where stateProvince is not null and species is not null and month is not null
group by stateProvince, month, species;

-- Test Query
select month, sightings
from smcveigh_birds_by_state_month
where stateProvince = 'Illinois' -- and species = 'Cardinalis cardinalis'
-- 'Piranga olivacea' -- scarlet tanager, a common migratory bird
order by cast(month as int);

-- while doing this, I learned that the source csv I downloaded has all the months as February. Bummer!
-- I'll do year instead to see if birds got more or less common over the years

--------------------------------------------------------
-------- (4) Bird Sightings By State and Year ---------
--------------------------------------------------------

create external table smcveigh_birds_by_state_year (
    stateProvince string,
    year string,
    species string,
    sightings integer)
stored as orc
location '/user/hadoop/smcveigh/birds_by_state_year';

insert overwrite table smcveigh_birds_by_state_year
select
  stateProvince,
  cast(year as string) as year,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where stateProvince is not null and species is not null and year is not null
group by stateProvince, year, species;

-- Test Query
select year, sightings
from smcveigh_birds_by_state_year
where stateProvince = 'Illinois' and species = 'Cardinalis cardinalis'
-- 'Piranga olivacea' -- scarlet tanager, a common migratory bird
order by cast(year as int);

+-------+------------+
| year  | sightings  |
+-------+------------+
| 1998  | 160        |
| 1999  | 1167       |
| 2000  | 1444       |
| 2001  | 1000       |
| 2002  | 788        |
| 2003  | 973        |
| 2004  | 840        |
| 2005  | 1014       |
| 2006  | 1166       |
| 2007  | 1403       |
| 2008  | 1409       |
| 2009  | 1432       |
+-------+------------+

-- HBASE TABLE
-- First, need to create the hbase table in hbase shell
-- hbase shell
-- create 'smcveigh_birds_by_state_year_hbase', 'info', 'stats'
-- exit
-- drop table smcveigh_birds_by_state_year_hbase;
create external table smcveigh_birds_by_state_year_hbase (
    state_year string,
    stateProvince string,
    species string,
    sightings integer)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:stateProvince,info:species,stats:sightings')
TBLPROPERTIES ('hbase.table.name' = 'smcveigh_birds_by_state_year_hbase');

insert overwrite table smcveigh_birds_by_state_year_hbase
select
  concat(stateProvince, '_', cast(year as string), '_', species) as state_year,
  stateProvince,
  species,
  count(*) as sightings
from smcveigh_birds_and_biomes
where stateProvince is not null and species is not null and year is not null
group by stateProvince, year, species;

-- Test Query
select species, sightings, state_year
from smcveigh_birds_by_state_year_hbase
where state_year = 'Illinois_2008_Cardinalis cardinalis' limit 10;

+------------------------+------------+--------------------------------------+
|        species         | sightings  |              state_year              |
+------------------------+------------+--------------------------------------+
| Cardinalis cardinalis  | 1409       | Illinois_2008_Cardinalis cardinalis  |
+------------------------+------------+--------------------------------------+






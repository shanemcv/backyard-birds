-- This is for creating the 'scratch table' that the kafka speed layer will write to. 

-- The weather example given in course materials was
-- create 'latest_weather', 'weather'
-- This is done in Hbase shell on the cluster

-- So I'll use something like 
hbase shell
create 'smcveigh_new_birds', 'smcveigh_observations'

Created table smcveigh_new_birds
Took 2.3309 seconds                                                                                                                                                                           
=> Hbase::Table - smcveigh_new_birds
hbase:002:0> 

exit

-- also wanted a 'backup' that matched the 'info' and 'stats' from the Hbase tables I created
-- so also did the following
[hadoop@ip-172-31-91-77 ~]$ hbase shell
                                                                                                  
hbase:001:0> create 'smcveigh_bird_observations', 'info', 'stats'
Created table smcveigh_bird_observations
Took 1.3477 seconds                                                                                                                                                                           
=> Hbase::Table - smcveigh_bird_observations

-- ultimately decided to 'copy' the smcveigh_birds_by_state_hbase to a new table for the speed layer

-- HBASE TABLE
-- First, need to create the hbase table in hbase shell
-- hbase shell
-- create 'smcveigh_birds_by_state_speed', 'info', 'stats'
-- exit
create external table smcveigh_birds_by_state_speed (
    state_species string,
    stateProvince string,
    species string,
    sightings integer)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:stateProvince,info:species,stats:sightings')
TBLPROPERTIES ('hbase.table.name' = 'smcveigh_birds_by_state_speed');

insert overwrite table smcveigh_birds_by_state_speed
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
from smcveigh_birds_by_state_speed
where state_species = 'Illinois_Junco hyemalis';

+------------+
| sightings  |
+------------+
| 11339      |
+------------+

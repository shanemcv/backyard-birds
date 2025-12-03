-- DROP TABLE IF EXISTS smcveigh_biomes_csv;

create external table smcveigh_biomes_csv(
    lat float,
    lon float,
    NA_L3CODE string,
    NA_L3NAME string)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
    location '/user/hadoop/smcveigh/inputs/biomes' -- This should be the HDFS location (my project location is at /home/hadoop/smcveigh and hadoop is /smcveigh)
    TBLPROPERTIES ("skip.header.line.count"="1");

select * from smcveigh_biomes_csv limit 5;
select lat, lon, NA_L3CODE, NA_L3NAME FROM smcveigh_biomes_csv WHERE NA_L3CODE IS NOT NULL limit 10;

-- ORC Table

create table smcveigh_biomes(
    lat float,
    lon float,
    NA_L3CODE string,
    NA_L3NAME string)
    stored as orc;

insert overwrite table smcveigh_biomes
select
    lat,
    lon,
    NA_L3CODE,
    NA_L3NAME
from smcveigh_biomes_csv;


select lat, lon, NA_L3CODE, NA_L3NAME FROM smcveigh_biomes WHERE NA_L3CODE IS NOT NULL limit 50;
select lat, lon, NA_L3CODE, NA_L3NAME FROM smcveigh_biomes WHERE NA_L3NAME = 'Idaho Batholith' limit 50;
select lat, lon, NA_L3CODE, NA_L3NAME FROM smcveigh_biomes WHERE lat = 52.29 AND lon = -113.83 limit 5;


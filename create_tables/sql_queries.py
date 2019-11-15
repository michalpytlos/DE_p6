# DROP TABLES

air_quality_table_drop = "DROP TABLE IF EXISTS air_quality"
attribution_table_drop = "DROP TABLE IF EXISTS attributions"
city_table_drop = "DROP TABLE IF EXISTS cities"
location_table_drop = "DROP TABLE IF EXISTS locations"
source_table_drop = "DROP TABLE IF EXISTS sources"
zone_table_drop = "DROP TABLE IF EXISTS zones"
time_table_drop = "DROP TABLE IF EXISTS time"
staging_weather_station_table_drop = "DROP TABLE IF EXISTS staging_weather_stations"
weather_station_table_drop = "DROP TABLE IF EXISTS weather_stations"
staging_weather_table_drop = "DROP TABLE IF EXISTS staging_weather"
weather_table_drop = "DROP TABLE IF EXISTS weather"

# CREATE TABLES

air_quality_table_create = """
    CREATE TABLE public.air_quality (
        measurement_id bigint NOT NULL,
        parameter varchar(4) NOT NULL,
        value real NOT NULL,
        ts timestamp NOT NULL,
        zone_id varchar(32) NOT NULL,
        city_id bigint,
        attr_id bigint,
        source_id bigint,
        location_id bigint,
        PRIMARY KEY(measurement_id)
    )"""


attribution_table_create = """
    CREATE TABLE public.attributions (
        attr_id bigint NOT NULL,
        attr_name1 varchar(256),
        attr_url1 varchar(256),
        attr_name2 varchar(256),
        attr_url2 varchar(256),
        PRIMARY KEY(attr_id)
    )"""

city_table_create = """
    CREATE TABLE public.cities (
        city_id bigint NOT NULL,
        name varchar(256),
        country varchar(4),
        zone_id varchar(32),
        PRIMARY KEY(city_id)
    )"""

location_table_create = """
    CREATE TABLE public.locations (
        location_id bigint NOT NULL,
        name varchar(256),
        zone_id varchar(32),
        PRIMARY KEY(location_id)
    )"""

zone_table_create = """
    CREATE TABLE public.zones (
        zone_id varchar(32) NOT NULL,
        latitude numeric(3, 1),
        longitude numeric(4, 1),
        PRIMARY KEY(zone_id)
    )"""

source_table_create = """
    CREATE TABLE public.sources (
        source_id bigint NOT NULL,
        name varchar(256) NOT NULL,
        type varchar(256),
        PRIMARY KEY (source_id)
    )"""

time_table_create = """
    CREATE TABLE public."time" (
        ts timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        PRIMARY KEY (ts)
    )"""

staging_weather_station_table_create = """
    CREATE TABLE public.staging_weather_stations (
        station_id varchar(16) NOT NULL,
        latitude numeric(3, 1),
        longitude numeric(4, 1),
        elevation numeric(6, 1),
        state varchar(4),
        name varchar(32),
        gsn varchar(4),
        hcn_crn varchar(4),
        wmo_id int,
        PRIMARY KEY (station_id)
    )"""

weather_station_table_create = """
    CREATE TABLE public.weather_stations (
        station_id varchar(16) NOT NULL,
        latitude numeric(3, 1) NOT NULL,
        longitude numeric(4, 1) NOT NULL,
        elevation numeric(6, 1),
        state varchar(4),
        name varchar(32),
        gsn varchar(4),
        hcn_crn varchar(4),
        wmo_id int,
        zone_id varchar(32) NOT NULL,
        PRIMARY KEY (station_id)
    )"""

staging_weather_table_create = """
    CREATE TABLE public.staging_weather (
        station_id char(11) NOT NULL,
        date char(8) NOT NULL,
        element char(4) NOT NULL,
        value real NOT NULL,
        m_flag char(1),
        q_flag char(1),
        s_flag char(1),
        obs_time char(4),
        PRIMARY KEY (station_id, date, element)
    )"""

weather_table_create = """
    CREATE TABLE public.weather (
        station_id char(11) NOT NULL,
        ts timestamp NOT NULL,
        element char(4) NOT NULL,
        value real NOT NULL,
        zone_id varchar(32) NOT NULL,
        m_flag char(1),
        q_flag char(1),
        s_flag char(1),
        PRIMARY KEY (station_id, ts, element)
    )"""

# QUERY LISTS

create_table_queries = [
    air_quality_table_create,
    attribution_table_create,
    city_table_create,
    location_table_create,
    zone_table_create,
    source_table_create,
    time_table_create,
    staging_weather_station_table_create,
    weather_station_table_create,
    staging_weather_table_create,
    weather_table_create
]

drop_table_queries = [
    air_quality_table_drop,
    attribution_table_drop,
    city_table_drop,
    location_table_drop,
    zone_table_drop,
    source_table_drop,
    time_table_drop,
    staging_weather_station_table_drop,
    weather_station_table_drop,
    staging_weather_table_drop,
    weather_table_drop
]

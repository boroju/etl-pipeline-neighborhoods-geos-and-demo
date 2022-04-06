CREATE SCHEMA IF NOT EXISTS attomjetstream_stage;
drop table if exists attomjetstream_stage.neighborhoods_hist;
create external table attomjetstream_stage.neighborhoods_hist (
	created_datetime timestamp,
	geo_type string,
	neighborhood_type string,
	geo_id string,
	name string,
	state_abbr string,
	state_fips_code string,
	metro string,
	incorporated_place string,
	county string,
	county_fips_code string,
	longitude decimal(9,6),
	latitude decimal(9,6),
	geoloc string)
	COMMENT 'Attom Jetstream Neighborhoods Boundaries - one to one mapping of the raw .tsv files from Attom Jetstream. Stored as ORC files and partitioned by load_date corresponding to files for neighborhoods* ALL'
	PARTITIONED BY (load_date string)
	STORED AS ORC
	LOCATION '/data/stage/attom_jetstream/neighborhoods/';
MSCK REPAIR TABLE attomjetstream_stage.neighborhoods_hist;
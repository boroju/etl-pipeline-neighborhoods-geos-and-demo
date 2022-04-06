CREATE SCHEMA IF NOT EXISTS attomjetstream;
drop table if exists attomjetstream.neighborhoods;
create external table attomjetstream.neighborhoods (
	created_datetime timestamp,
	geo_type string,
	neighborhood_type string,
	geo_id string,
	name string,
	fips_code string,
	state_abbr string,
	state_fips_code string,
	metro string,
	incorporated_place string,
	county string,
	county_fips_code string,
	longitude decimal(9,6),
	latitude decimal(9,6),
	geoloc string
	)
	STORED AS ORC
    LOCATION '/data/curated/attom_jetstream/neighborhoods'
    TBLPROPERTIES('serialization.null.format'='');
MSCK REPAIR TABLE attomjetstream.neighborhoods;
USE [TPDLoad]
GO

SET ansi_nulls ON
GO
SET quoted_identifier ON
GO
-- =============================================
-- Author:		Boronat, Julian
-- Create date: 01-11-2022
-- Description:	Generate Geometries for Attom Jetstream - Neighborhoods
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[AttomJetstream_GenerateGeometries_sp]
AS
BEGIN

	SET NOCOUNT ON;

	-- #### CREATE table: AttomJetstream_Neighborhoods

	DROP TABLE IF EXISTS [TPDLoad].[dbo].[AttomJetstream_Neighborhoods];

	CREATE TABLE [TPDLoad].[dbo].[AttomJetstream_Neighborhoods](
		create_timestamp datetime not null default CURRENT_TIMESTAMP,
		geo_type varchar(18) not null,
		neighborhood_type varchar(20) not null,
		geo_id varchar(32) not null,
		name varchar(200),
		fips_code varchar(5) not null,
		state_abbr varchar(2),
		state_fips_code varchar(2),
		metro varchar(200),
		incorporated_place varchar(200),
		county varchar(100),
		county_fips_code varchar(3),
		longitude decimal(9,6),
		latitude decimal(9,6),
		geoloc geography
	) ON [PRIMARY] with (data_compression = page);

	-- #### CONVERT and INSERT Geometries in table: [TPDLoad].[dbo].[AttomJetstream_Neighborhoods]
  insert into
    TPDLoad.dbo.AttomJetstream_Neighborhoods
  (
    geo_type
  , neighborhood_type
  , geo_id
  , name
  , fips_code
  , state_abbr
  , state_fips_code
  , metro
  , incorporated_place
  , county
  , county_fips_code
  , longitude
  , latitude
  , geoloc
  )
  select  geo_type
        , neighborhood_type
        , geo_id
        , name
        , fips_code
        , state_abbr
        , state_fips_code
        , metro
        , incorporated_place
        , county
        , county_fips_code
        , longitude
        , latitude
        , geography::STGeomFromText(geoloc, 4326)
  from  TPDLoad.dbo.AttomJetstream_Neighborhoods_sqoop;

	-- #### REMOVE duplicates (geoloc repeated points) and generate FINAL table: [ThirdPartyData].[dbo].[Shp_Neighborhoods_dt]

	DROP TABLE IF EXISTS ThirdPartyData.dbo.Shp_Neighborhoods_dt

	;with neighborhoods as
	(
	  select
	      create_timestamp
			 ,geo_type
			 ,neighborhood_type
			 ,geo_id
			 ,name
			 ,fips_code
			 ,state_abbr
			 ,state_fips_code
			 ,metro
			 ,incorporated_place
			 ,county
			 ,county_fips_code
			 ,longitude
			 ,latitude
       ,iif(geoloc.MakeValid().EnvelopeAngle() >= 90, geoloc.MakeValid().ReorientObject(), geoloc.MakeValid()) as geoloc
	  from  [TPDLoad].[dbo].[AttomJetstream_Neighborhoods]
	)
	  select
			create_timestamp
			,geo_type
			,neighborhood_type
			,geo_id as id
			,name
			,fips_code
			,state_abbr
			,state_fips_code
			,county
			,county_fips_code
			,metro
			,incorporated_place
			,longitude
			,latitude
			,geoloc
			,case when 100.0 * geoloc.Reduce(500).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(500)
				when 100.0 * geoloc.Reduce(100).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(100)
				when 100.0 * geoloc.Reduce(50).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(50)
				when 100.0 * geoloc.Reduce(25).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(25)
				when 100.0 * geoloc.Reduce(10).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(10)
				when 100.0 * geoloc.Reduce(5).STArea() / geoloc.STArea() > 97.0 then geoloc.Reduce(5)
				else geoloc
			  end as geoloc_reduced
			,case when 100.0 * geoloc.Reduce(500).STArea() / geoloc.STArea() > 97.0 then 500
				when 100.0 * geoloc.Reduce(100).STArea() / geoloc.STArea() > 97.0 then 100
				when 100.0 * geoloc.Reduce(50).STArea() / geoloc.STArea() > 97.0 then 50
				when 100.0 * geoloc.Reduce(25).STArea() / geoloc.STArea() > 97.0 then 25
				when 100.0 * geoloc.Reduce(10).STArea() / geoloc.STArea() > 97.0 then 10
				when 100.0 * geoloc.Reduce(5).STArea() / geoloc.STArea() > 97.0 then 5
				else 1
			end as geoloc_reduced_level
	into  ThirdPartyData.dbo.Shp_Neighborhoods_dt
	from  neighborhoods;

	-- #### DROP previous tables

	DROP TABLE IF EXISTS [ThirdPartyData].[dbo].[AttomJetstream_Neighborhoods_sqoop];
	DROP TABLE IF EXISTS [ThirdPartyData].[dbo].[AttomJetstream_Neighborhoods];
	DROP TABLE IF EXISTS [TPDLoad].[dbo].[AttomJetstream_Neighborhoods_sqoop];
	DROP TABLE IF EXISTS [TPDLoad].[dbo].[AttomJetstream_Neighborhoods];

	-- #### CREATE keys and indexes in FINAL table: [ThirdPartyData].[dbo].[Shp_Neighborhoods_dt]

	ALTER TABLE ThirdPartyData.dbo.Shp_Neighborhoods_dt ALTER COLUMN id varchar(32) not null;
	ALTER TABLE ThirdPartyData.dbo.Shp_Neighborhoods_dt ALTER COLUMN fips_code varchar(5) not null;

    ALTER TABLE [ThirdPartyData].[dbo].[Shp_Neighborhoods_dt]
    ADD CONSTRAINT PK_Shp_Neighborhoods_dt_FipsCode_Id
    PRIMARY KEY CLUSTERED (fips_code asc, id asc)
	with
	 (
		data_compression = page
		,fillfactor = 95
		,allow_row_locks = on
		,allow_page_locks = on
		,pad_index = off
		,statistics_norecompute = off
		,online = off
		,ignore_dup_key = off
	);

    CREATE SPATIAL INDEX SIndx_Shp_Neighborhoods_dt_geography_geoloc
    ON [ThirdPartyData].[dbo].[Shp_Neighborhoods_dt](geoloc)
	using geography_grid
	with
	(
		grids = (level_1 = high, level_2 = high, level_3 = high, level_4 = high)
	  ,cells_per_object = 2
	  ,pad_index = off
	  ,statistics_norecompute = off
	  ,sort_in_tempdb = off
	  ,drop_existing = off
	  ,online = off
	  ,allow_row_locks = on
	  ,allow_page_locks = on
	  ,fillfactor = 95
	);

    CREATE SPATIAL INDEX SIndx_Shp_Neighborhoods_dt_geography_geoloc_reduced
    ON [ThirdPartyData].[dbo].[Shp_Neighborhoods_dt](geoloc_reduced)
	using geography_grid
	with
	(
		grids = (level_1 = high, level_2 = high, level_3 = high, level_4 = high)
	  ,cells_per_object = 2
	  ,pad_index = off
	  ,statistics_norecompute = off
	  ,sort_in_tempdb = off
	  ,drop_existing = off
	  ,online = off
	  ,allow_row_locks = on
	  ,allow_page_locks = on
	  ,fillfactor = 95
	);

END;
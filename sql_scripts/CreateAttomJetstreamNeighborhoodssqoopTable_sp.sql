USE [TPDLoad]
GO

/******* Object:  StoredProcedure [dbo].[AttomJetstream_CreateTablesForSqoop_sp]    Script Date: 1/11/2022 6:57:53 PM *******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Boronat, Julian
-- Create date: 01-06-2022
-- Description:	Drops/creates AttomJetstream Neighborhoods table that is sqooped from HDFS
-- =============================================
CREATE OR ALTER PROCEDURE [dbo].[AttomJetstream_CreateTablesForSqoop_sp]
AS
BEGIN

	SET NOCOUNT ON;


	-- #### AttomJetstream_Neighborhoods_sqoop

	DROP TABLE IF EXISTS [dbo].[AttomJetstream_Neighborhoods_sqoop]

	CREATE TABLE [dbo].[AttomJetstream_Neighborhoods_sqoop](
		created_datetime datetime not null default CURRENT_TIMESTAMP,
		geo_type varchar(18) not null,
		neighborhood_type varchar(20) not null,
		geo_id varchar(32) not null,
		name varchar(200),
		fips_code varchar(5),
		state_abbr varchar(2),
		state_fips_code varchar(2),
		metro varchar(200),
		incorporated_place varchar(200),
		county varchar(100),
		county_fips_code varchar(3),
		longitude decimal(9,6),
		latitude decimal(9,6),
		geoloc varchar(max)
	) ON [PRIMARY] with (data_compression = page)

END
GO

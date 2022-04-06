sqoop eval --connect "jdbc:jtds:sqlserver://$1;useNTLMv2=true;domain=CORP.AMHERST.COM;databaseName=TPDLoad;" \
--username $2 --password $3 \
--query "TRUNCATE TABLE dbo.AttomJetstream_Neighborhoods_sqoop"

sqoop export -Dmapreduce.map.java.opts=' -Duser.timezone=UTC' --connect "jdbc:jtds:sqlserver://$1;useNTLMv2=true;domain=CORP.AMHERST.COM;databaseName=TPDLoad;" \
--username $2 --password $3 \
--table 'AttomJetstream_Neighborhoods_sqoop' \
--hcatalog-database 'attomjetstream' \
--hcatalog-table 'neighborhoods' \
--columns 'geo_type, neighborhood_type, geo_id, name, fips_code, state_abbr, state_fips_code, metro, incorporated_place, county, county_fips_code, longitude, latitude, geoloc' \
--map-column-java 'geo_type=String,neighborhood_type=String,geo_id=String,name=String,fips_code=String,state_abbr=String,state_fips_code=String,metro=String,incorporated_place=String,county=String,county_fips_code=String,geoloc=String' \
--validate \
-m 2
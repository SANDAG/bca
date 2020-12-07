-- Create dimension schema
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name='dimension')
EXEC ('CREATE SCHEMA [dimension]')
GO

-- Add metadata for [dimension]
IF EXISTS(SELECT * FROM [db_meta].[data_dictionary] WHERE [ObjectType] = 'SCHEMA' AND [FullObjectName] = '[dimension]' AND [PropertyName] = 'MS_Description')
EXECUTE [db_meta].[drop_xp] 'dimension', 'MS_Description'
GO
EXECUTE [db_meta].[add_xp] 'dimension', 'MS_Description', 'schema to hold and manage ABM dimension tables and views'
GO

-- Create fact schema
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name='fact')
EXEC ('CREATE SCHEMA [fact]')
GO

-- Add metadata for [fact]
IF EXISTS(SELECT * FROM [db_meta].[data_dictionary] WHERE [ObjectType] = 'SCHEMA' AND [FullObjectName] = '[fact]' AND [PropertyName] = 'MS_Description')
EXECUTE [db_meta].[drop_xp] 'fact', 'MS_Description'
GO
EXECUTE [db_meta].[add_xp] 'fact', 'MS_Description', 'schema to hold and manage ABM fact tables'
GO


-- Create synonyms for all objects used in the bca tool that reside
-- in the abm database
-- removes dependency of bca tool on the abm database name
-- dependency on abm database structure remains
IF OBJECT_ID('dimension.geography_household_location', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[geography_household_location]
GO
CREATE SYNONYM [dimension].[geography_household_location] FOR $(abm_db_name).[dimension].[geography_household_location]
GO

IF OBJECT_ID('dimension.geography_trip_destination', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[geography_trip_destination]
GO
CREATE SYNONYM [dimension].[geography_trip_destination] FOR $(abm_db_name).[dimension].[geography_trip_destination]
GO

IF OBJECT_ID('dimension.geography_trip_origin', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[geography_trip_origin]
GO
CREATE SYNONYM [dimension].[geography_trip_origin] FOR $(abm_db_name).[dimension].[geography_trip_origin]
GO

IF OBJECT_ID('dimension.household', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[household]
GO
CREATE SYNONYM [dimension].[household] FOR $(abm_db_name).[dimension].[household]
GO

IF OBJECT_ID('dimension.hwy_link', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[hwy_link]
GO
CREATE SYNONYM [dimension].[hwy_link] FOR $(abm_db_name).[dimension].[hwy_link]
GO

IF OBJECT_ID('dimension.hwy_link_ab_tod', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[hwy_link_ab_tod]
GO
CREATE SYNONYM [dimension].[hwy_link_ab_tod] FOR $(abm_db_name).[dimension].[hwy_link_ab_tod]
GO

IF OBJECT_ID('fact.hwy_flow', 'sn') IS NOT NULL
	DROP SYNONYM [fact].[hwy_flow]
GO
CREATE SYNONYM [fact].[hwy_flow] FOR $(abm_db_name).[fact].[hwy_flow]
GO

IF OBJECT_ID('fact.hwy_flow_mode', 'sn') IS NOT NULL
	DROP SYNONYM [fact].[hwy_flow_mode]
GO
CREATE SYNONYM [fact].[hwy_flow_mode] FOR $(abm_db_name).[fact].[hwy_flow_mode]
GO

IF OBJECT_ID('dimension.mode', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[mode]
GO
CREATE SYNONYM [dimension].[mode] FOR $(abm_db_name).[dimension].[mode]
GO

IF OBJECT_ID('dimension.mode_tour', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[mode_tour]
GO
CREATE SYNONYM [dimension].[mode_tour] FOR $(abm_db_name).[dimension].[mode_tour]
GO

IF OBJECT_ID('dimension.mode_trip', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[mode_trip]
GO
CREATE SYNONYM [dimension].[mode_trip] FOR $(abm_db_name).[dimension].[mode_trip]
GO

IF OBJECT_ID('dimension.model_trip', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[model_trip]
GO
CREATE SYNONYM [dimension].[model_trip] FOR $(abm_db_name).[dimension].[model_trip]
GO

IF OBJECT_ID('dimension.person', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[person]
GO
CREATE SYNONYM [dimension].[person] FOR $(abm_db_name).[dimension].[person]
GO

IF OBJECT_ID('dimension.time_trip_start', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[time_trip_start]
GO
CREATE SYNONYM [dimension].[time_trip_start] FOR $(abm_db_name).[dimension].[time_trip_start]
GO

IF OBJECT_ID('dimension.tour', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[tour]
GO
CREATE SYNONYM [dimension].[tour] FOR $(abm_db_name).[dimension].[tour]
GO

IF OBJECT_ID('fact.person_trip', 'sn') IS NOT NULL
	DROP SYNONYM [fact].[person_trip]
GO
CREATE SYNONYM [fact].[person_trip] FOR $(abm_db_name).[fact].[person_trip]
GO

IF OBJECT_ID('dimension.purpose_tour', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[purpose_tour]
GO
CREATE SYNONYM [dimension].[purpose_tour] FOR $(abm_db_name).[dimension].[purpose_tour]
GO

IF OBJECT_ID('dimension.purpose_trip_destination', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[purpose_trip_destination]
GO
CREATE SYNONYM [dimension].[purpose_trip_destination] FOR $(abm_db_name).[dimension].[purpose_trip_destination]
GO

IF OBJECT_ID('dimension.purpose_trip_origin', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[purpose_trip_origin]
GO
CREATE SYNONYM [dimension].[purpose_trip_origin] FOR $(abm_db_name).[dimension].[purpose_trip_origin]
GO

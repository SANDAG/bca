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
IF OBJECT_ID('dimension.household', 'sn') IS NOT NULL
	DROP SYNONYM [dimension].[household]
GO
CREATE SYNONYM [dimension].[household] FOR $(abm_db_name).[dimension].[household]
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

IF OBJECT_ID('fact.person_trip', 'sn') IS NOT NULL
	DROP SYNONYM [fact].[person_trip]
GO
CREATE SYNONYM [fact].[person_trip] FOR $(abm_db_name).[fact].[person_trip]
GO

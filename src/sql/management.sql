-- Create bca schema
IF NOT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name='bca')
EXEC ('CREATE SCHEMA [bca]')
GO

-- Add metadata for [bca]
IF EXISTS(SELECT * FROM [db_meta].[data_dictionary] WHERE [ObjectType] = 'SCHEMA' AND [FullObjectName] = '[bca]' AND [PropertyName] = 'MS_Description')
EXECUTE [db_meta].[drop_xp] 'bca', 'MS_Description'
GO
EXECUTE [db_meta].[add_xp] 'bca', 'MS_Description', 'schema to hold and manage all bca tool related objects'
GO

-- create bca_user role if it does not exist
-- grant read/execute permissions to bca_user role on bca schema
IF DATABASE_PRINCIPAL_ID('bca_user') IS NULL
BEGIN
	CREATE ROLE [bca_user]
	GRANT CONNECT TO [bca_user]
	EXEC sp_addrolemember [db_datareader], [bca_user]
	GRANT SELECT ON OBJECT:: [db_meta].[data_dictionary] TO [bca_user]
	GRANT SELECT ON OBJECT:: [db_meta].[schema_change_log] TO [bca_user]
	GRANT EXECUTE ON SCHEMA :: [bca] TO [bca_user]
	GRANT SELECT ON SCHEMA :: [bca] TO [bca_user]
	GRANT VIEW DEFINITION ON SCHEMA :: [bca] TO [bca_user]
END
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


-- Create aggregate_toll table valued function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bca].[fn_toll_cost]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [bca].[fn_toll_cost]
GO

CREATE FUNCTION [bca].[fn_toll_cost]
(
	@scenario_id_base integer,
	@scenario_id_build integer
)
RETURNS @tbl_toll_cost TABLE 
(
	[scenario_id] integer NOT NULL
	,[model_trip_description] nchar(20) NOT NULL
	,[toll_cost_drive] float NOT NULL
	,PRIMARY KEY ([scenario_id], [model_trip_description])
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 6/29/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database:
--	[dbo].[run_aggregate_toll_comparison]
--	[dbo].[run_aggregate_toll_processor]
-- ===========================================================================
BEGIN

	INSERT INTO @tbl_toll_cost
	SELECT
		[scenario_id]
		,[model_trip].[model_trip_description]
		,SUM([toll_cost_drive]) AS [toll_cost_drive]
	FROM
		[fact].[person_trip]
	INNER JOIN
		[dimension].[model_trip]
	ON
		[person_trip].[model_trip_id] = [model_trip].[model_trip_id]
	WHERE
		[scenario_id] = @scenario_id_base
		OR [scenario_id] = @scenario_id_build
	GROUP BY
		[scenario_id]
		,[model_trip].[model_trip_description]
	RETURN 
END
GO

-- Add metadata for [bca].[fn_toll_cost]
EXECUTE [db_meta].[add_xp] 'bca.fn_toll_cost', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_toll_cost', 'MS_Description', 'function to return total toll costs in dollars by scenario and abm sub-model'
GO
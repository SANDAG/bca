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




-- Create auto_ownership table valued function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bca].[fn_auto_ownership]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [bca].[fn_auto_ownership]
GO

CREATE FUNCTION [bca].[fn_auto_ownership]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@auto_ownership_cost float
)
RETURNS @tbl_auto_ownership TABLE
(
	[base_cost_auto_ownership] float NOT NULL
	,[build_cost_auto_ownership] float NOT NULL
	,[difference_auto_ownership] float NOT NULL
	,[difference_auto_ownership_coc] float NOT NULL
	,[benefits_auto_ownership] float NOT NULL
	,[benefits_auto_ownership_coc] float NOT NULL
	,[benefits_auto_ownership_senior] float NOT NULL
	,[benefits_auto_ownership_minority] float NOT NULL
	,[benefits_auto_ownership_low_income] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 6/30/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns table of total auto ownership, costs, and differences between the
-- base and build scenario for total households, Community of Concern
-- households, and each element that indicates a Community of Concern
-- household (seniors, minorities, low income).
--	[dbo].[auto_ownership_comparison]
--	[dbo].[auto_ownership_processor]
--	[dbo].[auto_ownership_summary]
-- ===========================================================================
BEGIN
	with [households] AS (
		SELECT
			[person].[scenario_id]
			,[person].[household_id]
			,MAX(CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END) AS [senior]
			,MAX(CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												 'Asian Alone',
												 'Black or African American Alone',
												 'Two or More Major Race Groups',
												 'Native Hawaiian and Other Pacific Islander Alone',
												 'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							 OR [person].[hispanic] = 'Hispanic' THEN 1
						 ELSE 0 END) AS [minority]
			,MAX(CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END) AS [low_income]
			-- note the autos field contains the 4+ category, set to 4 until better estimate of 4+ group can be obtained
			,MAX(CASE	WHEN [household].[autos] = '4+' THEN 4 ELSE CONVERT(integer, [household].[autos]) END) AS [autos]
			,MAX([household].[weight_household]) AS [weight_household]
		FROM
			[dimension].[person]
		INNER JOIN
			[dimension].[household]
		ON
			[person].[scenario_id] = [household].[scenario_id]
			AND [person].[household_id] = [household].[household_id]
		WHERE
			[person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [person].[weight_person] > 0
			AND [household].[weight_household] > 0
		GROUP BY
			[person].[scenario_id]
			,[person].[household_id]),
	[households_summary] AS (
		SELECT
			[scenario_id]
			,SUM([weight_household] * [autos]) AS [autos]
			,SUM(CASE WHEN [senior] = 1 OR [minority] = 1 OR [low_income] = 1 THEN [weight_household] * [autos] ELSE 0 END) AS [autos_coc_hh]
			,SUM(CASE WHEN [senior] = 1 THEN [weight_household] * [autos] ELSE 0 END) AS [autos_senior_hh]
			,SUM(CASE WHEN [minority] = 1 THEN [weight_household] * [autos] ELSE 0 END) AS [autos_minority_hh]
			,SUM(CASE WHEN [low_income] = 1 THEN [weight_household] * [autos] ELSE 0 END) AS [autos_low_income_hh]
		FROM
			[households]
		GROUP BY
			[scenario_id]),
	[households_summary_wide] AS (
		SELECT
			SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [autos] ELSE 0 END) AS [base_autos]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [autos] ELSE 0 END) AS [build_autos]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [autos_coc_hh] ELSE 0 END) AS [base_autos_coc_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [autos_coc_hh] ELSE 0 END) AS [build_autos_coc_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [autos_senior_hh] ELSE 0 END) AS [base_autos_senior_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [autos_senior_hh] ELSE 0 END) AS [build_autos_senior_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [autos_minority_hh] ELSE 0 END) AS [base_autos_minority_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [autos_minority_hh] ELSE 0 END) AS [build_autos_minority_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [autos_low_income_hh] ELSE 0 END) AS [base_autos_low_income_hh]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [autos_low_income_hh] ELSE 0 END) AS [build_autos_low_income_hh]
		FROM
			[households_summary])
	INSERT INTO @tbl_auto_ownership
	SELECT
		[base_autos] * @auto_ownership_cost AS [base_cost_auto_ownership]
		,[build_autos] * @auto_ownership_cost AS [build_cost_auto_ownership]
		,[base_autos] - [build_autos] AS [difference_auto_ownership]
		,[base_autos_coc_hh] - [build_autos_coc_hh] AS [difference_auto_ownership_coc]
		,([base_autos] - [build_autos]) * @auto_ownership_cost AS [benefits_auto_ownership]
		,([base_autos_coc_hh] - [build_autos_coc_hh]) * @auto_ownership_cost AS [benefits_auto_ownership_coc]
		,([base_autos_senior_hh] - [build_autos_senior_hh]) * @auto_ownership_cost AS [benefits_auto_ownership_senior]
		,([base_autos_minority_hh] - [build_autos_minority_hh]) * @auto_ownership_cost AS [benefits_auto_ownership_minority]
		,([base_autos_low_income_hh] - [build_autos_low_income_hh]) * @auto_ownership_cost AS [benefits_auto_ownership_low_income]
	FROM
		[households_summary_wide]
  RETURN
END
GO

-- Add metadata for [bca].[fn_auto_ownership]
EXECUTE [db_meta].[add_xp] 'bca.fn_auto_ownership', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_auto_ownership', 'MS_Description', 'function to return auto ownership results for base and build scenarios'
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
	[base_toll_ctm_model] float NOT NULL
	,[build_toll_ctm_model] float NOT NULL
	,[base_toll_truck_model] float NOT NULL
	,[build_toll_truck_model] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 6/29/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns table of total auto mode toll costs segmented by base and build
-- scenarios for the CTM and Truck ABM sub-models.
--	[dbo].[run_aggregate_toll_comparison]
--	[dbo].[run_aggregate_toll_processor]
-- ===========================================================================
BEGIN

	with [toll_costs] AS (
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
		[scenario_id] IN (@scenario_id_base, @scenario_id_build)
	GROUP BY
		[scenario_id]
		,[model_trip].[model_trip_description])
	INSERT INTO @tbl_toll_cost
	SELECT
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [toll_cost_drive] ELSE 0 END) AS [base_toll_ctm_model]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [toll_cost_drive] ELSE 0 END) AS [build_toll_ctm_model]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [model_trip_description] = 'Truck'
					THEN [toll_cost_drive] ELSE 0 END) AS [base_toll_truck_model]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Truck'
					THEN [toll_cost_drive] ELSE 0 END) AS [build_toll_truck_model]
	FROM
		toll_costs
	RETURN 
END
GO

-- Add metadata for [bca].[fn_toll_cost]
EXECUTE [db_meta].[add_xp] 'bca.fn_toll_cost', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_toll_cost', 'MS_Description', 'function to return toll cost results for base and build scenarios'
GO
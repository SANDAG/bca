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
	@auto_ownership_cost float -- bca auto ownership cost parameter
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
--	[dbo].[run_auto_ownership_comparison]
--	[dbo].[run_auto_ownership_processor]
--	[dbo].[run_auto_ownership_summary]
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




-- Create demographics table valued function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bca].[fn_demographics]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [bca].[fn_demographics]
GO

CREATE FUNCTION [bca].[fn_demographics]
(
	@scenario_id_base integer,
	@scenario_id_build integer
)
RETURNS @tbl_demographics TABLE
(
	[base_persons] integer NOT NULL
	,[build_persons] integer NOT NULL
	,[base_persons_coc] integer NOT NULL
	,[build_persons_coc] integer NOT NULL
	,[base_persons_senior] integer NOT NULL
	,[build_persons_senior] integer NOT NULL
	,[base_persons_minority] integer NOT NULL
	,[build_persons_minority] integer NOT NULL
	,[base_persons_low_income] integer NOT NULL
	,[build_persons_low_income] integer NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/2/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns table of total base and build scenario persons and total persons
-- within each Community of Concern (seniors, minorities, low income).
--	[dbo].[run_demographics_processor]
--	[dbo].[run_demographics_summary]
-- ===========================================================================
BEGIN

with [person_summary] AS (
	SELECT
		[person].[scenario_id]
		,SUM([person].[weight_person]) AS [persons]
		,SUM(CASE	WHEN [person].[age] >= 75 THEN [person].[weight_person]
					WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN [person].[weight_person]
					WHEN [household].[poverty] <= 2 THEN [person].[weight_person]
					ELSE 0 END) AS [persons_coc]
		,SUM(CASE WHEN [person].[age] >= 75 THEN [person].[weight_person] ELSE 0 END) AS [persons_senior]
		,SUM(CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN [person].[weight_person]
						ELSE 0 END) AS [persons_minority]
		,SUM(CASE WHEN [household].[poverty] <= 2 THEN [person].[weight_person] ELSE 0 END) AS [persons_low_income]
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
		[person].[scenario_id])
INSERT INTO @tbl_demographics
SELECT
	SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [persons] ELSE 0 END) AS [base_persons]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [persons] ELSE 0 END) AS [build_persons]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [persons_coc] ELSE 0 END) AS [base_persons_coc]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [persons_coc] ELSE 0 END) AS [build_persons_coc]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [persons_senior] ELSE 0 END) AS [base_persons_senior]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [persons_senior] ELSE 0 END) AS [build_persons_senior]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [persons_minority] ELSE 0 END) AS [base_persons_minority]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [persons_minority] ELSE 0 END) AS [build_persons_minority]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [persons_low_income] ELSE 0 END) AS [base_persons_low_income]
	,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [persons_low_income] ELSE 0 END) AS [build_persons_low_income]
FROM
	[person_summary]
  RETURN
END
GO

-- Add metadata for [bca].[fn_demographics]
EXECUTE [db_meta].[add_xp] 'bca.fn_demographics', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_demographics', 'MS_Description', 'function to return demographic results for base and build scenarios'
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




-- Create physical_activity table valued function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bca].[fn_physical_activity]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [bca].[fn_physical_activity]
GO

CREATE FUNCTION [bca].[fn_physical_activity]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@activity_threshold float, -- bca minimum minutes to define person as physically active parameter
	@activity_benefit float -- bca physically active person benefit parameter
)
RETURNS @tbl_physical_activity TABLE
(
	[base_active_persons] integer NOT NULL
	,[build_active_persons] integer NOT NULL
	,[diff_active_persons] integer NOT NULL
	,[benefit_active_persons] float NOT NULL
	,[diff_active_persons_coc] integer NOT NULL
	,[benefit_active_persons_coc] float NOT NULL
	,[diff_active_persons_senior] integer NOT NULL
	,[benefit_active_persons_senior] float NOT NULL
	,[diff_active_persons_minority] integer NOT NULL
	,[benefit_active_persons_minority] float NOT NULL
	,[diff_active_persons_low_income] integer NOT NULL
	,[benefit_active_persons_low_income] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/02/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- a threshold for minimum physical activity to define a person as active,
-- and the benefit of a person being physically active then returns table of
-- total physically active persons for the base and build scenarios by
-- Community of Concern and each element that indicates a Community of Concern
-- person (seniors, minorities, low income). Differences and benefits between
-- two base and build scenarios are calculated as well.
--	[dbo].[run_physical_activity_comparison]
--	[dbo].[run_physical_activity_processor]
--	[dbo].[run_physical_activity_summary]
-- ===========================================================================
BEGIN
	with [active_persons] AS (
		SELECT
			[person_trip].[scenario_id]
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
			,MAX([person].[weight_person]) AS [weight_person]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[person]
		ON
			[person_trip].[scenario_id] = [person].[scenario_id]
			AND [person_trip].[person_id] = [person].[person_id]
		INNER JOIN
			[dimension].[household]
		ON
			[person_trip].[scenario_id] = [household].[scenario_id]
			AND [person_trip].[household_id] = [household].[household_id]
		WHERE
			[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [person].[weight_person] > 0
			AND [household].[weight_household] > 0
		GROUP BY
			[person_trip].[scenario_id]
			,[person_trip].[person_id]
		HAVING
			SUM([person_trip].[time_walk] + [person_trip].[time_bike]) > @activity_threshold),
	[active_persons_summary] AS (
		SELECT
			[scenario_id]
			,SUM([weight_person]) AS [active_persons]
			,SUM(CASE WHEN [senior] = 1 OR [minority] = 1 OR [low_income] = 1 THEN [weight_person] ELSE 0 END) AS [active_persons_coc]
			,SUM(CASE WHEN [senior] = 1 THEN [weight_person] ELSE 0 END) AS [active_persons_senior]
			,SUM(CASE WHEN [minority] = 1 THEN [weight_person] ELSE 0 END) AS [active_persons_minority]
			,SUM(CASE WHEN [low_income] = 1 THEN [weight_person] ELSE 0 END) AS [active_persons_low_income]
		FROM
			[active_persons]
		GROUP BY
			[scenario_id]),
	[scenario_summary] AS (
		SELECT
			SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [active_persons] ELSE 0 END) AS [base_active_persons]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [active_persons] ELSE 0 END) AS [build_active_persons]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [active_persons_coc] ELSE 0 END) AS [base_active_persons_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [active_persons_coc] ELSE 0 END) AS [build_active_persons_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [active_persons_senior] ELSE 0 END) AS [base_active_persons_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [active_persons_senior] ELSE 0 END) AS [build_active_persons_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [active_persons_minority] ELSE 0 END) AS [base_active_persons_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [active_persons_minority] ELSE 0 END) AS [build_active_persons_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [active_persons_low_income] ELSE 0 END) AS [base_active_persons_low_income]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [active_persons_low_income] ELSE 0 END) AS [build_active_persons_low_income]
		FROM
			[active_persons_summary])
	INSERT INTO @tbl_physical_activity
	SELECT
		[base_active_persons]
		,[build_active_persons]
		,[build_active_persons] - [base_active_persons] AS [diff_active_persons]
		,([build_active_persons] - [base_active_persons]) * @activity_benefit AS [benefit_active_persons]
		,[build_active_persons_coc] - [base_active_persons_coc] AS [diff_active_persons_coc]
		,([build_active_persons_coc] - [base_active_persons_coc]) * @activity_benefit AS [benefit_active_persons_coc]
		,[build_active_persons_senior] - [base_active_persons_senior] AS [diff_active_persons_senior]
		,([build_active_persons_senior] - [base_active_persons_senior]) * @activity_benefit AS [benefit_active_persons_senior]
		,[build_active_persons_minority] - [base_active_persons_minority] AS [diff_active_persons_minority]
		,([build_active_persons_minority] - [base_active_persons_minority]) * @activity_benefit AS [benefit_active_persons_minority]
		,[build_active_persons_low_income] - [base_active_persons_low_income] AS [diff_active_persons_low_income]
		,([build_active_persons_low_income] - [base_active_persons_low_income]) * @activity_benefit AS [benefit_active_persons_low_income]
	FROM
		[scenario_summary]
	RETURN
END
GO

-- Add metadata for [bca].[fn_physical_activity]
EXECUTE [db_meta].[add_xp] 'bca.fn_physical_activity', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_physical_activity', 'MS_Description', 'function to return person physical activity results for base and build scenarios'
GO
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




-- Create highway link table valued function
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bca].[fn_highway_link]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
DROP FUNCTION [bca].[fn_highway_link]
GO

CREATE FUNCTION [bca].[fn_highway_link]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@reliability_ratio float, -- TODO requires comment
	@crash_cost_pdo float, -- TODO requires comment
	@crash_cost_injury float, -- TODO requires comment
	@crash_cost_fatal float, -- TODO requires comment
	@crash_rate_pdo float, -- TODO requires comment
	@crash_rate_injury float, -- TODO requires comment
	@crash_rate_fatal float, -- TODO requires comment
	@voc_auto float, -- TODO requires comment
	@voc_lhdt float, -- TODO requires comment
	@voc_mhdt float, -- TODO requires comment
	@voc_hhdt float, -- TODO requires comment
	@vor_auto float, -- TODO requires comment
	@vor_lhdt float, -- TODO requires comment
	@vor_mhdt float, -- TODO requires comment
	@vor_hhdt float -- TODO requires comment
)
RETURNS @tbl_highway_link TABLE
(
	[cost_change_op_auto] float NOT NULL,
	[cost_change_op_lhdt] float NOT NULL,
	[cost_change_op_mhdt] float NOT NULL,
	[cost_change_op_hhdt] float NOT NULL,
	[cost_change_rel_auto] float NOT NULL,
	[cost_change_rel_lhdt] float NOT NULL,
	[cost_change_rel_mhdt] float NOT NULL,
	[cost_change_rel_hhdt] float NOT NULL,
	[cost_change_crashes_pdo] float NOT NULL,
	[cost_change_crashes_injury] float NOT NULL,
	[cost_change_crashes_fatal] float NOT NULL,
	[base_cost_rel] float NOT NULL,
	[build_cost_rel] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/09/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns table of (TODO of what?) segmented by base and build
-- scenarios loaded highway network.
--	[dbo].[run_link_comparison]
--	[dbo].[run_link_processor]
--	[dbo].[run_link_summary]
-- ===========================================================================
BEGIN

	-- get link volumes by ab and tod for autos and trucks (lht, mht, hht)
	-- calculate link free flow speed by ab and tod
	-- begin caluclation of link vehicle delay per mile by ab and tod
	with [flow_ab_tod] AS (
		SELECT
			[hwy_flow].[scenario_id]
			,CASE	WHEN 1.0274 * POWER([hwy_flow].[time] / ([hwy_link_ab_tod].[tm]), 1.2204) > 3.0
					THEN 3.0
					ELSE 1.0274 * POWER([hwy_flow].[time] / ([hwy_link_ab_tod].[tm]), 1.2204)
					END AS [ttim2] -- begin caluclation of link vehicle delay per mile by ab and tod
			,[hwy_link].[length_mile] / ([hwy_link_ab_tod].[tm] / 60) AS [speed_free_flow]
			,[hwy_flow].[flow] * [hwy_link].[length_mile] AS [vmt_total]
			,[hwy_flow_mode_agg].[flow_auto] * [hwy_link].[length_mile] AS [vmt_auto]
			,[hwy_flow_mode_agg].[flow_lhdt] * [hwy_link].[length_mile] AS [vmt_lhdt]
			,[hwy_flow_mode_agg].[flow_mhdt] * [hwy_link].[length_mile] AS [vmt_mhdt]
			,[hwy_flow_mode_agg].[flow_hhdt] * [hwy_link].[length_mile] AS [vmt_hhdt]

		FROM
			[fact].[hwy_flow]
		INNER JOIN
			[dimension].[hwy_link]
		ON
			[hwy_flow].[scenario_id] = [hwy_link].[scenario_id]
			AND [hwy_flow].[hwy_link_id] = [hwy_link].[hwy_link_id]
		INNER JOIN
			[dimension].[hwy_link_ab_tod]
		ON
			[hwy_flow].[scenario_id] = [hwy_link_ab_tod].[scenario_id]
			AND [hwy_flow].[hwy_link_ab_tod_id] = [hwy_link_ab_tod].[hwy_link_ab_tod_id]
		INNER JOIN (
			SELECT
				[scenario_id]
				,[hwy_link_ab_tod_id]
				,SUM(CASE	WHEN [mode].[mode_description] IN ('Drive Alone Non-Toll',
															   'Drive Alone Toll Eligible',
															   'Shared Ride 2 Non-Toll',
															   'Shared Ride 2 Toll Eligible',
															   'Shared Ride 3 Non-Toll',
															   'Shared Ride 3 Toll Eligible')
							THEN [flow]
							ELSE 0 END) AS [flow_auto]
				,SUM(CASE	WHEN [mode].[mode_description] IN ('Light Heavy Duty Truck (Non-Toll)',
															   'Light Heavy Duty Truck (Toll)')
							THEN [flow]
							ELSE 0 END) AS [flow_lhdt]
				,SUM(CASE	WHEN [mode].[mode_description] IN ('Medium Heavy Duty Truck (Non-Toll)',
															   'Medium Heavy Duty Truck (Toll)')
							THEN [flow]
							ELSE 0 END) AS [flow_mhdt]
				,SUM(CASE	WHEN [mode].[mode_description] IN ('Heavy Heavy Duty Truck (Non-Toll)',
															   'Heavy Heavy Duty Truck (Toll)')
							THEN [flow]
							ELSE 0 END) AS [flow_hhdt]
			FROM
				[fact].[hwy_flow_mode]
			INNER JOIN
				[dimension].[mode]
			ON
				[hwy_flow_mode].[mode_id] = [mode].[mode_id]
			WHERE
				[hwy_flow_mode].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [hwy_flow_mode].[flow] > 0
			GROUP BY
				[hwy_flow_mode].[scenario_id]
				,[hwy_flow_mode].[hwy_link_ab_tod_id]) AS [hwy_flow_mode_agg]
		ON
			[hwy_flow].[scenario_id] = [hwy_flow_mode_agg].[scenario_id]
			AND [hwy_flow].[hwy_link_ab_tod_id] = [hwy_flow_mode_agg].[hwy_link_ab_tod_id]
		WHERE
			[hwy_flow].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [hwy_link].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [hwy_link_ab_tod].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [hwy_flow].[flow] > 0),
	[calc_ab_tod] AS (
		SELECT
			[scenario_id]
			,[speed_free_flow]
			,[vmt_total]
			,[vmt_auto]
			,[vmt_lhdt]
			,[vmt_mhdt]
			,[vmt_hhdt]
			,(POWER([ttim2], 0.8601) + @reliability_ratio *
				((1 + 2.1406 * LOG([ttim2])) - POWER([ttim2], 0.8601))) /
				[speed_free_flow] - (1 / [speed_free_flow]) AS [delay_per_mile]
		FROM
			[flow_ab_tod]),
	[summary] AS (
		SELECT
			[scenario_id]
			,@voc_auto * SUM([vmt_auto]) AS [cost_op_auto]
			,@voc_lhdt * SUM([vmt_lhdt]) AS [cost_op_lhdt]
			,@voc_mhdt * SUM([vmt_mhdt]) AS [cost_op_mhdt]
			,@voc_hhdt * SUM([vmt_hhdt]) AS [cost_op_hhdt]
			,@vor_auto * SUM(([delay_per_mile] * [vmt_auto]) / 60) AS [cost_rel_auto]
			,@vor_lhdt * SUM(([delay_per_mile] * [vmt_lhdt]) / 60) AS [cost_rel_lhdt]
			,@vor_mhdt * SUM(([delay_per_mile] * [vmt_mhdt]) / 60) AS [cost_rel_mhdt]
			,@vor_hhdt * SUM(([delay_per_mile] * [vmt_hhdt]) / 60) AS [cost_rel_hhdt]
			,@crash_rate_pdo * @crash_cost_pdo * SUM([vmt_total]) AS [crashes_pdo]
			,@crash_rate_injury * @crash_cost_injury * SUM([vmt_total]) AS [crashes_injury]
			,@crash_rate_fatal * @crash_cost_fatal * SUM([vmt_total]) AS [crashes_fatal]
		FROM
			[calc_ab_tod]
		GROUP BY
			[scenario_id]),
	[summary_wide] AS (
		SELECT
			SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_op_auto] ELSE 0 END) AS [base_cost_op_auto]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_op_auto] ELSE 0 END) AS [build_cost_op_auto]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_op_lhdt] ELSE 0 END) AS [base_cost_op_lhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_op_lhdt] ELSE 0 END) AS [build_cost_op_lhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_op_mhdt] ELSE 0 END) AS [base_cost_op_mhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_op_mhdt] ELSE 0 END) AS [build_cost_op_mhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_op_hhdt] ELSE 0 END) AS [base_cost_op_hhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_op_hhdt] ELSE 0 END) AS [build_cost_op_hhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_rel_auto] ELSE 0 END) AS [base_cost_rel_auto]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_rel_auto] ELSE 0 END) AS [build_cost_rel_auto]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_rel_lhdt] ELSE 0 END) AS [base_cost_rel_lhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_rel_lhdt] ELSE 0 END) AS [build_cost_rel_lhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_rel_mhdt] ELSE 0 END) AS [base_cost_rel_mhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_rel_mhdt] ELSE 0 END) AS [build_cost_rel_mhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [cost_rel_hhdt] ELSE 0 END) AS [base_cost_rel_hhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [cost_rel_hhdt] ELSE 0 END) AS [build_cost_rel_hhdt]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [crashes_pdo] ELSE 0 END) AS [base_crashes_pdo]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [crashes_pdo] ELSE 0 END) AS [build_crashes_pdo]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [crashes_injury] ELSE 0 END) AS [base_crashes_injury]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [crashes_injury] ELSE 0 END) AS [build_crashes_injury]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [crashes_fatal] ELSE 0 END) AS [base_crashes_fatal]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [crashes_fatal] ELSE 0 END) AS [build_crashes_fatal]
		FROM
			[summary])
	INSERT INTO @tbl_highway_link
	SELECT
		[base_cost_op_auto] - [build_cost_op_auto] AS [cost_change_op_auto]
		,[base_cost_op_lhdt] - [build_cost_op_lhdt] AS [cost_change_op_lhdt]
		,[base_cost_op_mhdt] - [build_cost_op_mhdt] AS [cost_change_op_mhdt]
		,[base_cost_op_hhdt] - [build_cost_op_hhdt] AS [cost_change_op_hhdt]
		,[base_cost_rel_auto] - [build_cost_rel_auto] AS [cost_change_rel_auto]
		,[base_cost_rel_lhdt] - [build_cost_rel_lhdt] AS [cost_change_rel_lhdt]
		,[base_cost_rel_mhdt] - [build_cost_rel_mhdt] AS [cost_change_rel_mhdt]
		,[base_cost_rel_hhdt] - [build_cost_rel_hhdt] AS [cost_change_rel_hhdt]
		,[base_crashes_pdo] - [build_crashes_pdo] AS [cost_change_crashes_pdo]
		,[base_crashes_injury] - [build_crashes_injury] AS [cost_change_crashes_injury]
		,[base_crashes_fatal] - [build_crashes_fatal] AS [cost_change_crashes_fatal]
		,[base_cost_rel_auto] + [base_cost_rel_lhdt] + [base_cost_rel_mhdt] + [base_cost_rel_hhdt] AS [base_cost_rel]
		,[build_cost_rel_auto] + [build_cost_rel_lhdt] + [build_cost_rel_mhdt] + [build_cost_rel_hhdt] AS [build_cost_rel]
	FROM
		[summary_wide]
	RETURN
END
GO

-- Add metadata for [bca].[fn_highway_link]
EXECUTE [db_meta].[add_xp] 'bca.fn_highway_link', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_highway_link', 'MS_Description', 'function to return loaded highway network results for base and build scenarios'
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
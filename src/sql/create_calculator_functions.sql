-- Create aggregate_toll table valued function
DROP FUNCTION IF EXISTS [bca].[fn_aggregate_toll]
GO

CREATE FUNCTION [bca].[fn_aggregate_toll]
(
	@scenario_id_base integer,
	@scenario_id_build integer
)
RETURNS @tbl_toll_cost TABLE
(
	[base_toll_ctm] float NOT NULL
	,[build_toll_ctm] float NOT NULL
	,[base_toll_truck] float NOT NULL
	,[build_toll_truck] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 6/29/2018
-- Updated: 12/3/2020 for ABM 14.2.1 toll cost field rename
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
            [person_trip].[scenario_id]
            ,[model_trip].[model_trip_description]
            ,SUM([person_trip].[cost_toll_drive] * [person_trip].[weight_trip]) AS [cost_toll_drive]
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
					THEN [cost_toll_drive] ELSE 0 END) AS [base_toll_ctm]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [cost_toll_drive] ELSE 0 END) AS [build_toll_ctm]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [model_trip_description] = 'Truck'
					THEN [cost_toll_drive] ELSE 0 END) AS [base_toll_truck]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Truck'
					THEN [cost_toll_drive] ELSE 0 END) AS [build_toll_truck]
	FROM
		toll_costs
	RETURN
END
GO

-- Add metadata for [bca].[fn_aggregate_toll]
EXECUTE [db_meta].[add_xp] 'bca.fn_aggregate_toll', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_aggregate_toll', 'MS_Description', 'function to return aggregate trips toll cost results for base and build scenarios'
GO




-- Create aggregate trips table valued function
DROP FUNCTION IF EXISTS [bca].[fn_aggregate_trips]
GO

CREATE FUNCTION [bca].[fn_aggregate_trips]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@vot_ctm float, -- value of time ($/hr) for Commercial Travel model trips
	@vot_truck float -- value of time ($/hr) for Truck model trips
)
RETURNS @tbl_aggregate_trips TABLE
(
	[build_trips_base_vot_ctm] float NOT NULL,
	[build_trips_build_vot_ctm] float NOT NULL,
	[all_trips_benefit_vot_ctm] float NOT NULL,
	[build_trips_base_vot_truck] float NOT NULL,
	[build_trips_build_vot_truck] float NOT NULL,
	[all_trips_benefit_vot_truck] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/16/2018
-- Updated: 12/3/2020 for ABM 14.2.1 new trip modes, value of time field
--   update, new assignment modes, transponder availability
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- and input values of time for commercial travel model and truck model trips,
-- returns table of value of time costs for the build scenario given build
-- scenario travel times and base scenario travel times segmented by the
-- commercial travel model and truck model and a benefits calculation of
-- 1/2 * (all trips under base scenario travel times - all trips under build scenario travel times)
--	[dbo].[run_aggregate_trips_comparison]
--	[dbo].[run_aggregate_trips_processor]
--	[dbo].[run_aggregate_trips_summary]
-- ===========================================================================
BEGIN
	INSERT INTO @tbl_aggregate_trips
	SELECT
		-- build trips vot under base skims - Commercial Vehicle model
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [alternate_cost_vot]
					ELSE 0 END) AS [build_trips_base_vot_ctm]
		-- build trips vot under build skims - Commercial Vehicle model
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [cost_vot]
					ELSE 0 END) AS [build_trips_build_vot_ctm]
		-- 1/2 * all trips vot under base skims minus all trips vot under build skims - Commercial Vehicle model
		,-- all trips under base skims
			SUM(CASE	WHEN [scenario_id] = @scenario_id_base
							AND [model_trip_description] = 'Commercial Vehicle'
						THEN [cost_vot]
						WHEN [scenario_id] = @scenario_id_build
							AND [model_trip_description] = 'Commercial Vehicle'
						THEN [alternate_cost_vot]
						ELSE NULL END
		 -- all trips under build skims
			- CASE	WHEN [scenario_id] = @scenario_id_base
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [alternate_cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Commercial Vehicle'
					THEN [cost_vot]
					ELSE NULL END)
			-- multiplied by 1/2
			* .5 AS [all_trips_benefit_vot_ctm]
		-- build trips vot under base skims - Truck model
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Truck'
					THEN [alternate_cost_vot]
					ELSE 0 END) AS [build_trips_base_vot_truck]
		-- build trips vot under build skims - Truck model
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Truck'
					THEN [cost_vot]
					ELSE 0 END) AS [build_trips_build_vot_truck]
		-- 1/2 * all trips vot under base skims minus all trips vot under build skims - Truck model
		,-- all trips under base skims
			SUM(CASE	WHEN [scenario_id] = @scenario_id_base
							AND [model_trip_description] = 'Truck'
						THEN [cost_vot]
						WHEN [scenario_id] = @scenario_id_build
							AND [model_trip_description] = 'Truck'
						THEN [alternate_cost_vot]
						ELSE NULL END
		 -- all trips under build skims
			- CASE	WHEN [scenario_id] = @scenario_id_base
						AND [model_trip_description] = 'Truck'
					THEN [alternate_cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [model_trip_description] = 'Truck'
					THEN [cost_vot]
					ELSE NULL END)
			-- multiplied by 1/2
			* .5 AS [all_trips_benefit_vot_truck]
	FROM (
		-- calculate trip value of time costs for base/build trips using base/build travel times
		-- calculate trip value of time costs for all trips using base/build skim travel times
			-- note there are base/build trips without alternate scenario skim travel times
			-- calculate the cost per base/build trip under build/base skims where
				-- only base/build trips with alternate scenario travel times are included
				-- then multiply this cost per trip by the total number of base/build trips
		SELECT
			[trips_ctm_truck].[scenario_id]
			,[trips_ctm_truck].[model_trip_description]
			-- trip value of time cost for person trips with their own skims
			,SUM([trips_ctm_truck].[weight_person_trip] * [trips_ctm_truck].[time_total] *
					CASE	WHEN [trips_ctm_truck].[model_trip_description] = 'Commercial Vehicle'
							THEN @vot_ctm / 60
							WHEN [trips_ctm_truck].[model_trip_description] = 'Truck'
							THEN @vot_truck / 60
							ELSE NULL END) AS [cost_vot]
			,-- trip value of time cost for person trips with alternate skims
				(SUM([trips_ctm_truck].[weight_person_trip] * ISNULL([auto_skims].[time_total], 0) *
						CASE	WHEN [trips_ctm_truck].[model_trip_description] = 'Commercial Vehicle'
								THEN @vot_ctm / 60
								WHEN [trips_ctm_truck].[model_trip_description] = 'Truck'
								THEN @vot_truck / 60
								ELSE NULL END)
				-- divided by number of person trips with alternate skims
				/ SUM(CASE	WHEN [auto_skims].[time_total] IS NOT NULL
							THEN [trips_ctm_truck].[weight_person_trip]
							ELSE 0 END))
				-- multiplied by the total number of person trips
				* SUM([trips_ctm_truck].[weight_person_trip]) AS [alternate_cost_vot]
		FROM (
			-- get base and build trip list for Commercial Vehicle and Truck models
			-- to be matched with skims from base and build scenario
			-- match base trips with build skims and vice versa
			-- note these models only use auto skims
			SELECT
				[person_trip].[scenario_id]
				,[model_trip].[model_trip_description]
				,[geography_trip_origin].[trip_origin_taz_13]
				,[geography_trip_destination].[trip_destination_taz_13]
				-- all trip modes are directly mapped to assignment modes for auto
                -- excepting the truck modes which are collapsed into Truck
                -- excepting the Taxi, School Bus, Non-Pooled TNC, and Pooled TNC which all use Shared Ride 3+
				,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                                   'Medium Heavy Duty Truck',
                                                                   'Heavy Heavy Duty Truck')
						THEN 'Truck'
                        WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                     'Pooled TNC',
                                                                     'School Bus',
                                                                     'Taxi')
						THEN 'Shared Ride 3+'
						ELSE [mode_trip].[mode_trip_description]
						END AS [assignment_mode] -- recode trip modes to assignment modes
				,[value_of_time_category_id]
                ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                      THEN [transponder_available_id]
                      ELSE 0 END AS [transponder_available_id]  -- only drive alone trips use transponder for assignment
				,[time_trip_start].[trip_start_abm_5_tod]
				,[person_trip].[time_total]
				,[person_trip].[weight_person_trip]
			FROM
				[fact].[person_trip]
			INNER JOIN
				[dimension].[model_trip]
			ON
				[person_trip].[model_trip_id] = [model_trip].[model_trip_id]
			INNER JOIN
				[dimension].[mode_trip]
			ON
				[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
			INNER JOIN
				[dimension].[geography_trip_origin]
			ON
				[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
			INNER JOIN
				[dimension].[geography_trip_destination]
			ON
				[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
			INNER JOIN
				[dimension].[time_trip_start]
			ON
				[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
			WHERE
				[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [model_trip].[model_trip_description] IN ('Commercial Vehicle',
															  'Truck') -- Commercial Vehicle and Truck models only, these models use auto modes only
		) AS [trips_ctm_truck]
		LEFT OUTER JOIN (
			-- create base and build scenario auto skims from person trips table
			-- skims are segmented by taz-taz, assignment mode, value of time bin, and ABM five time of day
			-- if a trip is not present in the person trips table corresponding to a skim then the skim
			-- is not present here
			SELECT
				[person_trip].[scenario_id]
				,[geography_trip_origin].[trip_origin_taz_13]
				,[geography_trip_destination].[trip_destination_taz_13]
				-- all trip modes are directly mapped to assignment modes for auto
                -- excepting the truck modes which are collapsed into Truck
                -- excepting the Taxi, School Bus, Non-Pooled TNC, and Pooled TNC which all use Shared Ride 3+
				,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                                   'Medium Heavy Duty Truck',
                                                                   'Heavy Heavy Duty Truck')
						THEN 'Truck'
                        WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                     'Pooled TNC',
                                                                     'School Bus',
                                                                     'Taxi')
						THEN 'Shared Ride 3+'
						ELSE [mode_trip].[mode_trip_description]
						END AS [assignment_mode] -- recode trip modes to assignment modes
				,[value_of_time_category_id]
                ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                      THEN [transponder_available_id]
                      ELSE 0 END AS [transponder_available_id]  -- only drive alone trips use transponder for assignment
				,[time_trip_start].[trip_start_abm_5_tod]
				-- use trip weights here instead of person trips weights as this is in line with assignment
				,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]) AS [time_total]
			FROM
				[fact].[person_trip]
			INNER JOIN
				[dimension].[mode_trip]
			ON
				[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
			INNER JOIN
				[dimension].[geography_trip_origin]
			ON
				[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
			INNER JOIN
				[dimension].[geography_trip_destination]
			ON
				[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
			INNER JOIN
				[dimension].[time_trip_start]
			ON
				[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
			WHERE
				[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [mode_trip].[mode_trip_description] IN ('Drive Alone',
															'Heavy Heavy Duty Truck',
															'Light Heavy Duty Truck',
															'Medium Heavy Duty Truck',
                                                            'Non-Pooled TNC',
                                                            'Pooled TNC',
															'School Bus',
															'Shared Ride 2',
															'Shared Ride 3+',
															'Taxi') -- auto modes
			GROUP BY
				[person_trip].[scenario_id]
				,[geography_trip_origin].[trip_origin_taz_13]
				,[geography_trip_destination].[trip_destination_taz_13]
				,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                                   'Medium Heavy Duty Truck',
                                                                   'Heavy Heavy Duty Truck')
						THEN 'Truck'
                        WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                     'Pooled TNC',
                                                                     'School Bus',
                                                                     'Taxi')
						THEN 'Shared Ride 3+'
						ELSE [mode_trip].[mode_trip_description]
						END
				,[value_of_time_category_id]
                ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                      THEN [transponder_available_id]
                      ELSE 0 END
				,[time_trip_start].[trip_start_abm_5_tod]
			HAVING
				SUM([person_trip].[weight_trip]) > 0
		) AS [auto_skims]
		ON
			[trips_ctm_truck].[scenario_id] != [auto_skims].[scenario_id] -- match base trips with build skims and vice versa
			AND [trips_ctm_truck].[trip_origin_taz_13] = [auto_skims].[trip_origin_taz_13]
			AND [trips_ctm_truck].[trip_destination_taz_13] = [auto_skims].[trip_destination_taz_13]
			AND [trips_ctm_truck].[assignment_mode] = [auto_skims].[assignment_mode]
			AND [trips_ctm_truck].[value_of_time_category_id] = [auto_skims].[value_of_time_category_id]
            AND [trips_ctm_truck].[transponder_available_id] = [auto_skims].[transponder_available_id]
			AND [trips_ctm_truck].[trip_start_abm_5_tod] = [auto_skims].[trip_start_abm_5_tod]
		GROUP BY
			[trips_ctm_truck].[scenario_id]
			,[trips_ctm_truck].[model_trip_description]
	) AS [result_table]
	RETURN
END
GO

-- Add metadata for [bca].[fn_aggregate_trips]
EXECUTE [db_meta].[add_xp] 'bca.fn_aggregate_trips', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_aggregate_trips', 'MS_Description', 'function to return aggregate trips value of time costs under alternative skims'
GO




-- Create auto_ownership table valued function
DROP FUNCTION IF EXISTS [bca].[fn_auto_ownership]
GO

CREATE FUNCTION [bca].[fn_auto_ownership]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@auto_ownership_cost float -- per vehicle cost ($/yr) associated with auto ownership
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
-- Updated: 12/3/2020 for ABM 14.2.1 removing household and person weight
--   fields and adding where clause to remove NA household and person records
--   updated to numeric [autos] field
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
			,MAX([household].[autos]) AS [autos]
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
			AND [person].[person_id] > 0  -- remove Not Applicable record
			AND [household].[household_id] > 0  -- remove Not Applicable record
		GROUP BY
			[person].[scenario_id]
			,[person].[household_id]),
	[households_summary] AS (
		SELECT
			[scenario_id]
			,SUM([autos]) AS [autos]
			,SUM(CASE WHEN [senior] = 1 OR [minority] = 1 OR [low_income] = 1 THEN [autos] ELSE 0 END) AS [autos_coc_hh]
			,SUM(CASE WHEN [senior] = 1 THEN [autos] ELSE 0 END) AS [autos_senior_hh]
			,SUM(CASE WHEN [minority] = 1 THEN [autos] ELSE 0 END) AS [autos_minority_hh]
			,SUM(CASE WHEN [low_income] = 1 THEN [autos] ELSE 0 END) AS [autos_low_income_hh]
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




-- Create emissions table valued function
DROP FUNCTION IF EXISTS [bca].[fn_emissions]
GO

CREATE FUNCTION [bca].[fn_emissions]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@cost_winter_CO float, -- $/ton of EMFAC Winter Carbon Monoxide Tons Per Day Total
	@cost_annual_PM2_5 float, -- $/ton of EMFAC Annual Fine Particulate Matter (<2.5 microns) Tons Per Day Total
	@cost_summer_NOx float, -- $/ton of EMFAC Summer Nitrogen Dioxide Tons Per Day Total
	@cost_summer_ROG float,-- $/ton of EMFAC Summer Reactive Organic Gases Tons Per Day Total
	@cost_annual_SOx float,-- $/ton of EMFAC Annual Sulfur Oxides Tons Per Day Total
	@cost_annual_PM10 float, -- $/ton of EMFAC Annual Fine Particulate Matter (<10 microns) Tons Per Day Total
	@cost_annual_CO2 float -- $/ton of EMFAC Annual Carbon Dioxide Tons Per Day Total
)
RETURNS @tbl_emissions TABLE
(
	[difference_Winter_CO_TOTEX] float NOT NULL
	,[difference_Annual_PM2_5_TOTAL] float NOT NULL
	,[difference_Summer_NOx_TOTEX] float NOT NULL
	,[difference_Summer_ROG_TOTAL] float NOT NULL
	,[difference_Annual_SOx_TOTEX] float NOT NULL
	,[difference_Annual_PM10_TOTAL] float NOT NULL
	,[difference_Annual_CO2_TOTEX] float NOT NULL
	,[benefit_Winter_CO_TOTEX] float NOT NULL
	,[benefit_Annual_PM2_5_TOTAL] float NOT NULL
	,[benefit_Summer_NOx_TOTEX] float NOT NULL
	,[benefit_Summer_ROG_TOTAL] float NOT NULL
	,[benefit_Annual_SOx_TOTEX] float NOT NULL
	,[benefit_Annual_PM10_TOTAL] float NOT NULL
	,[benefit_Annual_CO2_TOTEX] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/18/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values
-- and input cost parameters for each emission type, calculates the
-- differences and benefits between the base and build scenarios for emfac
-- emissions progam output emissions. Relies on the output for each Season
-- from the emfac emissions program to be loaded into the table
-- [bca].[emfac_output].
--	[dbo].[run_emissions_comparison]
--	[dbo].[run_emissions_processor]
--	[dbo].[run_emissions_summary]
-- ===========================================================================
BEGIN
	with [emissions] AS (
		SELECT
			SUM(CASE	WHEN [Season] = 'Winter'
						AND [scenario_id] = @scenario_id_base
						THEN [CO_TOTEX] ELSE 0
						END) AS [base_Winter_CO_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Winter'
						AND [scenario_id] = @scenario_id_build
						THEN [CO_TOTEX] ELSE 0
						END) AS [build_Winter_CO_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_base
						THEN [PM2_5_TOTAL] ELSE 0
						END) AS [base_Annual_PM2_5_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_build
						THEN [PM2_5_TOTAL] ELSE 0
						END) AS [build_Annual_PM2_5_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Summer'
						AND [scenario_id] = @scenario_id_base
						THEN [NOx_TOTEX] ELSE 0
						END) AS [base_Summer_NOx_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Summer'
						AND [scenario_id] = @scenario_id_build
						THEN [NOx_TOTEX] ELSE 0
						END) AS [build_Summer_NOx_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Summer'
						AND [scenario_id] = @scenario_id_base
						THEN [ROG_TOTAL] ELSE 0
						END) AS [base_Summer_ROG_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Summer'
						AND [scenario_id] = @scenario_id_build
						THEN [ROG_TOTAL] ELSE 0
						END) AS [build_Summer_ROG_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_base
						THEN [SOx_TOTEX] ELSE 0
						END) AS [base_Annual_SOx_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_build
						THEN [SOx_TOTEX] ELSE 0
						END) AS [build_Annual_SOx_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_base
						THEN [PM10_TOTAL] ELSE 0
						END) AS [base_Annual_PM10_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_build
						THEN [PM10_TOTAL] ELSE 0
						END) AS [build_Annual_PM10_TOTAL]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_base
						THEN [CO2_TOTEX] ELSE 0
						END) AS [base_Annual_CO2_TOTEX]
			,SUM(CASE	WHEN [Season] = 'Annual'
						AND [scenario_id] = @scenario_id_build
						THEN [CO2_TOTEX] ELSE 0
						END) AS [build_Annual_CO2_TOTEX]
		FROM
			[bca].[emfac_output]
		WHERE
			[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND LTRIM(RTRIM([Veh_Tech])) = 'All Vehicles') -- only interested in totals
	INSERT INTO @tbl_emissions
	SELECT
		([base_Winter_CO_TOTEX] - [build_Winter_CO_TOTEX]) AS [difference_Winter_CO_TOTEX]
		,([base_Annual_PM2_5_TOTAL] - [build_Annual_PM2_5_TOTAL]) AS [difference_Annual_PM2_5_TOTAL]
		,([base_Summer_NOx_TOTEX] - [build_Summer_NOx_TOTEX]) AS [difference_Summer_NOx_TOTEX]
		,([base_Summer_ROG_TOTAL] - [build_Summer_ROG_TOTAL]) AS [difference_Summer_ROG_TOTAL]
		,([base_Annual_SOx_TOTEX] - [build_Annual_SOx_TOTEX]) AS [difference_Annual_SOx_TOTEX]
		,([base_Annual_PM10_TOTAL] - [build_Annual_PM10_TOTAL]) AS [difference_Annual_PM10_TOTAL]
		,([base_Annual_CO2_TOTEX] - [build_Annual_CO2_TOTEX]) AS [difference_Annual_CO2_TOTEX]
		,@cost_winter_CO * ([base_Winter_CO_TOTEX] - [build_Winter_CO_TOTEX]) AS [benefit_Winter_CO_TOTEX]
		,@cost_annual_PM2_5 * ([base_Annual_PM2_5_TOTAL] - [build_Annual_PM2_5_TOTAL]) AS [benefit_Annual_PM2_5_TOTAL]
		,@cost_summer_NOx * ([base_Summer_NOx_TOTEX] - [build_Summer_NOx_TOTEX]) AS [benefit_Summer_NOx_TOTEX]
		,@cost_summer_ROG * ([base_Summer_ROG_TOTAL] - [build_Summer_ROG_TOTAL]) AS [benefit_Summer_ROG_TOTAL]
		,@cost_annual_SOx * ([base_Annual_SOx_TOTEX] - [build_Annual_SOx_TOTEX]) AS [benefit_Annual_SOx_TOTEX]
		,@cost_annual_PM10 * ([base_Annual_PM10_TOTAL] - [build_Annual_PM10_TOTAL]) AS [benefit_Annual_PM10_TOTAL]
		,@cost_annual_CO2 * ([base_Annual_CO2_TOTEX] - [build_Annual_CO2_TOTEX]) AS [benefit_Annual_CO2_TOTEX]
	FROM
		[emissions]
	RETURN
END
GO

-- Add metadata for [bca].[fn_emissions]
EXECUTE [db_meta].[add_xp] 'bca.fn_emissions', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_emissions', 'MS_Description', 'function to return emfac program output results for base and build scenarios'
GO




-- Create demographics table valued function
DROP FUNCTION IF EXISTS [bca].[fn_demographics]
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
-- Updated: 12/3/2020 for ABM 14.2.1 removing household and person weight fields
--   and adding where clause to remove NA household and person records
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
		,COUNT([person_id]) AS [persons]
		,SUM(CASE	WHEN [person].[age] >= 75 THEN 1
					WHEN [person].[race] IN ('Some Other Race Alone',
											 'Asian Alone',
											 'Black or African American Alone',
											 'Two or More Major Race Groups',
											 'Native Hawaiian and Other Pacific Islander Alone',
											 'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
					WHEN [household].[poverty] <= 2 THEN 1
					ELSE 0 END) AS [persons_coc]
		,SUM(CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END) AS [persons_senior]
		,SUM(CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic'
                    THEN 1 ELSE 0 END) AS [persons_minority]
		,SUM(CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END) AS [persons_low_income]
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
		AND [person].[person_id] > 0  -- remove Not Applicable records
		AND [household].[household_id] > 0  -- remove Not Applicable records
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
DROP FUNCTION IF EXISTS [bca].[fn_highway_link]
GO

CREATE FUNCTION [bca].[fn_highway_link]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@reliability_ratio float, -- reliability ratio (value of reliability ($/hr) / value of time ($/hr))
	@crash_cost_pdo float, -- property damage only crash cost ($/cash)
	@crash_cost_injury float, -- injury crash cost ($/crash)
	@crash_cost_fatal float, -- fatal crash cost ($/crash)
	@crash_rate_pdo float, -- property damage only crash rate (crashes/1,000,000 vmt)
	@crash_rate_injury float, -- injury crash rate (crashes/1,000,000 vmt)
	@crash_rate_fatal float, -- fatal crash rate (crashes/1,000,000 vmt)
	@voc_auto float, -- auto vehicle operating cost ($/mile)
	@voc_lhdt float, -- light heavy-duty truck vehicle operating cost ($/mile)
	@voc_mhdt float, -- medium heavy-duty truck vehicle operating cost ($/mile)
	@voc_hhdt float, -- heavy heavy-duty truck vehicle operating cost ($/mile)
	@vor_auto float, -- auto trip value of reliability ($/hr)
	@vor_lhdt float, -- light heavy-duty truck trip value of reliability ($/hr)
	@vor_mhdt float, -- medium heavy-duty truck trip value of reliability ($/hr)
	@vor_hhdt float -- heavy heavy-duty truck trip value of reliability ($/hr)
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
-- Updated: 12/3/2020 for ABM 14.2.1 new assignment modes, add filter to remove
--   records with bus preload flow only but no free flow time or travel time
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns and compares operating costs, reliability costs, and crash costs
-- derived from the loaded highway network.
--	[dbo].[run_link_comparison]
--	[dbo].[run_link_processor]
--	[dbo].[run_link_summary]
-- ===========================================================================
BEGIN

	-- get link volumes by ab and tod for autos and trucks (lht, mht, hht)
	-- calculate link free flow speed by ab and tod
	-- begin calculation of link vehicle delay per mile by ab and tod
	with [flow_ab_tod] AS (
		SELECT
			[hwy_flow].[scenario_id]
			,CASE	WHEN 1.0274 * POWER([hwy_flow].[time] / ([hwy_link_ab_tod].[tm]), 1.2204) > 3.0
					THEN 3.0
					ELSE 1.0274 * POWER([hwy_flow].[time] / ([hwy_link_ab_tod].[tm]), 1.2204)
					END AS [ttim2] -- begin calculation of link vehicle delay per mile by ab and tod
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
				,SUM(CASE	WHEN [mode].[mode_description] IN ('Drive Alone',
															   'Shared Ride 2',
															   'Shared Ride 3+')
							THEN [flow]
							ELSE 0 END) AS [flow_auto]
				,SUM(CASE	WHEN [mode].[mode_description] ='Light Heavy Duty Truck'
							THEN [flow]
							ELSE 0 END) AS [flow_lhdt]
				,SUM(CASE	WHEN [mode].[mode_description] = 'Medium Heavy Duty Truck'
							THEN [flow]
							ELSE 0 END) AS [flow_mhdt]
				,SUM(CASE	WHEN [mode].[mode_description] = 'Heavy Heavy Duty Truck'
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
			AND [hwy_flow].[flow] > 0
            -- there are records with bus preload but no time(s)
            AND [hwy_link_ab_tod].[tm] > 0),
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




-- Create physical_activity table valued function
DROP FUNCTION IF EXISTS [bca].[fn_physical_activity]
GO

CREATE FUNCTION [bca].[fn_physical_activity]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@bike_vot_recreational float, -- value of time benefit ($/hr) of recreational bicycling
	@bike_vot_non_recreational float, -- value of time benefit ($/hr) of non-recreational bicycling
	@walk_vot_recreational float, -- value of time benefit ($/hr) of recreational walking
	@walk_vot_non_recreational float -- value of time benefit ($/hr) of non-recreational walking
)
RETURNS @tbl_physical_activity TABLE
(
	[base_vot_bike] float NOT NULL
	,[build_vot_bike] float NOT NULL
	,[benefit_bike] float NOT NULL
	,[benefit_bike_coc] float NOT NULL
	,[benefit_bike_senior] float NOT NULL
	,[benefit_bike_minority] float NOT NULL
	,[benefit_bike_low_income] float NOT NULL
	,[base_vot_walk] float NOT NULL
	,[build_vot_walk] float NOT NULL
	,[benefit_walk] float NOT NULL
	,[benefit_walk_coc] float NOT NULL
	,[benefit_walk_senior] float NOT NULL
	,[benefit_walk_minority] float NOT NULL
	,[benefit_walk_low_income] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		Gregor Schroeder
-- Create date: 9/21/2018
-- Updated: 12/3/2020 for ABM 14.2.1 removing household and person weight fields
--   and adding where clause to remove NA household and person records
-- Description:	Given two input scenario_id values and input values for value
-- of time benefits ($/hr) of recreation/non-recreational cycling/walking,
-- calculates the monetary benefit of physically active transportation minutes
--  for the base and build scenarios by
-- Community of Concern and each element that indicates a Community of Concern
-- person (seniors, minorities, low income). Benefits between
-- the base and build scenarios are calculated.
-- ===========================================================================
BEGIN
	INSERT INTO @tbl_physical_activity
	SELECT
		[base_vot_bike]
		,[build_vot_bike]
		,[build_vot_bike] - [base_vot_bike] AS [benefit_bike]
		,[build_vot_bike_coc] - [base_vot_bike_coc] AS [benefit_bike_coc]
		,[build_vot_bike_senior] - [base_vot_bike_senior] AS [benefit_bike_senior]
		,[build_vot_bike_minority] - [base_vot_bike_minority] AS [benefit_bike_minority]
		,[build_vot_bike_low_income] - [base_vot_bike_low_income] AS [benefit_bike_low_income]
		,[base_vot_walk]
		,[build_vot_walk]
		,[build_vot_walk] - [base_vot_walk] AS [benefit_walk]
		,[build_vot_walk_coc] - [base_vot_walk_coc] AS [benefit_walk_coc]
		,[build_vot_walk_senior] - [base_vot_walk_senior] AS [benefit_walk_senior]
		,[build_vot_walk_minority] - [base_vot_walk_minority] AS [benefit_walk_minority]
		,[build_vot_walk_low_income] - [base_vot_walk_low_income] AS [benefit_walk_low_income]
	FROM (
		SELECT
			SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [vot_bike] ELSE 0 END) AS [base_vot_bike]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [vot_bike] ELSE 0 END) AS [build_vot_bike]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND ([senior] = 1 OR [minority] = 1 OR [low_income] = 1) THEN [vot_bike] ELSE 0 END) AS [base_vot_bike_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND ([senior] = 1 OR [minority] = 1 OR [low_income] = 1) THEN [vot_bike] ELSE 0 END) AS [build_vot_bike_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [senior] = 1 THEN [vot_bike] ELSE 0 END) AS [base_vot_bike_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [senior] = 1 THEN [vot_bike] ELSE 0 END) AS [build_vot_bike_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [minority] = 1 THEN [vot_bike] ELSE 0 END) AS [base_vot_bike_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [minority] = 1 THEN [vot_bike] ELSE 0 END) AS [build_vot_bike_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [low_income] = 1 THEN [vot_bike] ELSE 0 END) AS [base_vot_bike_low_income]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [low_income] = 1 THEN [vot_bike] ELSE 0 END) AS [build_vot_bike_low_income]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base THEN [vot_walk] ELSE 0 END) AS [base_vot_walk]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build THEN [vot_walk] ELSE 0 END) AS [build_vot_walk]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND ([senior] = 1 OR [minority] = 1 OR [low_income] = 1) THEN [vot_walk] ELSE 0 END) AS [base_vot_walk_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND ([senior] = 1 OR [minority] = 1 OR [low_income] = 1) THEN [vot_walk] ELSE 0 END) AS [build_vot_walk_coc]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [senior] = 1 THEN [vot_walk] ELSE 0 END) AS [base_vot_walk_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [senior] = 1 THEN [vot_walk] ELSE 0 END) AS [build_vot_walk_senior]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [minority] = 1 THEN [vot_walk] ELSE 0 END) AS [base_vot_walk_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [minority] = 1 THEN [vot_walk] ELSE 0 END) AS [build_vot_walk_minority]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_base AND [low_income] = 1 THEN [vot_walk] ELSE 0 END) AS [base_vot_walk_low_income]
			,SUM(CASE WHEN [scenario_id] = @scenario_id_build AND [low_income] = 1 THEN [vot_walk] ELSE 0 END) AS [build_vot_walk_low_income]
		FROM (
			SELECT
				[person_trip].[scenario_id]
				,CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END AS [senior]
				,CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												 'Asian Alone',
												 'Black or African American Alone',
												 'Two or More Major Race Groups',
								 				 'Native Hawaiian and Other Pacific Islander Alone',
								 				 'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
						ELSE 0 END AS [minority]
				,CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END AS [low_income]
				,SUM(CASE	WHEN [purpose_tour].[purpose_tour_description] = 'Discretionary'
							THEN [weight_person_trip] * [time_bike] * @bike_vot_recreational / 60 -- most but not all trips are recreational in this tour purpose, assume all
							ELSE [weight_person_trip] * [time_bike] * @bike_vot_non_recreational / 60
							END) AS [vot_bike]
				,SUM(CASE	WHEN [purpose_tour].[purpose_tour_description] = 'Discretionary'
							THEN [weight_person_trip] * [time_walk] * @walk_vot_recreational / 60 -- most but not all trips are recreational in this tour purpose, assume all
							ELSE [weight_person_trip] * [time_walk] * @walk_vot_non_recreational / 60
							END) AS [vot_walk] -- includes transit walk components
			FROM
				[fact].[person_trip]
			INNER JOIN
				[dimension].[model_trip]
			ON
				[person_trip].[model_trip_id] = [model_trip].[model_trip_id]
			INNER JOIN
				[dimension].[tour]
			ON
				[person_trip].[scenario_id] = [tour].[scenario_id]
				AND [person_trip].[tour_id] = [tour].[tour_id]
			INNER JOIN
				[dimension].[purpose_tour]
			ON
				[tour].[purpose_tour_id] = [purpose_tour].[purpose_tour_id]
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
				AND [tour].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
				AND [person].[person_id] > 0  -- remove Not Applicable records
				AND [household].[household_id] > 0  -- remove Not Applicable records
				AND [model_trip].[model_trip_description] IN ('Individual',
															  'Internal-External',
															  'Joint') -- resident models only
			GROUP BY
				[person_trip].[scenario_id]
				,CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END
				,CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												 'Asian Alone',
												 'Black or African American Alone',
												 'Two or More Major Race Groups',
								 				 'Native Hawaiian and Other Pacific Islander Alone',
								 				 'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
						ELSE 0 END
				,CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END
		) AS [results_table_long]
	) AS [results_table_wide]
	RETURN
END
GO

-- Add metadata for [bca].[fn_physical_activity]
EXECUTE [db_meta].[add_xp] 'bca.fn_physical_activity', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_physical_activity', 'MS_Description', 'function to return person physical activity results for base and build scenarios'
GO




-- Create active transportation resident trips table valued function
DROP FUNCTION IF EXISTS [bca].[fn_resident_trips_at]
GO

CREATE FUNCTION [bca].[fn_resident_trips_at]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@vot_commute float, -- value of time ($/hr) of work tour purpose trips
	@vot_non_commute float -- value of time ($/hr) of non-work tour purpose trips
)
RETURNS @tbl_resident_trips_at TABLE
(
	[person_trips] float NOT NULL
	,[base_person_trips] float NOT NULL
	,[build_person_trips] float NOT NULL
	,[coc_person_trips] float NOT NULL
	,[base_coc_person_trips] float NOT NULL
	,[build_coc_person_trips] float NOT NULL
	,[benefit_vot] float NOT NULL
	,[base_vot] float NOT NULL
	,[build_vot] float NOT NULL
	,[work_benefit_vot] float NOT NULL
	,[non_work_benefit_vot] float NOT NULL
	,[work_coc_benefit_vot] float NOT NULL
	,[non_work_coc_benefit_vot] float NOT NULL
	,[work_senior_benefit_vot] float NOT NULL
	,[non_work_senior_benefit_vot] float NOT NULL
	,[work_minority_benefit_vot] float NOT NULL
	,[non_work_minority_benefit_vot] float NOT NULL
	,[work_low_income_benefit_vot] float NOT NULL
	,[non_work_low_income_benefit_vot] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/24/2018
-- Update: 12/3/2020 Removing household and person weight fields
--   and adding where clause to remove NA household and person records
--   Added taz-taz skim matching when mgra-mgra fails.
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- and input values of time for Work and Non-Work tour purpose trips,
-- returns table of value of time costs for the build scenario given build
-- scenario travel times and base scenario travel times for active transportation
-- resident trips and a benefits calculation of
-- 1/2 * (all trips under base scenario travel times - all trips under build scenario travel times)
-- Note that all trips without alternate scenario travel times are assigned the
-- average per trip travel time of trips with alternate travel times.
--	[dbo].[run_person_trip_processor]
--	[dbo].[run_person_trip_summary]
-- ===========================================================================
BEGIN
    with [at_skims_mgra] AS (
        -- create base and build scenario active transportation skims from person trips table
        -- skims are segmented by mgra-mgra and assignment mode (bike+walk)
        -- if a trip is not present in the person trips table corresponding to a skim then the skim is not present here
        SELECT
            [person_trip].[scenario_id]
            ,[geography_trip_origin].[trip_origin_mgra_13]
            ,[geography_trip_destination].[trip_destination_mgra_13]
            -- all trip modes are directly mapped to assignment modes for
            -- active transportation assignment modes
            ,[mode_trip].[mode_trip_description] AS [assignment_mode]
            -- use trip weights here instead of person trips weights as this is in line with assignment
            ,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]) AS [time_total]
        FROM
            [fact].[person_trip]
        INNER JOIN
            [dimension].[mode_trip]
        ON
            [person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
        INNER JOIN
            [dimension].[geography_trip_origin]
        ON
            [person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
        INNER JOIN
            [dimension].[geography_trip_destination]
        ON
            [person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
        WHERE
            [person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
            -- active transportation modes only
            AND [mode_trip].[mode_trip_description] IN ('Bike', 'Walk')
        GROUP BY
            [person_trip].[scenario_id]
            ,[geography_trip_origin].[trip_origin_mgra_13]
            ,[geography_trip_destination].[trip_destination_mgra_13]
            ,[mode_trip].[mode_trip_description]
        HAVING
            SUM([person_trip].[weight_trip]) > 0
    ),
    [at_skims_taz] AS (
        -- create base and build scenario active transportation skims from person trips table
        -- skims are segmented by taz-taz and assignment mode (bike+walk)
        -- if a trip is not present in the person trips table corresponding to a skim then the skim is not present here
        SELECT
            [person_trip].[scenario_id]
            ,[geography_trip_origin].[trip_origin_taz_13]
            ,[geography_trip_destination].[trip_destination_taz_13]
            -- all trip modes are directly mapped to assignment modes for
            -- active transportation assignment modes
            ,[mode_trip].[mode_trip_description] AS [assignment_mode]
            -- use trip weights here instead of person trips weights as this is in line with assignment
            ,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]) AS [time_total]
        FROM
            [fact].[person_trip]
        INNER JOIN
            [dimension].[mode_trip]
        ON
            [person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
        INNER JOIN
            [dimension].[geography_trip_origin]
        ON
            [person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
        INNER JOIN
            [dimension].[geography_trip_destination]
        ON
            [person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
        WHERE
            [person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
            -- active transportation modes only
            AND [mode_trip].[mode_trip_description] IN ('Bike', 'Walk')
        GROUP BY
            [person_trip].[scenario_id]
            ,[geography_trip_origin].[trip_origin_taz_13]
            ,[geography_trip_destination].[trip_destination_taz_13]
            ,[mode_trip].[mode_trip_description]
        HAVING
            SUM([person_trip].[weight_trip]) > 0
    ),
    [at_resident_trips] AS (
        -- get trip list for base and build scenario of all resident trips
        -- that use the synthetic population
        -- this includes Individual, Internal-External, and Joint models
        -- append tour purpose and person Community of Concern information
        -- restrict to active transportation modes (Bike, Walk)
        -- skims are segmented by mgra-mgra and assignment mode
        -- include tazs to allow taz-taz skim matching if no mgra-mgra match
        SELECT
            [person_trip].[scenario_id]
            ,[purpose_tour].[purpose_tour_description]
            ,CASE	WHEN [person].[age] >= 75 THEN 1
                    WHEN [person].[race] IN ('Some Other Race Alone',
                                                'Asian Alone',
                                                'Black or African American Alone',
                                                'Two or More Major Race Groups',
                                                'Native Hawaiian and Other Pacific Islander Alone',
                                                'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
                            OR [person].[hispanic] = 'Hispanic' THEN 1
                    WHEN [household].[poverty] <= 2 THEN 1
                    ELSE 0 END AS [persons_coc]
            ,CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END AS [persons_senior]
            ,CASE	WHEN [person].[race] IN ('Some Other Race Alone',
                                             'Asian Alone',
                                             'Black or African American Alone',
                                             'Two or More Major Race Groups',
                                             'Native Hawaiian and Other Pacific Islander Alone',
                                             'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
                        OR [person].[hispanic] = 'Hispanic' THEN 1
                    ELSE 0 END AS [persons_minority]
            ,CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END AS [persons_low_income]
            ,[geography_trip_origin].[trip_origin_mgra_13]
            ,[geography_trip_origin].[trip_origin_taz_13]
            ,[geography_trip_destination].[trip_destination_mgra_13]
            ,[geography_trip_destination].[trip_destination_taz_13]
            -- all trip modes are directly mapped to assignment modes for
            -- active transportation assignment modes
            ,[mode_trip].[mode_trip_description] AS [assignment_mode]
            ,[person_trip].[time_total]
            ,[person_trip].[weight_trip]
            ,[person_trip].[weight_person_trip]
        FROM
            [fact].[person_trip]
        INNER JOIN
            [dimension].[model_trip]
        ON
            [person_trip].[model_trip_id] = [model_trip].[model_trip_id]
        INNER JOIN
            [dimension].[mode_trip]
        ON
            [person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
        INNER JOIN
            [dimension].[geography_trip_origin]
        ON
            [person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
        INNER JOIN
            [dimension].[geography_trip_destination]
        ON
            [person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
        INNER JOIN
            [dimension].[tour]
        ON
            [person_trip].[scenario_id] = [tour].[scenario_id]
            AND [person_trip].[tour_id] = [tour].[tour_id]
        INNER JOIN
            [dimension].[purpose_tour]
        ON
            [tour].[purpose_tour_id] = [purpose_tour].[purpose_tour_id]
        INNER JOIN
            [dimension].[household]
        ON
            [person_trip].[scenario_id] = [household].[scenario_id]
            AND [person_trip].[household_id] = [household].[household_id]
        INNER JOIN
            [dimension].[person]
        ON
            [person_trip].[scenario_id] = [person].[scenario_id]
            AND [person_trip].[person_id] = [person].[person_id]
        WHERE
            [person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
            AND [tour].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
            AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
            AND [person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
                -- resident trips that use synthetic population
            AND [model_trip].[model_trip_description] IN ('Individual',
                                                          'Internal-External',
                                                          'Joint')
            -- active transportation modes only
            AND [mode_trip].[mode_trip_description] IN ('Bike', 'Walk')
            AND [person].[person_id] > 0  -- remove Not Applicable records
            AND [household].[household_id] > 0  -- remove Not Applicable records
    ),
    [avg_alternate_trip_time] AS (
        -- calculate average trip time for all trips using alternate scenario skim travel times
            -- note there are base/build trips without alternate scenario skim travel times
            -- calculate the base/build average trip time under build/base skims where
                -- only base/build trips with alternate scenario travel times are included
                -- then divide this by total number of base/build trips with alternate scenario travel times
        SELECT
            [at_resident_trips].[scenario_id]
            ,-- total trip time for trips with alternative skims, substitute taz skim if mgra skim not present
                (SUM([at_resident_trips].[weight_trip] * ISNULL([at_skims_mgra].[time_total], ISNULL([at_skims_taz].[time_total], 0)))
                -- divided by number of trips with alternative skims
                / SUM(CASE	WHEN [at_skims_mgra].[time_total] IS NOT NULL
                              OR [at_skims_taz].[time_total] IS NOT NULL
                            THEN [at_resident_trips].[weight_trip]
                            ELSE 0 END)) AS [time_total_avg]
        FROM
            [at_resident_trips]
        LEFT OUTER JOIN
            [at_skims_mgra]
        ON
            [at_resident_trips].[scenario_id] != [at_skims_mgra].[scenario_id] -- match base trips with build skims and vice versa
            AND [at_resident_trips].[trip_origin_mgra_13] = [at_skims_mgra].[trip_origin_mgra_13]
            AND [at_resident_trips].[trip_destination_mgra_13] = [at_skims_mgra].[trip_destination_mgra_13]
            AND [at_resident_trips].[assignment_mode] = [at_skims_mgra].[assignment_mode]
        LEFT OUTER JOIN
            [at_skims_taz]
        ON
            [at_resident_trips].[scenario_id] != [at_skims_taz].[scenario_id] -- match base trips with build skims and vice versa
            AND [at_resident_trips].[trip_origin_taz_13] = [at_skims_taz].[trip_origin_taz_13]
            AND [at_resident_trips].[trip_destination_taz_13] = [at_skims_taz].[trip_destination_taz_13]
            AND [at_resident_trips].[assignment_mode] = [at_skims_taz].[assignment_mode]
        GROUP BY
            [at_resident_trips].[scenario_id]
    ),
    [results_table] AS (
        SELECT
            [at_resident_trips].[scenario_id]
            ,[at_resident_trips].[purpose_tour_description]
            ,[at_resident_trips].[persons_coc]
            ,[at_resident_trips].[persons_senior]
            ,[at_resident_trips].[persons_minority]
            ,[at_resident_trips].[persons_low_income]
            -- person trip value of time cost for trips with their own skims
            ,SUM([at_resident_trips].[weight_person_trip] * [at_resident_trips].[time_total] *
                    CASE	WHEN [at_resident_trips].[purpose_tour_description] = 'Work'
                            THEN @vot_commute / 60
                            WHEN [at_resident_trips].[purpose_tour_description] != 'Work'
                            THEN @vot_non_commute / 60
                            ELSE NULL END) AS [cost_vot]
            -- person trip value of time cost for trips with alternative skims
            -- substitute taz skim if mgra skim is not present
            -- substitute average alternative skim time for the trip if no alternative skim is present
            ,(SUM([at_resident_trips].[weight_person_trip] * ISNULL([at_skims_mgra].[time_total], ISNULL([at_skims_taz].[time_total], [avg_alternate_trip_time].[time_total_avg])) *
                CASE	WHEN [at_resident_trips].[purpose_tour_description] = 'Work'
                        THEN @vot_commute / 60
                        WHEN [at_resident_trips].[purpose_tour_description] != 'Work'
                        THEN @vot_non_commute / 60
                        ELSE NULL END)) AS [alternate_cost_vot]
			,SUM([at_resident_trips].[weight_person_trip]) AS [person_trips]
        FROM
            [at_resident_trips]
        LEFT OUTER JOIN
            [at_skims_mgra]
        ON
            [at_resident_trips].[scenario_id] != [at_skims_mgra].[scenario_id] -- match base trips with build skims and vice versa
            -- at trips match at skims geographies, no need for aggregation
            AND [at_resident_trips].[trip_origin_mgra_13] = [at_skims_mgra].[trip_origin_mgra_13]
            AND [at_resident_trips].[trip_destination_mgra_13] = [at_skims_mgra].[trip_destination_mgra_13]
            AND [at_resident_trips].[assignment_mode] = [at_skims_mgra].[assignment_mode]
        LEFT OUTER JOIN
            [at_skims_taz]
        ON
            [at_resident_trips].[scenario_id] != [at_skims_taz].[scenario_id] -- match base trips with build skims and vice versa
            -- at trips match at skims geographies, no need for aggregation
            AND [at_resident_trips].[trip_origin_taz_13] = [at_skims_taz].[trip_origin_taz_13]
            AND [at_resident_trips].[trip_destination_taz_13] = [at_skims_taz].[trip_destination_taz_13]
            AND [at_resident_trips].[assignment_mode] = [at_skims_taz].[assignment_mode]
        INNER JOIN
            [avg_alternate_trip_time]
        ON
            [at_resident_trips].[scenario_id] = [avg_alternate_trip_time].[scenario_id]
        GROUP BY
            [at_resident_trips].[scenario_id]
            ,[at_resident_trips].[purpose_tour_description]
            ,[at_resident_trips].[persons_coc]
            ,[at_resident_trips].[persons_senior]
            ,[at_resident_trips].[persons_minority]
            ,[at_resident_trips].[persons_low_income]
    )
    INSERT INTO @tbl_resident_trips_at
    SELECT
		SUM([person_trips]) AS [person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [person_trips]
					ELSE 0 END) AS [base_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
                    THEN [person_trips]
					ELSE 0 END) AS [build_person_trips]
		,SUM(CASE	WHEN [persons_coc] = 1
					THEN [person_trips]
					ELSE 0 END) AS [coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [persons_coc] = 1
                    THEN [person_trips]
					ELSE 0 END) AS [base_coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [persons_coc] = 1
                    THEN [person_trips]
					ELSE 0 END) AS [build_coc_person_trips]
        ,-- 1/2 * all trips vot under base skims minus all trips vot under build skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- all trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [benefit_vot]
		,-- 1/2 * all trips vot under base skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                    THEN [alternate_cost_vot]
                    ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [base_vot]
		,-- 1/2 * all trips vot under build skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [alternate_cost_vot]
					WHEN [scenario_id] = @scenario_id_build
					THEN [cost_vot]
					ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [build_vot]
        ,-- 1/2 * work trips vot under base skims minus work trips vot under build skims
        -- work trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] = 'Work'
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] = 'Work'
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- work trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] = 'Work'
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] = 'Work'
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [work_benefit_vot]
        ,-- 1/2 * non work trips vot under base skims minus non work trips trips vot under build skims
        -- non work trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] != 'Work'
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] != 'Work'
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- non work trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] != 'Work'
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] != 'Work'
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [non_work_benefit_vot]
        ,-- 1/2 * work coc person trips vot under base skims minus work coc trips vot under build skims
        -- work coc under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_coc] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_coc] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- work coc trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_coc] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_coc] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [work_coc_benefit_vot]
        ,-- 1/2 * non work coc person trips vot under base skims minus non work coc trips vot under build skims
        -- non work coc under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_coc] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_coc] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- non work coc trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_coc] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_coc] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [non_work_coc_benefit_vot]
        ,-- 1/2 * work senior trips vot under base skims minus work senior trips vot under build skims
        -- work senior trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_senior] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_senior] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- work senior trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_senior] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_senior] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [work_senior_benefit_vot]
        ,-- 1/2 * non work senior trips vot under base skims minus non work senior trips vot under build skims
        -- non work senior trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_senior] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_senior] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- non work senior trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_senior] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_senior] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [non_work_senior_benefit_vot]
        ,-- 1/2 * work minority trips vot under base skims minus work minority trips vot under build skims
        -- work minority trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_minority] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_minority] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- work minority trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_minority] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_minority] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [work_minority_benefit_vot]
        ,-- 1/2 * non work minority trips vot under base skims minus non work minority trips vot under build skims
        -- non work minority trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_minority] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_minority] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- non work minority trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_minority] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_minority] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [non_work_minority_benefit_vot]
        ,-- 1/2 * work low income trips vot under base skims minus work low income trips vot under build skims
        -- work low income trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_low_income] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] = 'Work'
                        AND [persons_low_income] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- work low income trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_low_income] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] = 'Work'
                    AND [persons_low_income] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [work_low_income_benefit_vot]
        ,-- 1/2 * non work low income trips vot under base skims minus non work low income trips vot under build skims
        -- non work low income trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_low_income] = 1
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                        AND [purpose_tour_description] != 'Work'
                        AND [persons_low_income] = 1
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- non work low income trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_low_income] = 1
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                    AND [purpose_tour_description] != 'Work'
                    AND [persons_low_income] = 1
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [non_work_low_income_benefit_vot]
    FROM
        [results_table]

    RETURN
END
GO

-- Add metadata for [bca].[fn_resident_trips_at]
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_at', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_at', 'MS_Description', 'function to return active transportation resident trips value of time costs under alternative skims'
GO




-- Create auto resident trips table valued function
DROP FUNCTION IF EXISTS [bca].[fn_resident_trips_auto]
GO

CREATE FUNCTION [bca].[fn_resident_trips_auto]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@vot_commute float, -- value of time ($/hr) of work tour purpose trips
	@vot_non_commute float -- value of time ($/hr) of non-work tour purpose trips
)
RETURNS @tbl_resident_trips_auto TABLE
(
	[person_trips] float NOT NULL
	,[base_person_trips] float NOT NULL
	,[build_person_trips] float NOT NULL
	,[coc_person_trips] float NOT NULL
	,[base_coc_person_trips] float NOT NULL
	,[build_coc_person_trips] float NOT NULL
	,[benefit_vot] float NOT NULL
	,[base_vot] float NOT NULL
	,[build_vot] float NOT NULL
	,[work_benefit_vot] float NOT NULL
	,[non_work_benefit_vot] float NOT NULL
	,[work_coc_benefit_vot] float NOT NULL
	,[non_work_coc_benefit_vot] float NOT NULL
	,[work_senior_benefit_vot] float NOT NULL
	,[non_work_senior_benefit_vot] float NOT NULL
	,[work_minority_benefit_vot] float NOT NULL
	,[non_work_minority_benefit_vot] float NOT NULL
	,[work_low_income_benefit_vot] float NOT NULL
	,[non_work_low_income_benefit_vot] float NOT NULL
	,[base_cost_toll] float NOT NULL
	,[build_cost_toll] float NOT NULL
	,[work_base_cost_toll] float NOT NULL
	,[work_build_cost_toll] float NOT NULL
	,[non_work_base_cost_toll] float NOT NULL
	,[non_work_build_cost_toll] float NOT NULL
	,[work_coc_base_cost_toll] float NOT NULL
	,[work_coc_build_cost_toll] float NOT NULL
	,[non_work_coc_base_cost_toll] float NOT NULL
	,[non_work_coc_build_cost_toll] float NOT NULL
)
AS

-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/24/2018
-- Updated: 12/4/2020 for ABM 14.2.1 new trip modes, value of time field
--   update, new assignment modes, transponder availability, toll cost field rename,
--   removing household and person weight fields and adding where clause to remove
--   NA household and person records
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- and input values of time for Work and Non-Work tour purpose trips,
-- returns table of value of time costs for the build scenario given build
-- scenario travel times and base scenario travel times for auto
-- resident trips and a benefits calculation of
-- 1/2 * (all trips under base scenario travel times - all trips under build scenario travel times)
-- Note that all trips without alternate scenario travel times are assigned the
-- average per trip travel time of trips with alternate travel times.
--	[dbo].[run_person_trip_processor]
--	[dbo].[run_person_trip_summary]
-- ===========================================================================
BEGIN
	with [auto_skims] AS (
		-- create base and build scenario auto skims from person trips table
		-- skims are segmented by taz-taz, assignment mode, value of time bin, and ABM five time of day
		-- if a trip is not present in the person trips table corresponding to a skim then the skim
		-- is not present here
		SELECT
			[person_trip].[scenario_id]
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
			-- all trip modes are directly mapped to assignment modes for auto
            -- excepting the truck modes which are collapsed into Truck
            -- excepting the Taxi, School Bus, Non-Pooled TNC, and Pooled TNC which all use Shared Ride 3+
			,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                               'Medium Heavy Duty Truck',
                                                               'Heavy Heavy Duty Truck')
					THEN 'Truck'
                    WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                 'Pooled TNC',
                                                                 'School Bus',
                                                                 'Taxi')
					THEN 'Shared Ride 3+'
					ELSE [mode_trip].[mode_trip_description]
					END AS [assignment_mode] -- recode trip modes to assignment modes
			,[value_of_time_category_id]
            ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                    THEN [transponder_available_id]
                    ELSE 0 END AS [transponder_available_id]  -- only drive alone trips use transponder for assignment
			,[time_trip_start].[trip_start_abm_5_tod]
			-- use trip weights here instead of person trips weights as this is in line with assignment
			,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]) AS [time_total]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[mode_trip]
		ON
			[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
		INNER JOIN
			[dimension].[geography_trip_origin]
		ON
			[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
		INNER JOIN
			[dimension].[geography_trip_destination]
		ON
			[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
		INNER JOIN
			[dimension].[time_trip_start]
		ON
			[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
		WHERE
			[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [mode_trip].[mode_trip_description] IN ('Drive Alone',
														'Heavy Heavy Duty Truck',
														'Light Heavy Duty Truck',
														'Medium Heavy Duty Truck',
                                                        'Non-Pooled TNC',
                                                        'Pooled TNC',
														'School Bus',
														'Shared Ride 2',
														'Shared Ride 3+',
														'Taxi') -- auto modes
		GROUP BY
			[person_trip].[scenario_id]
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
			,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                               'Medium Heavy Duty Truck',
                                                               'Heavy Heavy Duty Truck')
					THEN 'Truck'
                    WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                 'Pooled TNC',
                                                                 'School Bus',
                                                                 'Taxi')
					THEN 'Shared Ride 3+'
					ELSE [mode_trip].[mode_trip_description]
					END
			,[value_of_time_category_id]
            ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                    THEN [transponder_available_id]
                    ELSE 0 END
			,[time_trip_start].[trip_start_abm_5_tod]
		HAVING
			SUM([person_trip].[weight_trip]) > 0
	),
	[auto_resident_trips] AS (
		-- get trip list for base and build scenario of all resident trips
		-- that use the synthetic population
		-- this includes Individual, Internal-External, and Joint models
		-- append tour purpose and person Community of Concern information
		-- restrict to auto modes
		-- skims are segmented by taz-taz, assignment mode, value of time bin, and ABM 5 time of day
		SELECT
			[person_trip].[scenario_id]
			,[purpose_tour].[purpose_tour_description]
			,CASE	WHEN [person].[age] >= 75 THEN 1
					WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
					WHEN [household].[poverty] <= 2 THEN 1
					ELSE 0 END AS [persons_coc]
			,CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END AS [persons_senior]
			,CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
						OR [person].[hispanic] = 'Hispanic' THEN 1
					ELSE 0 END AS [persons_minority]
			,CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END AS [persons_low_income]
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
			-- all trip modes are directly mapped to assignment modes for auto
            -- excepting the truck modes which are collapsed into Truck
            -- excepting the Taxi, School Bus, Non-Pooled TNC, and Pooled TNC which all use Shared Ride 3+
			,CASE WHEN [mode_trip].[mode_trip_description] IN ('Light Heavy Duty Truck',
                                                                'Medium Heavy Duty Truck',
                                                                'Heavy Heavy Duty Truck')
					THEN 'Truck'
                    WHEN [mode_trip].[mode_trip_description] IN ('Non-Pooled TNC',
                                                                    'Pooled TNC',
                                                                    'School Bus',
                                                                    'Taxi')
					THEN 'Shared Ride 3+'
					ELSE [mode_trip].[mode_trip_description]
					END AS [assignment_mode] -- recode trip modes to assignment modes
			,[value_of_time_category_id]
            ,CASE WHEN [mode_trip].[mode_trip_description] = 'Drive Alone'
                    THEN [transponder_available_id]
                    ELSE 0 END AS [transponder_available_id]  -- only drive alone trips use transponder for assignment
			,[time_trip_start].[trip_start_abm_5_tod]
			,[person_trip].[time_total]
			,[person_trip].[cost_toll_drive]
			,[person_trip].[weight_trip]
			,[person_trip].[weight_person_trip]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[model_trip]
		ON
			[person_trip].[model_trip_id] = [model_trip].[model_trip_id]
		INNER JOIN
			[dimension].[mode_trip]
		ON
			[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
		INNER JOIN
			[dimension].[tour]
		ON
			[person_trip].[scenario_id] = [tour].[scenario_id]
			AND [person_trip].[tour_id] = [tour].[tour_id]
		INNER JOIN
			[dimension].[purpose_tour]
		ON
			[tour].[purpose_tour_id] = [purpose_tour].[purpose_tour_id]
		INNER JOIN
			[dimension].[household]
		ON
			[person_trip].[scenario_id] = [household].[scenario_id]
			AND [person_trip].[household_id] = [household].[household_id]
		INNER JOIN
			[dimension].[person]
		ON
			[person_trip].[scenario_id] = [person].[scenario_id]
			AND [person_trip].[person_id] = [person].[person_id]
		INNER JOIN
			[dimension].[geography_trip_origin]
		ON
			[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
		INNER JOIN
			[dimension].[geography_trip_destination]
		ON
			[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
		INNER JOIN
			[dimension].[time_trip_start]
		ON
			[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
		WHERE
			[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [tour].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			-- resident trips that use synthetic population
			AND [model_trip].[model_trip_description] IN ('Individual',
														  'Internal-External',
														  'Joint')
			AND [mode_trip].[mode_trip_description] IN ('Drive Alone',
														'Heavy Heavy Duty Truck',
														'Light Heavy Duty Truck',
														'Medium Heavy Duty Truck',
                                                        'Non-Pooled TNC',
                                                        'Pooled TNC',
														'School Bus',
														'Shared Ride 2',
														'Shared Ride 3+',
														'Taxi') -- auto modes
	),
	[avg_alternate_trip_time] AS (
		-- calculate average trip time for all trips using alternate scenario skim travel times
			-- note there are base/build trips without alternate scenario skim travel times
			-- calculate the base/build average trip time under build/base skims where
				-- only base/build trips with alternate scenario travel times are included
				-- then divide this by total number of base/build trips with alternate scenario travel times
		SELECT
			[auto_resident_trips].[scenario_id]
			,-- total trip time for trips with alternative skims
				(SUM([auto_resident_trips].[weight_trip] * ISNULL([auto_skims].[time_total], 0))
				-- divided by number of trips with alternative skims
				/ SUM(CASE	WHEN [auto_skims].[time_total] IS NOT NULL
							THEN [auto_resident_trips].[weight_trip]
							ELSE 0 END)) AS [time_total_avg]
		FROM
			[auto_resident_trips]
		LEFT OUTER JOIN
			[auto_skims]
		ON
			[auto_resident_trips].[scenario_id] != [auto_skims].[scenario_id] -- match base trips with build skims and vice versa
			-- auto trips match at taz_13 geography
			AND [auto_resident_trips].[trip_origin_taz_13] = [auto_skims].[trip_origin_taz_13]
			AND [auto_resident_trips].[trip_destination_taz_13] = [auto_skims].[trip_destination_taz_13]
			AND [auto_resident_trips].[assignment_mode] = [auto_skims].[assignment_mode]
			AND [auto_resident_trips].[value_of_time_category_id] = [auto_skims].[value_of_time_category_id]
            AND [auto_resident_trips].[transponder_available_id] = [auto_skims].[transponder_available_id]
			AND [auto_resident_trips].[trip_start_abm_5_tod] = [auto_skims].[trip_start_abm_5_tod]
		GROUP BY
			[auto_resident_trips].[scenario_id]
	),
	[results_table] AS (
		SELECT
			[auto_resident_trips].[scenario_id]
			,[auto_resident_trips].[purpose_tour_description]
			,[auto_resident_trips].[persons_coc]
			,[auto_resident_trips].[persons_senior]
			,[auto_resident_trips].[persons_minority]
			,[auto_resident_trips].[persons_low_income]
			 -- split toll cost within scenarios amongst all trip participants
			,SUM(([auto_resident_trips].[weight_trip] * [auto_resident_trips].[cost_toll_drive]) / [auto_resident_trips].[weight_person_trip]) AS [cost_toll]
			-- trip value of time cost for person trips with their own skims
			,SUM([auto_resident_trips].[weight_person_trip] * [auto_resident_trips].[time_total] *
					CASE	WHEN [auto_resident_trips].[purpose_tour_description] = 'Work'
							THEN @vot_commute / 60
							WHEN [auto_resident_trips].[purpose_tour_description] != 'Work'
							THEN @vot_non_commute / 60
							ELSE NULL END) AS [cost_vot]
			-- person trip value of time cost for trips with alternative skims
			-- substitute average alternative skim time for the trip if no alternative skim is present
			,(SUM([auto_resident_trips].[weight_person_trip] * ISNULL([auto_skims].[time_total], [avg_alternate_trip_time].[time_total_avg]) *
				CASE	WHEN [auto_resident_trips].[purpose_tour_description] = 'Work'
						THEN @vot_commute / 60
						WHEN [auto_resident_trips].[purpose_tour_description] != 'Work'
						THEN @vot_non_commute / 60
						ELSE NULL END)) AS [alternate_cost_vot]
			,SUM([auto_resident_trips].[weight_person_trip]) AS [person_trips]
		FROM
			[auto_resident_trips]
		LEFT OUTER JOIN
			[auto_skims]
		ON
			[auto_resident_trips].[scenario_id] != [auto_skims].[scenario_id] -- match base trips with build skims and vice versa
			-- auto trips match at taz_13 geography
			AND [auto_resident_trips].[trip_origin_taz_13] = [auto_skims].[trip_origin_taz_13]
			AND [auto_resident_trips].[trip_destination_taz_13] = [auto_skims].[trip_destination_taz_13]
			AND [auto_resident_trips].[assignment_mode] = [auto_skims].[assignment_mode]
			AND [auto_resident_trips].[value_of_time_category_id] = [auto_skims].[value_of_time_category_id]
            AND [auto_resident_trips].[transponder_available_id] = [auto_skims].[transponder_available_id]
			AND [auto_resident_trips].[trip_start_abm_5_tod] = [auto_skims].[trip_start_abm_5_tod]
		INNER JOIN
			[avg_alternate_trip_time]
		ON
			[auto_resident_trips].[scenario_id] = [avg_alternate_trip_time].[scenario_id]
		GROUP BY
			[auto_resident_trips].[scenario_id]
			,[auto_resident_trips].[purpose_tour_description]
			,[auto_resident_trips].[persons_coc]
			,[auto_resident_trips].[persons_senior]
			,[auto_resident_trips].[persons_minority]
			,[auto_resident_trips].[persons_low_income]
	)
	INSERT INTO @tbl_resident_trips_auto
	SELECT
		SUM([person_trips]) AS [person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [person_trips]
					ELSE 0 END) AS [base_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
                    THEN [person_trips]
					ELSE 0 END) AS [build_person_trips]
		,SUM(CASE	WHEN [persons_coc] = 1
					THEN [person_trips]
					ELSE 0 END) AS [coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [persons_coc] = 1
                    THEN [person_trips]
					ELSE 0 END) AS [base_coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [persons_coc] = 1
                    THEN [person_trips]
					ELSE 0 END) AS [build_coc_person_trips]
        ,-- 1/2 * all trips vot under base skims minus all trips vot under build skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                    THEN [alternate_cost_vot]
                    ELSE NULL END
        -- all trips under build skims
        - CASE	WHEN [scenario_id] = @scenario_id_base
                THEN [alternate_cost_vot]
                WHEN [scenario_id] = @scenario_id_build
                THEN [cost_vot]
                ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [benefit_vot]
		,-- 1/2 * all trips vot under base skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                    THEN [cost_vot]
                    WHEN [scenario_id] = @scenario_id_build
                    THEN [alternate_cost_vot]
                    ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [base_vot]
		,-- 1/2 * all trips vot under build skims
        -- all trips under base skims
        SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [alternate_cost_vot]
					WHEN [scenario_id] = @scenario_id_build
					THEN [cost_vot]
					ELSE NULL END)
        -- multiplied by 1/2
        * .5 AS [build_vot]
		,-- 1/2 * work trips vot under base skims minus work trips vot under build skims
		-- work trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_benefit_vot]
		,-- 1/2 * non work trips vot under base skims minus non work trips trips vot under build skims
		-- non work trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_benefit_vot]
		,-- 1/2 * work coc person trips vot under base skims minus work coc trips vot under build skims
		-- work coc under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work coc trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_coc] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_coc] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_coc_benefit_vot]
		,-- 1/2 * non work coc person trips vot under base skims minus non work coc trips vot under build skims
		-- non work coc under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work coc trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_coc] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_coc] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_coc_benefit_vot]
		,-- 1/2 * work senior trips vot under base skims minus work senior trips vot under build skims
		-- work senior trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_senior] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_senior] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work senior trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_senior] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_senior] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_senior_benefit_vot]
		,-- 1/2 * non work senior trips vot under base skims minus non work senior trips vot under build skims
		-- non work senior trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_senior] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_senior] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work senior trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_senior] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_senior] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_senior_benefit_vot]
		,-- 1/2 * work minority trips vot under base skims minus work minority trips vot under build skims
		-- work minority trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_minority] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_minority] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work minority trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_minority] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_minority] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_minority_benefit_vot]
		,-- 1/2 * non work minority trips vot under base skims minus non work minority trips vot under build skims
		-- non work minority trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_minority] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_minority] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work minority trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_minority] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_minority] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_minority_benefit_vot]
		,-- 1/2 * work low income trips vot under base skims minus work low income trips vot under build skims
		-- work low income trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_low_income] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_low_income] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work low income trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_low_income] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_low_income] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_low_income_benefit_vot]
		,-- 1/2 * non work low income trips vot under base skims minus non work low income trips vot under build skims
		-- non work low income trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_low_income] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_low_income] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work low income trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_low_income] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_low_income] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_low_income_benefit_vot]
		,-- toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [cost_toll]
					ELSE 0 END) AS [base_cost_toll]
		,-- toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
					THEN [cost_toll]
					ELSE 0 END) AS [build_cost_toll]
		,-- work toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
					THEN [cost_toll]
					ELSE 0 END) AS [work_base_cost_toll]
		,-- work toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
					THEN [cost_toll]
					ELSE 0 END) AS [work_build_cost_toll]
		,-- non work toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
					THEN [cost_toll]
					ELSE 0 END) AS [non_work_base_cost_toll]
		,-- non work toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
					THEN [cost_toll]
					ELSE 0 END) AS [non_work_build_cost_toll]
		,-- work coc persons toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_toll]
					ELSE 0 END) AS [work_coc_base_cost_toll]
		,-- work coc persons toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_toll]
					ELSE 0 END) AS [work_coc_build_cost_toll]
		,-- non work coc persons toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_toll]
					ELSE 0 END) AS [non_work_coc_base_cost_toll]
		,-- non work coc persons toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_toll]
					ELSE 0 END) AS [non_work_coc_build_cost_toll]
	FROM
		[results_table]

	RETURN
END
GO

-- Add metadata for [bca].[fn_resident_trips_auto]
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_auto', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_auto', 'MS_Description', 'function to return auto resident trips value of time costs under alternative skims'
GO




-- Create transit resident trips table valued function
DROP FUNCTION IF EXISTS [bca].[fn_resident_trips_transit]
GO

CREATE FUNCTION [bca].[fn_resident_trips_transit]
(
	@scenario_id_base integer,
	@scenario_id_build integer,
	@vot_commute float, -- value of time in $/hour for Work tour purpose trips
	@vot_non_commute float, -- value of time in $/hour for Non-Work tour purpose trips
	@ovt_weight float -- per minute weight of out-of-vehicle time in transit trips
)
RETURNS @tbl_resident_trips_transit TABLE
(
	[person_trips] float NOT NULL
	,[base_person_trips] float NOT NULL
	,[build_person_trips] float NOT NULL
	,[coc_person_trips] float NOT NULL
	,[base_coc_person_trips] float NOT NULL
	,[build_coc_person_trips] float NOT NULL
	,[benefit_vot] float NOT NULL
	,[base_vot] float NOT NULL
	,[build_vot] float NOT NULL
	,[work_benefit_vot] float NOT NULL
	,[non_work_benefit_vot] float NOT NULL
	,[work_coc_benefit_vot] float NOT NULL
	,[non_work_coc_benefit_vot] float NOT NULL
	,[work_senior_benefit_vot] float NOT NULL
	,[non_work_senior_benefit_vot] float NOT NULL
	,[work_minority_benefit_vot] float NOT NULL
	,[non_work_minority_benefit_vot] float NOT NULL
	,[work_low_income_benefit_vot] float NOT NULL
	,[non_work_low_income_benefit_vot] float NOT NULL
	,[base_cost_transit] float NOT NULL
	,[build_cost_transit] float NOT NULL
	,[work_base_cost_transit] float NOT NULL
	,[work_build_cost_transit] float NOT NULL
	,[non_work_base_cost_transit] float NOT NULL
	,[non_work_build_cost_transit] float NOT NULL
	,[work_coc_base_cost_transit] float NOT NULL
	,[work_coc_build_cost_transit] float NOT NULL
	,[non_work_coc_base_cost_transit] float NOT NULL
	,[non_work_coc_build_cost_transit] float NOT NULL
)
AS
-- ===========================================================================
-- Author:		RSG and Gregor Schroeder
-- Create date: 7/24/2018
-- Updated: 12/4/2020 for ABM 14.2.1 new trip modes, new modes, transit fare
--   cost field rename, removing household and person weight fields and adding
--   where clause to remove NA household and person records
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- input values of time for Work and Non-Work tour purpose trips, and an input
-- weight parameter for transit out of vehicle time, returns table of value of
-- time costs for the build scenario given build scenario travel times and
-- base scenario travel times for transit resident trips and a benefits
-- calculation of
-- 1/2 * (all trips under base scenario travel times - all trips under build scenario travel times)
-- Note that build trips without matching base scenario travel times are
-- assigned estimated base scenario travel times. Then all trips still without
-- alternate scenario travel times are assigned the average per trip travel
-- time of trips with alternate travel times.
--	[dbo].[run_person_trip_processor]
--	[dbo].[run_person_trip_summary]
-- ===========================================================================
BEGIN
	with [transit_resident_trips] AS (
		-- get trip list for base and build scenario of all resident trips
		-- that use the synthetic population
		-- this includes Individual, Internal-External, and Joint models
		-- restrict to transit modes
		-- skims are segmented by mgra-mgra, assignment mode, and ABM 5 time of day
		-- the geography surrogate keys include taz-taz od pairs to account for external zones
		SELECT
			[person_trip].[scenario_id]
			,[purpose_tour].[purpose_tour_description]
			,CASE	WHEN [person].[age] >= 75 THEN 1
					WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
					WHEN [household].[poverty] <= 2 THEN 1
					ELSE 0 END AS [persons_coc]
			,CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END AS [persons_senior]
			,CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
						OR [person].[hispanic] = 'Hispanic' THEN 1
					ELSE 0 END AS [persons_minority]
			,CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END AS [persons_low_income]
			-- at trips match at skims geographies, no need for aggregation
			,[person_trip].[geography_trip_origin_id]
			,[person_trip].[geography_trip_destination_id]
			-- include taz geographies for later match to estimated transit skims
			-- for build trips with no base skims using auto skims
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
			-- all trip modes are directly mapped to assignment modes
			-- excepting TNC to Transit is treated exactly like Kiss and Ride to Transit
			,CASE WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus and Premium Transit'
                  THEN 'Kiss and Ride to Transit - Local Bus and Premium Transit'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus'
                  THEN 'Kiss and Ride to Transit - Local Bus'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Premium Transit'
                  THEN 'Kiss and Ride to Transit - Premium Transit'
                  ELSE [mode_trip].[mode_trip_description] END AS [assignment_mode]
			,[time_trip_start].[trip_start_abm_5_tod]
			-- include transit in vehicle time
			,[person_trip].[time_transit_in_vehicle]
			-- include transit out vehicle time
			,[person_trip].[time_total]
				- ([person_trip].[time_transit_in_vehicle] + [person_trip].[time_drive])
				AS [time_transit_out_vehicle]
			,[person_trip].[time_total]
			,[person_trip].[weight_trip]
			,[person_trip].[weight_person_trip]
			,[person_trip].[cost_fare_transit]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[model_trip]
		ON
			[person_trip].[model_trip_id] = [model_trip].[model_trip_id]
		INNER JOIN
			[dimension].[mode_trip]
		ON
			[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
		INNER JOIN
			[dimension].[tour]
		ON
			[person_trip].[scenario_id] = [tour].[scenario_id]
			AND [person_trip].[tour_id] = [tour].[tour_id]
		INNER JOIN
			[dimension].[purpose_tour]
		ON
			[tour].[purpose_tour_id] = [purpose_tour].[purpose_tour_id]
		INNER JOIN
			[dimension].[household]
		ON
			[person_trip].[scenario_id] = [household].[scenario_id]
			AND [person_trip].[household_id] = [household].[household_id]
		INNER JOIN
			[dimension].[person]
		ON
			[person_trip].[scenario_id] = [person].[scenario_id]
			AND [person_trip].[person_id] = [person].[person_id]
		INNER JOIN
			[dimension].[geography_trip_origin]
		ON
			[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
		INNER JOIN
			[dimension].[geography_trip_destination]
		ON
			[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
		INNER JOIN
			[dimension].[time_trip_start]
		ON
			[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
		WHERE
			[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [tour].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			AND [person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			-- resident trips that use synthetic population
			AND [model_trip].[model_trip_description] IN ('Individual',
														  'Internal-External',
														  'Joint')
			-- transit modes only
			AND [mode_trip].[mode_trip_description] IN ('Kiss and Ride to Transit - Local Bus and Premium Transit',
														'Kiss and Ride to Transit - Local Bus',
														'Kiss and Ride to Transit - Premium Transit',
														'Park and Ride to Transit - Local Bus and Premium Transit',
														'Park and Ride to Transit - Local Bus',
														'Park and Ride to Transit - Premium Transit',
                                                        'TNC to Transit - Local Bus and Premium Transit',
														'TNC to Transit - Local Bus',
														'TNC to Transit - Premium Transit',
														'Walk to Transit - Local Bus and Premium Transit',
														'Walk to Transit - Local Bus',
														'Walk to Transit - Premium Transit')
	),
	[transit_skims] AS (
		-- create base and build scenario transit skims from person trips table
		-- skims are segmented by mgra-mgra, assignment mode, and ABM 5 time of day
		-- the geography surrogate keys include taz-taz od pairs to account for external zones
		-- if a trip is not present in the person trips table corresponding to a skim then the skim
		-- is not present here
		SELECT
			[person_trip].[scenario_id]
			-- transit trips match at skims geographies, no need for aggregation
			,[person_trip].[geography_trip_origin_id]
			,[person_trip].[geography_trip_destination_id]
			-- all trip modes are directly mapped to assignment modes
			-- excepting TNC to Transit is treated exactly like Kiss and Ride to Transit
			,CASE WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus and Premium Transit'
                  THEN 'Kiss and Ride to Transit - Local Bus and Premium Transit'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus'
                  THEN 'Kiss and Ride to Transit - Local Bus'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Premium Transit'
                  THEN 'Kiss and Ride to Transit - Premium Transit'
                  ELSE [mode_trip].[mode_trip_description] END AS [assignment_mode]
			,[time_trip_start].[trip_start_abm_5_tod]
			-- include transit in vehicle time
			-- use trip weights here instead of person trips weights as this is in line with assignment
			,SUM(([person_trip].[weight_trip] * [person_trip].[time_transit_in_vehicle]))
				/ SUM([person_trip].[weight_trip]) AS [time_transit_in_vehicle]
			-- include transit out vehicle time
			-- use trip weights here instead of person trips weights as this is in line with assignment
			,SUM(([person_trip].[weight_trip]
					* ([person_trip].[time_total] - ([person_trip].[time_transit_in_vehicle] + [person_trip].[time_drive]))))
				/ SUM([person_trip].[weight_trip])
				AS [time_transit_out_vehicle]
			-- use trip weights here instead of person trips weights as this is in line with assignment
			,SUM(([person_trip].[weight_trip] * [person_trip].[time_total]))
				/ SUM([person_trip].[weight_trip]) AS [time_total]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[mode_trip]
		ON
			[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
		INNER JOIN
			[dimension].[time_trip_start]
		ON
			[person_trip].[time_trip_start_id] = [time_trip_start].[time_trip_start_id]
		WHERE
			[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
			-- active transportation modes only
			AND [mode_trip].[mode_trip_description] IN ('Kiss and Ride to Transit - Local Bus and Premium Transit',
														'Kiss and Ride to Transit - Local Bus',
														'Kiss and Ride to Transit - Premium Transit',
														'Park and Ride to Transit - Local Bus and Premium Transit',
														'Park and Ride to Transit - Local Bus',
														'Park and Ride to Transit - Premium Transit',
                                                        'TNC to Transit - Local Bus and Premium Transit',
														'TNC to Transit - Local Bus',
														'TNC to Transit - Premium Transit',
														'Walk to Transit - Local Bus and Premium Transit',
														'Walk to Transit - Local Bus',
														'Walk to Transit - Premium Transit')
		GROUP BY
			[person_trip].[scenario_id]
			,[person_trip].[geography_trip_origin_id]
			,[person_trip].[geography_trip_destination_id]
			,CASE WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus and Premium Transit'
                  THEN 'Kiss and Ride to Transit - Local Bus and Premium Transit'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Local Bus'
                  THEN 'Kiss and Ride to Transit - Local Bus'
                  WHEN [mode_trip].[mode_trip_description] = 'TNC to Transit - Premium Transit'
                  THEN 'Kiss and Ride to Transit - Premium Transit'
                  ELSE [mode_trip].[mode_trip_description] END
			,[time_trip_start].[trip_start_abm_5_tod]
		HAVING
			SUM([person_trip].[weight_trip]) > 0
	),
	[estimated_base_transit_skims] AS (
		-- create base scenario estimated transit skims from person trips table
		-- use for build scenario resident transit trips with no base scenario skims
		--  by taking average base skim value weighted by person trips
		-- skims are segmented only by taz-taz to ensure maxiumum coverage
		-- if a trip is not present in the person trips table corresponding to a skim
		-- then the skim is not present here
		SELECT
			[person_trip].[scenario_id]
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
			-- use trip weights here instead of person trips weights as this is in line with assignment
			-- walk to transit estimated total time requires both distance and time for calculation
			-- average travel time weighted by trips
			,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip])
			-- multiplied by 19.7 * average travel distance weighted by trips raised to the power of -.362
				* (19.7 * POWER(SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]), -.362))
				AS [estimated_walk_transit_time_total]
			-- use trip weights here instead of person trips weights as this is in line with assignment
			-- drive to transit estimated total time requires just time for calculation
			-- average travel time weighted by trips
			,SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip])
			-- multiplied by 12.653 * average travel distance weighted by person trips raised to the power of -.362
				* (12.653 * POWER(SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]), -.358))
				AS [estimated_drive_transit_time_total]
		FROM
			[fact].[person_trip]
		INNER JOIN
			[dimension].[mode_trip]
		ON
			[person_trip].[mode_trip_id] = [mode_trip].[mode_trip_id]
		INNER JOIN
			[dimension].[geography_trip_origin]
		ON
			[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
		INNER JOIN
			[dimension].[geography_trip_destination]
		ON
			[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
		WHERE
			[person_trip].[scenario_id] = @scenario_id_base
			AND [mode_trip].[mode_trip_description] IN ('Drive Alone',
														'Heavy Heavy Duty Truck',
														'Light Heavy Duty Truck',
														'Medium Heavy Duty Truck',
                                                        'Non-Pooled TNC',
                                                        'Pooled TNC',
														'School Bus',
														'Shared Ride 2',
														'Shared Ride 3+',
														'Taxi') -- auto modes
		GROUP BY
			[person_trip].[scenario_id]
			,[geography_trip_origin].[trip_origin_taz_13]
			,[geography_trip_destination].[trip_destination_taz_13]
		HAVING
			SUM([person_trip].[weight_trip]) > 0
			AND SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) > 0
	),
	[avg_alternate_trip_time] AS (
		SELECT
			[transit_resident_trips].[scenario_id]
			-- the average base/build scenario travel time using alternate scenario skim values
			-- get the base/build scenario travel time using alternate scenario skim values
			,SUM(CASE
				-- the base/build scenario travel time using alternate scenario skim values
				-- only where there exists an alternate scenario skim value
				-- weight out of vehicle transit time using input parameter
				WHEN [transit_skims].[time_transit_out_vehicle] IS NOT NULL
					AND [transit_skims].[time_transit_in_vehicle] IS NOT NULL
				THEN [transit_resident_trips].[weight_trip]
						* (@ovt_weight * [transit_skims].[time_transit_out_vehicle] + [transit_skims].[time_transit_in_vehicle])
				-- the build scenario travel time using estimated alternate scenario skim values
				-- for the drive to transit modes
				-- only where there exists an estimated alternate scenario skim value
				-- cannot weight out of vehicle time as the estimate is for total time only
				WHEN [transit_resident_trips].[scenario_id] = @scenario_id_build
					AND ([transit_skims].[time_transit_out_vehicle] IS NULL OR [transit_skims].[time_transit_in_vehicle] IS NULL)
					AND [estimated_base_transit_skims].[estimated_drive_transit_time_total] IS NOT NULL
					AND [transit_resident_trips].[assignment_mode] IN ('Kiss and Ride to Transit - Local Bus and Premium Transit',
																	   'Kiss and Ride to Transit - Local Bus',
																	   'Kiss and Ride to Transit - Premium Transit',
																	   'Park and Ride to Transit - Local Bus and Premium Transit',
																	   'Park and Ride to Transit - Local Bus',
																	   'Park and Ride to Transit - Premium Transit')
				THEN [transit_resident_trips].[weight_trip]
						* [estimated_base_transit_skims].[estimated_drive_transit_time_total]
				-- the build scenario travel time using estimated alternate scenario skim values
				-- for the walk to transit modes
				-- only where there exists an estimated alternate scenario skim value
				-- cannot weight out of vehicle time as the estimate is for total time only
				WHEN [transit_resident_trips].[scenario_id] = @scenario_id_build
					AND ([transit_skims].[time_transit_out_vehicle] IS NULL OR [transit_skims].[time_transit_in_vehicle] IS NULL)
					AND [estimated_base_transit_skims].[estimated_walk_transit_time_total] IS NOT NULL
					AND [transit_resident_trips].[assignment_mode] IN ('Walk to Transit - Local Bus and Premium Transit',
																	   'Walk to Transit - Local Bus',
																	   'Walk to Transit - Premium Transit')
				THEN [transit_resident_trips].[weight_trip]
						* [estimated_base_transit_skims].[estimated_walk_transit_time_total]
				ELSE NULL
				END) /
				-- divided by the number of trips with alternate scenario skim values
				-- or estimated alternate scenario skim values (build trips only)
				SUM(CASE	WHEN ([transit_skims].[time_transit_out_vehicle] IS NOT NULL
								  AND [transit_skims].[time_transit_in_vehicle] IS NOT NULL)
								OR ([estimated_base_transit_skims].[estimated_drive_transit_time_total] IS NOT NULL
									AND [estimated_base_transit_skims].[estimated_walk_transit_time_total] IS NOT NULL)
							THEN [transit_resident_trips].[weight_trip]
							ELSE 0 END) AS [avg_alternate_trip_time]
		FROM
			[transit_resident_trips]
		LEFT OUTER JOIN
			[transit_skims]
		ON
			[transit_resident_trips].[scenario_id] != [transit_skims].[scenario_id] -- match base trips with build skims and vice versa
			-- transit trips match at skims geographies, no need for aggregation
			AND [transit_resident_trips].[geography_trip_origin_id] = [transit_skims].[geography_trip_origin_id]
			AND [transit_resident_trips].[geography_trip_destination_id] = [transit_skims].[geography_trip_destination_id]
			AND [transit_resident_trips].[assignment_mode] = [transit_skims].[assignment_mode]
			AND [transit_resident_trips].[trip_start_abm_5_tod] = [transit_skims].[trip_start_abm_5_tod]
		LEFT OUTER JOIN
			[estimated_base_transit_skims]
		ON
			[transit_resident_trips].[scenario_id] != [estimated_base_transit_skims].[scenario_id] -- match build trips with estimated base skims
			AND [transit_resident_trips].[trip_origin_taz_13] = [estimated_base_transit_skims].[trip_origin_taz_13]
			AND [transit_resident_trips].[trip_destination_taz_13] = [estimated_base_transit_skims].[trip_destination_taz_13]
		GROUP BY
			[transit_resident_trips].[scenario_id]),
	[results_table] AS (
		SELECT
			[transit_resident_trips].[scenario_id]
			,[transit_resident_trips].[purpose_tour_description]
			,[transit_resident_trips].[persons_coc]
			,[transit_resident_trips].[persons_senior]
			,[transit_resident_trips].[persons_minority]
			,[transit_resident_trips].[persons_low_income]
			-- the base/build scenario vot cost using their own skim values
			-- weight out of vehicle transit time using input parameter
			,SUM([transit_resident_trips].[weight_person_trip]
				* (@ovt_weight * [transit_resident_trips].[time_transit_out_vehicle] + [transit_resident_trips].[time_transit_in_vehicle])
				* CASE	WHEN [transit_resident_trips].[purpose_tour_description] = 'Work'
						THEN @vot_commute / 60
						WHEN [transit_resident_trips].[purpose_tour_description] != 'Work'
						THEN @vot_non_commute / 60
						ELSE NULL END) AS [cost_vot]
			-- the base/build scenario vot cost using alternate scenario skim values
			,SUM(CASE
					-- the base/build scenario vot cost using alternate scenario skim values
					-- only where there exists an alternate scenario skim value
					-- weight out of vehicle transit time using input parameter
					WHEN [transit_skims].[time_transit_out_vehicle] IS NOT NULL
						AND [transit_skims].[time_transit_in_vehicle] IS NOT NULL
					THEN [transit_resident_trips].[weight_person_trip]
							* (@ovt_weight * [transit_skims].[time_transit_out_vehicle] + [transit_skims].[time_transit_in_vehicle])
							* CASE	WHEN [transit_resident_trips].[purpose_tour_description] = 'Work'
									THEN @vot_commute / 60
									WHEN [transit_resident_trips].[purpose_tour_description] != 'Work'
									THEN @vot_non_commute / 60
									ELSE NULL END
					-- the build scenario vot cost using estimated alternate scenario skim values
					-- for the drive to transit modes
					-- only where there exists an estimated alternate scenario skim value
					-- cannot weight out of vehicle time as the estimate is for total time only
					WHEN [transit_resident_trips].[scenario_id] = @scenario_id_build
						AND ([transit_skims].[time_transit_out_vehicle] IS NULL OR [transit_skims].[time_transit_in_vehicle] IS NULL)
						AND [estimated_base_transit_skims].[estimated_drive_transit_time_total] IS NOT NULL
						AND [transit_resident_trips].[assignment_mode] IN ('Kiss and Ride to Transit - Local Bus and Premium Transit',
																		   'Kiss and Ride to Transit - Local Bus',
																		   'Kiss and Ride to Transit - Premium Transit',
																		   'Park and Ride to Transit - Local Bus and Premium Transit',
																		   'Park and Ride to Transit - Local Bus',
																		   'Park and Ride to Transit - Premium Transit')
					THEN [transit_resident_trips].[weight_person_trip]
							* [estimated_base_transit_skims].[estimated_drive_transit_time_total]
							* CASE	WHEN [transit_resident_trips].[purpose_tour_description] = 'Work'
									THEN @vot_commute / 60
									WHEN [transit_resident_trips].[purpose_tour_description] != 'Work'
									THEN @vot_non_commute / 60
									ELSE NULL END
					-- the build scenario vot cost using estimated alternate scenario skim values
					-- for the walk to transit modes
					-- only where there exists an estimated alternate scenario skim value
					-- cannot weight out of vehicle time as the estimate is for total time only
					WHEN [transit_resident_trips].[scenario_id] = @scenario_id_build
						AND ([transit_skims].[time_transit_out_vehicle] IS NULL OR [transit_skims].[time_transit_in_vehicle] IS NULL)
						AND [estimated_base_transit_skims].[estimated_walk_transit_time_total] IS NOT NULL
						AND [transit_resident_trips].[assignment_mode] IN ('Walk to Transit - Local Bus and Premium Transit',
																		   'Walk to Transit - Local Bus',
																		   'Walk to Transit - Premium Transit')
					THEN [transit_resident_trips].[weight_person_trip]
							* [estimated_base_transit_skims].[estimated_walk_transit_time_total]
							* CASE	WHEN [transit_resident_trips].[purpose_tour_description] = 'Work'
									THEN @vot_commute / 60
									WHEN [transit_resident_trips].[purpose_tour_description] != 'Work'
									THEN @vot_non_commute / 60
									ELSE NULL END
					 -- if no matched skims or estimated base skims for build trips use the average alternate skim
					ELSE [transit_resident_trips].[weight_person_trip] * [avg_alternate_trip_time].[avg_alternate_trip_time]
						* CASE	WHEN [transit_resident_trips].[purpose_tour_description] = 'Work'
									THEN @vot_commute / 60
									WHEN [transit_resident_trips].[purpose_tour_description] != 'Work'
									THEN @vot_non_commute / 60
									ELSE NULL END
					END) AS [alternate_cost_vot]
				,SUM([transit_resident_trips].[weight_person_trip]) AS [person_trips]
				-- transit costs are shared by all participants
				,SUM([transit_resident_trips].[weight_person_trip] * [transit_resident_trips].[cost_fare_transit]) AS [cost_transit]
		FROM
			[transit_resident_trips]
		LEFT OUTER JOIN
			[transit_skims]
		ON
			[transit_resident_trips].[scenario_id] != [transit_skims].[scenario_id] -- match base trips with build skims and vice versa
			-- transit trips match at skims geographies, no need for aggregation
			AND [transit_resident_trips].[geography_trip_origin_id] = [transit_skims].[geography_trip_origin_id]
			AND [transit_resident_trips].[geography_trip_destination_id] = [transit_skims].[geography_trip_destination_id]
			AND [transit_resident_trips].[assignment_mode] = [transit_skims].[assignment_mode]
			AND [transit_resident_trips].[trip_start_abm_5_tod] = [transit_skims].[trip_start_abm_5_tod]
		LEFT OUTER JOIN
			[estimated_base_transit_skims]
		ON
			[transit_resident_trips].[scenario_id] != [estimated_base_transit_skims].[scenario_id] -- match build trips with estimated base skims
			AND [transit_resident_trips].[trip_origin_taz_13] = [estimated_base_transit_skims].[trip_origin_taz_13]
			AND [transit_resident_trips].[trip_destination_taz_13] = [estimated_base_transit_skims].[trip_destination_taz_13]
		INNER JOIN
			[avg_alternate_trip_time]
		ON
			[transit_resident_trips].[scenario_id] = [avg_alternate_trip_time].[scenario_id]
		GROUP BY
			[transit_resident_trips].[scenario_id]
			,[transit_resident_trips].[purpose_tour_description]
			,[transit_resident_trips].[persons_coc]
			,[transit_resident_trips].[persons_senior]
			,[transit_resident_trips].[persons_minority]
			,[transit_resident_trips].[persons_low_income])
	INSERT INTO @tbl_resident_trips_transit
	SELECT
		SUM([person_trips]) AS [person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [person_trips]
					ELSE 0 END) AS [base_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
					THEN [person_trips]
					ELSE 0 END) AS [build_person_trips]
		,SUM(CASE	WHEN [persons_coc] = 1
					THEN [person_trips]
					ELSE 0 END) AS [coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [persons_coc] = 1
					THEN [person_trips]
					ELSE 0 END) AS [base_coc_person_trips]
		,SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [persons_coc] = 1
					THEN [person_trips]
					ELSE 0 END) AS [build_coc_person_trips]
		,-- 1/2 * all trips vot under base skims minus all trips vot under build skims
		-- all trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- all trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [benefit_vot]
		,-- 1/2 * all trips vot under base skims
		-- all trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
					THEN [alternate_cost_vot]
					ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [base_vot]
		,-- 1/2 * all trips vot under build skims
		-- all trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [alternate_cost_vot]
					WHEN [scenario_id] = @scenario_id_build
					THEN [cost_vot]
					ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [build_vot]
		,-- 1/2 * work trips vot under base skims minus work trips vot under build skims
		-- work trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_benefit_vot]
		,-- 1/2 * non work trips vot under base skims minus non work trips trips vot under build skims
		-- non work trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_benefit_vot]
		,-- 1/2 * work coc person trips vot under base skims minus work coc trips vot under build skims
		-- work coc under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work coc trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_coc] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_coc] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_coc_benefit_vot]
		,-- 1/2 * non work coc person trips vot under base skims minus non work coc trips vot under build skims
		-- non work coc under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work coc trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_coc] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_coc] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_coc_benefit_vot]
		,-- 1/2 * work senior trips vot under base skims minus work senior trips vot under build skims
		-- work senior trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_senior] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_senior] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work senior trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_senior] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_senior] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_senior_benefit_vot]
		,-- 1/2 * non work senior trips vot under base skims minus non work senior trips vot under build skims
		-- non work senior trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_senior] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_senior] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work senior trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_senior] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_senior] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_senior_benefit_vot]
		,-- 1/2 * work minority trips vot under base skims minus work minority trips vot under build skims
		-- work minority trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_minority] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_minority] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work minority trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_minority] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_minority] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_minority_benefit_vot]
		,-- 1/2 * non work minority trips vot under base skims minus non work minority trips vot under build skims
		-- non work minority trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_minority] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_minority] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work minority trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_minority] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_minority] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_minority_benefit_vot]
		,-- 1/2 * work low income trips vot under base skims minus work low income trips vot under build skims
		-- work low income trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_low_income] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_low_income] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- work low income trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] = 'Work'
					AND [persons_low_income] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] = 'Work'
					AND [persons_low_income] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [work_low_income_benefit_vot]
		,-- 1/2 * non work low income trips vot under base skims minus non work low income trips vot under build skims
		-- non work low income trips under base skims
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_low_income] = 1
					THEN [cost_vot]
					WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_low_income] = 1
					THEN [alternate_cost_vot]
					ELSE NULL END
		-- non work low income trips under build skims
		- CASE	WHEN [scenario_id] = @scenario_id_base
					AND [purpose_tour_description] != 'Work'
					AND [persons_low_income] = 1
				THEN [alternate_cost_vot]
				WHEN [scenario_id] = @scenario_id_build
					AND [purpose_tour_description] != 'Work'
					AND [persons_low_income] = 1
				THEN [cost_vot]
				ELSE NULL END)
		-- multiplied by 1/2
		* .5 AS [non_work_low_income_benefit_vot]
		,-- toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
					THEN [cost_transit]
					ELSE 0 END) AS [base_cost_transit]
		,-- toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
					THEN [cost_transit]
					ELSE 0 END) AS [build_cost_transit]
		,-- work toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
					THEN [cost_transit]
					ELSE 0 END) AS [work_base_cost_transit]
		,-- work toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
					THEN [cost_transit]
					ELSE 0 END) AS [work_build_cost_transit]
		,-- non work toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
					THEN [cost_transit]
					ELSE 0 END) AS [non_work_base_cost_transit]
		,-- non work toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
					THEN [cost_transit]
					ELSE 0 END) AS [non_work_build_cost_transit]
		,-- work coc persons toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_transit]
					ELSE 0 END) AS [work_coc_base_cost_transit]
		,-- work coc persons toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] = 'Work'
						AND [persons_coc] = 1
					THEN [cost_transit]
					ELSE 0 END) AS [work_coc_build_cost_transit]
		,-- non work coc persons toll cost for the base scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_base
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_transit]
					ELSE 0 END) AS [non_work_coc_base_cost_transit]
		,-- non work coc persons toll cost for the build scenario
		SUM(CASE	WHEN [scenario_id] = @scenario_id_build
						AND [purpose_tour_description] != 'Work'
						AND [persons_coc] = 1
					THEN [cost_transit]
					ELSE 0 END) AS [non_work_coc_build_cost_transit]
	FROM
		[results_table]

	RETURN
END
GO

-- Add metadata for [bca].[fn_resident_trips_transit]
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_transit', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.fn_resident_trips_transit', 'MS_Description', 'function to return transit resident trips value of time costs under alternative skims'
GO




-- Create stored procedure to load emfac emissions output xlsx files
DROP PROCEDURE IF EXISTS [bca].[sp_load_emfac_output]
GO

CREATE PROCEDURE [bca].[sp_load_emfac_output]
	@scenario_id integer,
	@annual_emfac_path nvarchar(max), -- unc path to the emfac emissions program output workbook for the Annual Season
	@summer_emfac_path nvarchar(max), -- unc path to the emfac emissions program output workbook for the Summer Season
	@winter_emfac_path nvarchar(max) -- unc path to the emfac emissions program output workbook for the Winter Season
AS

/*	Author: Gregor Schroeder
	Date: 7/17/2018
	Description: Loads EMFAC emissions program output xlsx workbook data into the
		[bca].[emfac_output] table for a given input scenario for each
		Season (Annual,Summer,Winter) if the data has not already been loaded.
		Each EMFAC emissions program output workbook must contain the worksheet
		[Total SANDAG] and the columns ([Season],[Veh_Tech],,[CO2_TOTEX], [CO2_TOTEX],
		[NOx_TOTEX],[PM2_5_TOTAL],[PM10_TOTAL],[ROG_TOTAL],[SOx_TOTEX]) within that worksheet.
		One can specify file paths for all three seasons, a subset, or none at all
		depending if none, a subset, or all of the data has already been loaded
		into the [bca].[emfac_output] table.
		*/

-- in order to run OPENROWSET or OPENDATASOURCE the server must allow Ad Hoc Distriuted Queries
--https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/ad-hoc-distributed-queries-server-configuration-option?view=sql-server-2017
--sp_configure 'show advanced options', 1;
--RECONFIGURE;
--GO
--sp_configure 'Ad Hoc Distributed Queries', 1;
--RECONFIGURE;
--GO

-- in order to connect to Microsoft Excel using the ACE 12.0 Driver in a 64 bit instance of MSSQL
-- the following options must be turned on and the driver itself must be installed on the instance
--EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0' , N'AllowInProcess' , 1
--GO
--EXEC master.dbo.sp_MSset_oledb_prop N'Microsoft.ACE.OLEDB.12.0' , N'DynamicParameters' , 1
--GO

-- download the 64 bit driver here and install from the command prompt with the passive flag
-- passive flag is necessary in case the 32 bit version is already installed otherwise it will not allow installation
-- https://www.microsoft.com/en-us/download/details.aspx?id=13255

DECLARE @sql nvarchar(max)


-- Annual emfac data
-- if there is no data for the scenario_id in the [bca].[emfac_output] table for a given season
-- then insert data from the emfac output xlsx file for the given season
IF NOT EXISTS (SELECT TOP 1 [scenario_id] FROM [bca].[emfac_output] WHERE [scenario_id] = @scenario_id AND [Season] = 'Annual')
BEGIN
	-- insert emfac output xlsx data of interest into [bca].[emfac_output] table
	SET @sql = '
	INSERT INTO [bca].[emfac_output]
	SELECT
		' + CONVERT(nvarchar, @scenario_id) + ' AS [scenario_id]
		,[Season]
		,[Veh_Tech]
		,[CO_TOTEX]
		,[CO2_TOTEX]
		,[NOx_TOTEX]
		,[PM2_5_TOTAL]
		,[PM10_TOTAL]
		,[ROG_TOTAL]
		,[SOx_TOTEX]
	FROM
		OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',
				   ''Excel 12.0;Database=' + @annual_emfac_path + ';HDR=YES'',
				   ''SELECT * FROM [Total SANDAG$]'')'
	EXECUTE(@sql)
END
ELSE
	PRINT 'EMFAC emissions program output data for [scenario_id] = ' + CONVERT(nvarchar, @scenario_id) + ' has already been loaded for Annual Season'


-- Summer emfac data
-- if there is no data for the scenario_id in the [bca].[emfac_output] table for a given season
-- then insert data from the emfac output xlsx file for the given season
IF NOT EXISTS (SELECT TOP 1 [scenario_id] FROM [bca].[emfac_output] WHERE [scenario_id] = @scenario_id AND [Season] = 'Summer')
BEGIN
	-- insert emfac output xlsx data of interest into [bca].[emfac_output] table
	SET @sql = '
	INSERT INTO [bca].[emfac_output]
	SELECT
		' + CONVERT(nvarchar, @scenario_id) + ' AS [scenario_id]
		,[Season]
		,[Veh_Tech]
		,[CO_TOTEX]
		,[CO2_TOTEX]
		,[NOx_TOTEX]
		,[PM2_5_TOTAL]
		,[PM10_TOTAL]
		,[ROG_TOTAL]
		,[SOx_TOTEX]
	FROM
		OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',
				   ''Excel 12.0;Database=' + @summer_emfac_path + ';HDR=YES'',
				   ''SELECT * FROM [Total SANDAG$]'')'
	EXECUTE(@sql)
END
ELSE
	PRINT 'EMFAC emissions program output data for [scenario_id] = ' + CONVERT(nvarchar, @scenario_id) + ' has already been loaded for Summer Season'


-- Winter emfac data
-- if there is no data for the scenario_id in the [bca].[emfac_output] table for a given season
-- then insert data from the emfac output xlsx file for the given season
IF NOT EXISTS (SELECT TOP 1 [scenario_id] FROM [bca].[emfac_output] WHERE [scenario_id] = @scenario_id AND [Season] = 'Winter')
BEGIN
	-- insert emfac output xlsx data of interest into [bca].[emfac_output] table
	SET @sql = '
	INSERT INTO [bca].[emfac_output]
	SELECT
		' + CONVERT(nvarchar, @scenario_id) + ' AS [scenario_id]
		,[Season]
		,[Veh_Tech]
		,[CO_TOTEX]
		,[CO2_TOTEX]
		,[NOx_TOTEX]
		,[PM2_5_TOTAL]
		,[PM10_TOTAL]
		,[ROG_TOTAL]
		,[SOx_TOTEX]
	FROM
		OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',
				   ''Excel 12.0;Database=' + @winter_emfac_path + ';HDR=YES'',
				   ''SELECT * FROM [Total SANDAG$]'')'
	EXECUTE(@sql)
END
ELSE
	PRINT 'EMFAC emissions program output data for [scenario_id] = ' + CONVERT(nvarchar, @scenario_id) + ' has already been loaded for Winter Season'
GO

-- Add metadata for [bca].[sp_load_emfac_output]
EXECUTE [db_meta].[add_xp] 'bca.sp_load_emfac_output', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.sp_load_emfac_output', 'MS_Description', 'stored procedure to load emfac emissions program output xlsx files'
GO




-- Create stored procedure for resident telecommuters
DROP PROCEDURE IF EXISTS [bca].[sp_resident_telecommute_benefits]
GO

CREATE PROCEDURE [bca].[sp_resident_telecommute_benefits]
	@scenario_id_base integer,
	@scenario_id_build integer,
	@vot_commute float -- value of time ($/hr) of work purpose trips
AS
-- ===========================================================================
-- Author:		Gregor Schroeder
-- Create date: 9/25/2018
-- Updated: 12/4/2020 for ABM 14.2.1 removing household and person weight
--   fields and adding where clause to remove NA household and person records
-- Description:	Given two input scenario_id values and an input value of time
-- for Work purpose trips, returns value of time benefit of non-taken work
-- work tours for telecommuters. Non-taken work tour travel times for
-- telecommuters are assumed to be two times the average of all direct to work
-- trips (Home-Work, Work-Home) originating from the telecommuters home TAZ.
-- A benefits calculation of
-- 1/2 * (all trips under build scenario travel times - all trips under base scenario travel times)
-- is used for consistency with other BCA components.
-- Note that home TAZs without scenario travel times or alternate scenario
-- travel times are assigned the average scenario/alternate scenario travel
-- time of all home TAZs for the scenario.
-- ===========================================================================
-- #taz_work_trip_time
-- create base and build scenario average direct to work tour travel time
-- use only home-work and work-home trips for each home TAZ
-- this is the average one-way direct commute travel time for each home TAZ
-- multiplied by two
SELECT
	[person_trip].[scenario_id]
	,CASE	WHEN [purpose_trip_origin].[purpose_trip_origin_description] = 'Home'
			THEN [geography_trip_origin].[trip_origin_taz_13]
			WHEN [purpose_trip_destination].[purpose_trip_destination_description] = 'Home'
			THEN [geography_trip_destination].[trip_destination_taz_13]
			ELSE NULL END AS [taz_home] -- set home TAZ depending on direction of home-work trip
	-- use trip weights here instead of person trips weights as this is in line with assignment
	,2 * SUM(([person_trip].[weight_trip] * [person_trip].[time_total])) / SUM([person_trip].[weight_trip]) AS [time_total]
INTO
	#taz_work_tour_time
FROM
	[fact].[person_trip]
INNER JOIN
	[dimension].[purpose_trip_origin]
ON
	[person_trip].[purpose_trip_origin_id] = [purpose_trip_origin].[purpose_trip_origin_id]
INNER JOIN
	[dimension].[purpose_trip_destination]
ON
	[person_trip].[purpose_trip_destination_id] = [purpose_trip_destination].[purpose_trip_destination_id]
INNER JOIN
	[dimension].[geography_trip_origin]
ON
	[person_trip].[geography_trip_origin_id] = [geography_trip_origin].[geography_trip_origin_id]
INNER JOIN
	[dimension].[geography_trip_destination]
ON
	[person_trip].[geography_trip_destination_id] = [geography_trip_destination].[geography_trip_destination_id]
WHERE
	[person_trip].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
	AND ( -- direct home-work trips
			([purpose_trip_origin].[purpose_trip_origin_description] = 'Home'
				AND [purpose_trip_destination].[purpose_trip_destination_description] = 'Work') OR
			-- direct work-home trips
			([purpose_trip_origin].[purpose_trip_origin_description] = 'Work'
				AND [purpose_trip_destination].[purpose_trip_destination_description] = 'Home')
		)
GROUP BY
	[person_trip].[scenario_id]
	,CASE	WHEN [purpose_trip_origin].[purpose_trip_origin_description] = 'Home'
			THEN [geography_trip_origin].[trip_origin_taz_13]
			WHEN [purpose_trip_destination].[purpose_trip_destination_description] = 'Home'
			THEN [geography_trip_destination].[trip_destination_taz_13]
			ELSE NULL END
HAVING
	SUM([person_trip].[weight_trip]) > 0


-- #results_table
-- get the number of telecommuters by household TAZ
-- segmented by scenario and Community of Concern categories
-- and append the average direct to work tour travel time for each home TAZ
-- using the scenario's own skims and the alternate scenario's skims from #taz_work_tour_time
-- note some household TAZs may not have values in #taz_work_tour_time for these skims
-- they are populated later
SELECT
	[telecommute_persons].[scenario_id]
	,[telecommute_persons].[persons_coc]
	,[telecommute_persons].[persons_senior]
	,[telecommute_persons].[persons_minority]
	,[telecommute_persons].[persons_low_income]
	,[telecommute_persons].[persons]
	,[#taz_work_tour_time].[time_total]
	,[alternate_taz_work_trip_time].[time_total] AS [alternate_time_total]
INTO #results_table
FROM (
	-- get the number of telecommuters by household TAZ
	-- segmented by scenario and Community of Concern categories
	SELECT
		[person].[scenario_id]
		,[geography_household_location].[household_location_taz_13]
		,SUM(CASE	WHEN [person].[age] >= 75 THEN 1
					WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
							OR [person].[hispanic] = 'Hispanic' THEN 1
					WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END) AS [persons_coc]
		,SUM(CASE WHEN [person].[age] >= 75 THEN 1 ELSE 0 END) AS [persons_senior]
		,SUM(CASE	WHEN [person].[race] IN ('Some Other Race Alone',
												'Asian Alone',
												'Black or African American Alone',
												'Two or More Major Race Groups',
												'Native Hawaiian and Other Pacific Islander Alone',
												'American Indian and Alaska Native Tribes specified; or American Indian or Alaska Native, not specified and no other races')
						OR [person].[hispanic] = 'Hispanic' THEN 1
					ELSE 0 END) AS [persons_minority]
		,SUM(CASE WHEN [household].[poverty] <= 2 THEN 1 ELSE 0 END) AS [persons_low_income]
		,COUNT([person_id]) AS [persons]
	FROM
		[dimension].[person]
	INNER JOIN
		[dimension].[household]
	ON
		[person].[scenario_id] = [household].[scenario_id]
		AND [person].[household_id] = [household].[household_id]
	INNER JOIN
		[dimension].[geography_household_location]
	ON
		[household].[geography_household_location_id] = [geography_household_location].[geography_household_location_id]
	WHERE
		[person].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
		AND [household].[scenario_id] IN (@scenario_id_base, @scenario_id_build)
		AND [person].[work_segment] = 'Work from Home'  -- full-time telecommuters only, no partial telecommuters
		AND [person].[person_id] > 0  -- remove Not Applicable records
	GROUP BY
		[person].[scenario_id]
		,[geography_household_location].[household_location_taz_13]) AS [telecommute_persons]
LEFT OUTER JOIN -- keep home locations without direct to work tour travel times
	[#taz_work_tour_time]
ON
	[telecommute_persons].[scenario_id] = [#taz_work_tour_time].[scenario_id]
	AND [telecommute_persons].[household_location_taz_13] = [#taz_work_tour_time].[taz_home]
LEFT OUTER JOIN -- keep home locations without alternate scenario direct to work tour travel times
	[#taz_work_tour_time] AS [alternate_taz_work_trip_time]
ON
	[telecommute_persons].[scenario_id] != [alternate_taz_work_trip_time].[scenario_id] -- match base/build to build/base
	AND [telecommute_persons].[household_location_taz_13] = [alternate_taz_work_trip_time].[taz_home]


-- return the not-taken direct commute work tour benefits
SELECT
	-- 1/2 * all not-taken tours vot under build skims minus all not-taken tours vot under base skims
    -- all not-taken tours vot under build skims
	SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons] * [benefit_vot]
				ELSE NULL END
	-- minus all not-taken tours vot under base skims
	- CASE	WHEN [scenario_id] = @scenario_id_base
			THEN [persons] * [benefit_vot]
			WHEN [scenario_id] = @scenario_id_build
			THEN [persons] * [alternate_benefit_vot]
				ELSE NULL END)
	-- multiplied by 1/2
	* .5 AS [benefit_vot]
	,-- 1/2 * all not-taken tours vot under base skims
    -- not-taken tours vot under base skims
    SUM(CASE	WHEN [scenario_id] = @scenario_id_base
                THEN [persons] * [benefit_vot]
                WHEN [scenario_id] = @scenario_id_build
                THEN [persons] * [alternate_benefit_vot]
                ELSE NULL END)
    -- multiplied by 1/2
    * .5 AS [base_vot]
	,-- 1/2 * all not-taken tours vot under build skims
    -- all not-taken tours under base skims
    SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons] * [benefit_vot]
				ELSE NULL END)
    -- multiplied by 1/2
    * .5 AS [build_vot]
	-- 1/2 * all coc not-taken tours vot under build skims minus all coc not-taken tours vot under base skims
    -- all coc not-taken tours vot under build skims
	,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons_coc] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons_coc] * [benefit_vot]
				ELSE NULL END
	-- minus all coc not-taken tours vot under base skims
	- CASE	WHEN [scenario_id] = @scenario_id_base
			THEN [persons_coc] * [benefit_vot]
			WHEN [scenario_id] = @scenario_id_build
			THEN [persons_coc] * [alternate_benefit_vot]
				ELSE NULL END)
	-- multiplied by 1/2
	* .5 AS [coc_benefit_vot]
	-- 1/2 * all senior not-taken tours vot under build skims minus all senior not-taken tours vot under base skims
    -- all senior not-taken tours vot under build skims
	,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons_senior] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons_senior] * [benefit_vot]
				ELSE NULL END
	-- minus all senior not-taken tours vot under base skims
	- CASE	WHEN [scenario_id] = @scenario_id_base
			THEN [persons_senior] * [benefit_vot]
			WHEN [scenario_id] = @scenario_id_build
			THEN [persons_senior] * [alternate_benefit_vot]
				ELSE NULL END)
	-- multiplied by 1/2
	* .5 AS [senior_benefit_vot]
	-- 1/2 * all minority not-taken tours vot under build skims minus all minority not-taken tours vot under base skims
    -- all minority not-taken tours vot under build skims
	,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons_minority] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons_minority] * [benefit_vot]
				ELSE NULL END
	-- minus all minority not-taken tours vot under base skims
	- CASE	WHEN [scenario_id] = @scenario_id_base
			THEN [persons_minority] * [benefit_vot]
			WHEN [scenario_id] = @scenario_id_build
			THEN [persons_minority] * [alternate_benefit_vot]
				ELSE NULL END)
	-- multiplied by 1/2
	* .5 AS [minority_benefit_vot]
	-- 1/2 * all low_income not-taken tours vot under build skims minus all low_income not-taken tours vot under base skims
    -- all low_income not-taken tours vot under build skims
	,SUM(CASE	WHEN [scenario_id] = @scenario_id_base
				THEN [persons_low_income] * [alternate_benefit_vot]
				WHEN [scenario_id] = @scenario_id_build
				THEN [persons_low_income] * [benefit_vot]
				ELSE NULL END
	-- minus all low_income not-taken tours vot under base skims
	- CASE	WHEN [scenario_id] = @scenario_id_base
			THEN [persons_low_income] * [benefit_vot]
			WHEN [scenario_id] = @scenario_id_build
			THEN [persons_low_income] * [alternate_benefit_vot]
				ELSE NULL END)
	-- multiplied by 1/2
	* .5 AS [low_income_benefit_vot]
FROM (
	SELECT
		[#results_table].[scenario_id]
		,[#results_table].[persons_coc]
		,[#results_table].[persons_senior]
		,[#results_table].[persons_minority]
		,[#results_table].[persons_low_income]
		,[#results_table].[persons]
		-- telecommute non-taken direct to work tour value of time benefit using scenario's own skims
		-- if no skim use the average
		,ISNULL([#results_table].[time_total], [avg_trip_time].[time_total_avg]) * @vot_commute / 60 AS [benefit_vot]
		-- telecommute non-taken direct to work tour value of time benefit using alternate scenario's skims
		-- if no alternate scenario skim use the average alternate scenario skim
		,ISNULL([#results_table].[alternate_time_total], [avg_trip_time].[alternate_time_total_avg]) * @vot_commute / 60 AS [alternate_benefit_vot]
	FROM
		[#results_table]
	INNER JOIN ( -- get the average one-way direct commute travel time for each scenario under their own and alternate scenario skims
		SELECT
			[scenario_id]
			,SUM([persons] * [time_total]) / SUM([persons]) AS [time_total_avg]
			,SUM([persons] * [alternate_time_total]) / SUM([persons]) AS [alternate_time_total_avg]
		FROM
			#results_table
		GROUP BY
			[scenario_id]) AS [avg_trip_time]
	ON
		[#results_table].[scenario_id] = [avg_trip_time].[scenario_id]) AS [tt]
GO

-- Add metadata for [bca].[sp_resident_telecommute_benefits]
EXECUTE [db_meta].[add_xp] 'bca.sp_resident_telecommute_benefits', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.sp_resident_telecommute_benefits', 'MS_Description', 'stored procedure to return non-taken telecommuter work tour benefits under alternative skims'
GO

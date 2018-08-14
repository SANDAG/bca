USE [bca]
GO



CREATE PROCEDURE [bca].[run_comparison_year] @analysis_id integer, @scenario_year smallint
	WITH EXECUTE AS CALLER
AS
DECLARE @overall_start_date DATETIME = GETDATE();

-- ===========================================================================
-- Author:		RSG and Daniel Flyte
-- Create date: 8/13/2018
-- Description:	Translation of RSG stored procedure to run single-year base
--              vs. build comparisons. Runs comparison for each BCA component
--              and store results to scenario_comparison table in preparation
--              of subsequent multi-year analysis.
-- ===========================================================================

PRINT 'Started run_comparison_year(@analysis_id: ' + CAST(@analysis_id AS VARCHAR) +
    ', @scenario_year: ' + CAST(@scenario_year AS VARCHAR) + ') at ' +
	CONVERT(VARCHAR, @overall_start_date, 114);

RAISERROR ('', 10, 1)
WITH NOWAIT;

DECLARE @base_scenario_id  integer; -- base year		
DECLARE @build_scenario_id integer; -- build year	
DECLARE @ref_year          integer; -- reference (base) year

-- emissions variable
DECLARE @cost_winter_co    float;
DECLARE @cost_annual_PM2_5 float;
DECLARE @cost_summer_NOx   float;
DECLARE @cost_summer_ROG   float;
DECLARE @cost_annual_SOx   float;
DECLARE @cost_annual_PM10  float;
DECLARE @cost_annual_CO2   float;

DECLARE @auto_operating_cost float;

-- demographics variables
DECLARE @coc_age_threshold     integer;
DECLARE @coc_race_threshold    integer;
DECLARE @coc_poverty_threshold integer;

-- highway link variables
DECLARE @reliability_ratio float;
DECLARE @crash_cost_pdo    float;
DECLARE @crash_cost_injury float;
DECLARE @crash_cost_fatal  float;
DECLARE @crash_rate_pdo    float;
DECLARE @crash_rate_injury float;
DECLARE @crash_rate_fatal  float;
DECLARE @voc_auto          float;
DECLARE @voc_lhdt          float;
DECLARE @voc_mhdt          float;
DECLARE @voc_hhdt          float;
DECLARE @vor_auto          float;
DECLARE @vor_lhdt          float;
DECLARE @vor_mhdt          float;
DECLARE @vor_hhdt          float;


-- Look up base and build scenario IDs
SELECT
    @base_scenario_id     =   [scenario_id_base]
	,@build_scenario_id   =   [scenario_id_build]
    ,@cost_winter_co      =   [co2_value]
    ,@cost_annual_PM2_5   =   [pm2_5_value]
    ,@cost_summer_NOx     =   [nox_value]
    ,@cost_summer_ROG     =   [rog_value]
    ,@cost_annual_SOx     =   [so2_value]
    ,@cost_annual_PM10    =   [pm_10_value]
    ,@cost_annual_CO2     =   [co2_value]
    ,@auto_operating_cost =   [aoc_auto]
    ,@coc_age_threshold     =   [coc_age_thresh]
    ,@coc_race_threshold    =   [coc_race_thresh]
    ,@coc_poverty_threshold = [coc_poverty_thresh]
    ,@reliability_ratio = rel_ratio
    ,@crash_cost_pdo = crash_pdo_cost
    ,@crash_cost_injury  = crash_injury_cost
    ,@crash_cost_fatal  = crash_fatal_cost
    ,@crash_rate_pdo    = crash_rate_pdo
    ,@crash_rate_injury = crash_rate_injury
    ,@crash_rate_fatal  = crash_rate_fatal
    ,@voc_auto          = voc_auto
    ,@voc_lhdt          = voc_truck_light
    ,@voc_mhdt          = voc_truck_medium
    ,@voc_hhdt          = voc_truck_heavy
    ,@vor_auto          = vor_auto
    ,@vor_lhdt          = vor_truck_light
    ,@vor_mhdt          = vor_truck_medium
    ,@vor_hhdt          = vor_truck_heavy
 
FROM [bca].[analysis_parameters]
WHERE [analysis_id] = @analysis_id
AND [comparison_year] = @scenario_year;

-- Look up reference year
SELECT
    @ref_year = [year_reference]
FROM [bca].[analysis]
WHERE [analysis_id] = @analysis_id;

-- Execute stored procedures; some stored procedures aren't called in the reference year

-- Emissions
IF @scenario_year <> @ref_year
    UPDATE [bca].[scenario_comparison]
    SET
        [diff_pm25] = difference_Annual_PM2_5_TOTAL
        ,[diff_so2] = difference_Annual_SOx_TOTEX
        ,[diff_pm10] = difference_Annual_PM10_TOTAL
        ,[diff_co2] = difference_Annual_CO2_TOTEX
        ,[ben_pm25] = benefit_Annual_PM2_5_TOTAL
        ,[ben_so2]  = benefit_Annual_SOx_TOTEX
        ,[ben_pm10] = benefit_Annual_PM10_TOTAL
        ,[ben_co2] = benefit_Annual_CO2_TOTEX
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN [bca].[fn_emissions](
            @base_scenario_id
            ,@build_scenario_id
            ,@cost_winter_co
            ,@cost_annual_PM2_5
            ,@cost_summer_NOx
            ,@cost_summer_ROG
            ,@cost_annual_SOx
            ,@cost_annual_PM10
            ,@cost_annual_CO2)
    WHERE [scenario_comparison].[analysis_id] = @analysis_id
    AND [scenario_comparison].[scenario_year] = @scenario_year;


-- Auto ownership
IF @scenario_year <> @ref_year
    UPDATE [bca].[scenario_comparison]
    SET base_cost_autos_owned = [base_cost_auto_ownership]
        ,build_cost_autos_owned = [build_cost_auto_ownership]
        ,diff_autos_owned = [difference_auto_ownership]
        ,diff_autos_owned_coc = [difference_auto_ownership_coc]
        ,ben_autos_owned = [benefits_auto_ownership]
        ,ben_autos_owned_coc = [benefits_auto_ownership_coc]
        ,ben_autos_owned_coc_age = [benefits_auto_ownership_senior]
        ,ben_autos_owned_coc_race = [benefits_auto_ownership_minority]
        ,ben_autos_owned_coc_poverty = [benefits_auto_ownership_low_income]    
    FROM [bca].[scenario_comparison]
        CROSS JOIN [bca].[fn_auto_ownership](
            @base_scenario_id
            ,@build_scenario_id
            ,@auto_operating_cost)
    WHERE [analysis_id] = @analysis_id
    AND [scenario_year] = @scenario_year;


-- Demographics
UPDATE [bca].[scenario_comparison]
SET [persons] = [base_persons]
    ,[persons_coc] = [base_persons_coc]
    ,[persons_coc_race] = [base_persons_minority]
    ,[persons_coc_age] = [base_persons_senior]
	,[persons_coc_poverty] = [base_persons_low_income]
	,[coc_age_thresh] = @coc_age_threshold
	,[coc_race_thresh] = @coc_race_threshold
	,[coc_poverty_thresh] = @coc_poverty_threshold
FROM [bca].[scenario_comparison]
    CROSS JOIN [bca].[fn_demographics](@base_scenario_id, @build_scenario_id)
WHERE [analysis_id] = @analysis_id
AND [scenario_year] = @scenario_year;

-- Highway link analysis for personal and commercial vehicle trips, safety
--UPDATE [bca].[scenario_comparison]
--SET 
--SELECT *
--FROM [bca].[fn_highway_link](
--    @base_scenario_id
--    ,@build_scenario_id
--    ,@reliability_ratio
--    ,@crash_cost_pdo
--    ,@crash_cost_injury
--    ,@crash_cost_fatal
--    ,@crash_rate_pdo
--    ,@crash_rate_injury
--    ,@crash_rate_fatal
--    ,@voc_auto
--    ,@voc_lhdt
--    ,@voc_mhdt
--    ,@voc_hhdt
--    ,@vor_auto
--    ,@vor_lhdt
--    ,@vor_mhdt
--    ,@vor_hhdt);
    

--IF @scenario_year <> @ref_year
	--EXEC run_physical_activity_processor @base_scenario_id,
	--	@build_scenario_id,
	--	@analysis_id,
	--	@scenario_year

--IF @scenario_year <> @ref_year
	--EXEC run_aggregate_toll_processor @base_scenario_id,
	--	@build_scenario_id,
	--	@analysis_id,
	--	@scenario_year

--EXEC run_aggregate_trips_processor @base_scenario_id,
--	@build_scenario_id,
--	@analysis_id,
--	@scenario_year

--EXEC run_person_trip_processor @base_scenario_id,
--	@build_scenario_id,
--	@analysis_id,
--	@scenario_year

PRINT 'Finished run_comparison_year(@analysis_id: ' + CAST(@analysis_id AS VARCHAR) + ', @scenario_year: ' + CAST(@scenario_year AS VARCHAR) + ') at ' + + 
	CONVERT(VARCHAR, GETDATE(), 114) + '. Elapsed time: ' + CONVERT(VARCHAR, (GETDATE() - @overall_start_date), 114);

RAISERROR ('', 10, 1)
WITH NOWAIT;


GO




CREATE PROCEDURE [bca].[run_analysis_full]  @analysis_id INT, @scenario_year_forced INT = NULL
	WITH EXECUTE AS CALLER
AS
SET NOCOUNT ON;

-- ===========================================================================
-- Author:		RSG and Daniel Flyte
-- Create date: 8/13/2018
-- Description:	Translation and combination of the bca tool stored procedures
-- for the new abm database listed below. Given two input scenario_id values,
-- returns table of total auto mode toll costs segmented by base and build
-- scenarios for the CTM and Truck ABM sub-models.
--	[dbo].[run_aggregate_toll_comparison]
--	[dbo].[run_aggregate_toll_processor]
-- ===========================================================================

--Validate @analysis_id to avoid later foreign key constraint violations
IF NOT EXISTS (
    SELECT TOP 1 [id]
    FROM [bca].[analysis_parameters]
    WHERE [analysis_id] = @analysis_id
    AND ((@scenario_year_forced IS NULL) OR (@scenario_year_forced = [comparison_year]))
)
BEGIN
	DECLARE @error_string NVARCHAR(MAX) = CAST(SYSDATETIME() AS NVARCHAR) + N'  Analysis ID ' + CAST(@analysis_id AS NVARCHAR) + 
		N' does not exist in table [bca].[analysis_parameters] or else @scenario_year_forced (' + CAST(@scenario_year_forced AS NVARCHAR) +
        N') was passed in but could not be found for that analysis_id. Check analysis_id and scenario_year_forced (if passed) and try again.';

	RAISERROR (@error_string, 16,
			1
			)
	WITH NOWAIT;

	RETURN
END

-- Each analysis is associated with one or more comparison years. Evaluate one comparison year at a time
DECLARE @scenario_year smallint;

-- Create cursor; used to step through each comparison year  
DECLARE cur CURSOR LOCAL
FOR
    SELECT [comparison_year]
    FROM [bca].[analysis_parameters]
    WHERE [analysis_id] = @analysis_id
    AND ((@scenario_year_forced IS NULL) OR (@scenario_year_forced = [comparison_year]));
OPEN cur

FETCH NEXT
FROM cur
INTO @scenario_year;

--Run each comparison year in analysis
WHILE @@FETCH_STATUS = 0
BEGIN
	EXECUTE [bca].[run_comparison_year] @analysis_id, @scenario_year;

	FETCH NEXT
	FROM cur
	INTO @scenario_year;
END

CLOSE cur;

DEALLOCATE cur;

--Compile multiyear results
--EXECUTE [bca].[run_multiyear_processor] @analysis_id;

GO




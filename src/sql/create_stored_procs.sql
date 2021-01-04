CREATE PROCEDURE [bca].[sp_run_comparison_year] @analysis_id integer, @scenario_year smallint
	WITH EXECUTE AS CALLER
AS
-- ===========================================================================
-- Author:		RSG and Daniel Flyte
-- Create date: 8/13/2018
-- Description:	Translation of RSG stored procedure to run single-year base
--              vs. build comparisons. Runs comparison for each BCA component
--              and store results to scenario_comparison table in preparation
--              of subsequent multi-year analysis.
-- ===========================================================================

DECLARE @overall_start_date DATETIME = GETDATE();

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
DECLARE @vot_commute       float;
DECLARE @vot_noncommute    float;

-- physical activity variables
DECLARE @bike_vot_recreational FLOAT;
DECLARE @bike_vot_non_recreational FLOAT;
DECLARE @walk_vot_recreational FLOAT;
DECLARE @walk_vot_non_recreational FLOAT;

-- aggretate trips (CTM, truck) variables
DECLARE @vot_ctm   float;
DECLARE @vot_truck float;

DECLARE @ovt_weight float;

-- Look up base and build scenario IDs
SELECT
    @base_scenario_id       = [scenario_id_base]
	,@build_scenario_id     = [scenario_id_build]
    ,@cost_winter_co        = [co2_value]
    ,@cost_annual_PM2_5     = [pm2_5_value]
    ,@cost_summer_NOx       = [nox_value]
    ,@cost_summer_ROG       = [rog_value]
    ,@cost_annual_SOx       = [so2_value]
    ,@cost_annual_PM10      = [pm_10_value]
    ,@cost_annual_CO2       = [co2_value]
    ,@auto_operating_cost   = [aoc_auto]
    ,@coc_age_threshold     = [coc_age_thresh]
    ,@coc_race_threshold    = [coc_race_thresh]
    ,@coc_poverty_threshold = [coc_poverty_thresh]
    ,@reliability_ratio     = [rel_ratio]
    ,@crash_cost_pdo        = [crash_pdo_cost]
    ,@crash_cost_injury     = [crash_injury_cost]
    ,@crash_cost_fatal      = [crash_fatal_cost]
    ,@crash_rate_pdo        = [crash_rate_pdo]
    ,@crash_rate_injury     = [crash_rate_injury]
    ,@crash_rate_fatal      = [crash_rate_fatal]
    ,@voc_auto              = [voc_auto]
    ,@voc_lhdt              = [voc_truck_light]
    ,@voc_mhdt              = [voc_truck_medium]
    ,@voc_hhdt              = [voc_truck_heavy]
    ,@vor_auto              = [vor_auto]
    ,@vor_lhdt              = [vor_truck_light]
    ,@vor_mhdt              = [vor_truck_medium]
    ,@vor_hhdt              = [vor_truck_heavy]
    ,@vot_commute           = [vot_commute]
    ,@vot_noncommute        = [vot_noncommute]
    ,@vot_ctm               = [vot_truck_light]
	,@vot_truck             = [vot_truck_heavy]
	,@ovt_weight            = [ovt_weight]
	,@bike_vot_recreational     = [bike_vot_recreational]
	,@bike_vot_non_recreational = [bike_vot_non_recreational]
	,@walk_vot_recreational     = [walk_vot_recreational]
	,@walk_vot_non_recreational = [walk_vot_non_recreational]
FROM [bca].[analysis_parameters]
WHERE [analysis_id] = @analysis_id
AND [comparison_year] = @scenario_year;

-- Look up reference year
SELECT
    @ref_year = [year_reference]
FROM [bca].[analysis]
WHERE [analysis_id] = @analysis_id;

    DELETE
    FROM
        [bca].[scenario_comparison]
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] = @scenario_year;

    -- Insert record for analysis_id and scenario year.
    INSERT INTO
        [bca].[scenario_comparison] (
            analysis_id
            ,scenario_year
            ,scenario_id_base
            ,scenario_id_build
            ,last_update_date
            ,diff_co2
            ,diff_pm25
            ,diff_nox
            ,diff_rogs
            ,diff_so2
            ,[diff_co]
            ,[diff_pm10]
            ,[ben_co2]
            ,[ben_pm25]
            ,[ben_nox]
            ,[ben_rogs]
            ,[ben_so2]
            ,[ben_co]
            ,[ben_pm10]
            ,[ben_autos_owned]
            ,[ben_autos_owned_coc]
            ,[ben_autos_owned_coc_age]
            ,[ben_autos_owned_coc_race]
            ,[ben_autos_owned_coc_poverty]
            ,[ben_voc_auto]
            ,[ben_voc_truck_lht]
            ,[ben_voc_truck_med]
            ,[ben_voc_truck_hvy]
            ,[ben_relcost_auto]
            ,[ben_relcost_truck_lht]
            ,[ben_relcost_truck_med]
            ,[ben_relcost_truck_hvy]
            ,[ben_crashcost_pdo]
            ,[ben_crashcost_inj]
            ,[ben_crashcost_fat]
            ,[benefit_bike]
            ,[benefit_bike_coc]
            ,[benefit_bike_senior]
            ,[benefit_bike_minority]
            ,[benefit_bike_low_income]
            ,[benefit_walk]
            ,[benefit_walk_coc]
            ,[benefit_walk_senior]
            ,[benefit_walk_minority]
            ,[benefit_walk_low_income]
            ,[ben_tt_comm]
            ,[ben_tt_truck]
            ,[ben_tt_at_commute]
            ,[ben_tt_at_commute_coc]      
		    ,[ben_tt_at_noncommute]       
		    ,[ben_tt_at_noncommute_coc]
		    ,[ben_tt_at_commute_coc_race]
		    ,[ben_tt_at_noncommute_coc_race]
		    ,[ben_tt_at_commute_coc_age]
		    ,[ben_tt_at_noncommute_coc_age]
		    ,[ben_tt_at_commute_coc_poverty]
		    ,[ben_tt_at_noncommute_coc_poverty]
            ,[ben_tt_auto_commute]                 
            ,[ben_tt_auto_commute_coc]            
            ,[ben_tt_auto_commute_coc_age]        
            ,[ben_tt_auto_commute_coc_poverty]    
            ,[ben_tt_auto_commute_coc_race]
            ,[ben_tt_auto_noncommute]
            ,[ben_tt_auto_noncommute_coc]
            ,[ben_tt_auto_noncommute_coc_age]
            ,[ben_tt_auto_noncommute_coc_poverty]
            ,[ben_tt_auto_noncommute_coc_race]
            ,[ben_tt_transit_commute]
            ,[ben_tt_transit_commute_coc]
            ,[ben_tt_transit_commute_coc_age]
            ,[ben_tt_transit_commute_coc_poverty]
            ,[ben_tt_transit_commute_coc_race]   
            ,[ben_tt_transit_noncommute]         
            ,[ben_tt_transit_noncommute_coc]     
            ,[ben_tt_transit_noncommute_coc_age] 
            ,[ben_tt_transit_noncommute_coc_poverty] 
            ,[ben_tt_transit_noncommute_coc_race]            
            )
    VALUES (@analysis_id, @scenario_year, @base_scenario_id, @build_scenario_id, CAST(GETDATE() as date)
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0
        ,0,0);

-- Execute stored procedures; some stored procedures aren't called in the reference year

-- Emissions cost calculator
 IF @scenario_year <> @ref_year
    BEGIN
        PRINT '     Starting [fn_emissions] calculator for year ' + CAST(@scenario_year AS VARCHAR);
        RAISERROR ('', 10, 1)
        WITH NOWAIT;

        UPDATE
            [bca].[scenario_comparison]
        SET
            [diff_co2]   = [difference_Annual_CO2_TOTEX]
            ,[diff_pm25] = [difference_Annual_PM2_5_TOTAL]
            ,[diff_nox]  = [difference_Summer_NOx_TOTEX]
            ,[diff_rogs] = [difference_Summer_ROG_TOTAL]
            ,[diff_so2]  = [difference_Annual_SOx_TOTEX]
            ,[diff_co]   = [difference_Winter_CO_TOTEX]
            ,[diff_pm10] = [difference_Annual_PM10_TOTAL]
            ,[ben_co2]   = [benefit_Annual_CO2_TOTEX]
            ,[ben_pm25]  = [benefit_Annual_PM2_5_TOTAL]
            ,[ben_nox]   = [benefit_Summer_NOx_TOTEX]
            ,[ben_rogs]  = [benefit_Summer_ROG_TOTAL]
            ,[ben_so2]   = [benefit_Annual_SOx_TOTEX]
            ,[ben_co]    = [benefit_Winter_CO_TOTEX]
            ,[ben_pm10]  = [benefit_Annual_PM10_TOTAL]
        FROM
            [bca].[scenario_comparison]
            CROSS JOIN
                [bca].[fn_emissions](
                    @base_scenario_id
                    ,@build_scenario_id
                    ,@cost_winter_co
                    ,@cost_annual_PM2_5
                    ,@cost_summer_NOx
                    ,@cost_summer_ROG
                    ,@cost_annual_SOx
                    ,@cost_annual_PM10
                    ,@cost_annual_CO2)
        WHERE
            [scenario_comparison].[analysis_id] = @analysis_id
            AND [scenario_comparison].[scenario_year] = @scenario_year;
    END


-- Auto ownership benefit calculator
IF @scenario_year <> @ref_year
    BEGIN
        PRINT '     Starting [fn_auto_ownership] calculator for year ' + CAST(@scenario_year AS VARCHAR);
        RAISERROR ('', 10, 1)
        WITH NOWAIT;

        UPDATE
            [bca].[scenario_comparison]
        SET
            [base_cost_autos_owned]        = [base_cost_auto_ownership]
            ,[build_cost_autos_owned]      = [build_cost_auto_ownership]
            ,[diff_autos_owned]            = [difference_auto_ownership]
            ,[diff_autos_owned_coc]        = [difference_auto_ownership_coc]
            ,[ben_autos_owned]             = [benefits_auto_ownership]
            ,[ben_autos_owned_coc]         = [benefits_auto_ownership_coc]
            ,[ben_autos_owned_coc_age]     = [benefits_auto_ownership_senior]
            ,[ben_autos_owned_coc_race]    = [benefits_auto_ownership_minority]
            ,[ben_autos_owned_coc_poverty] = [benefits_auto_ownership_low_income]    
        FROM
            [bca].[scenario_comparison]
            CROSS JOIN
                [bca].[fn_auto_ownership](
                    @base_scenario_id
                    ,@build_scenario_id
                    ,@auto_operating_cost)
        WHERE
            [analysis_id] = @analysis_id
        AND
            [scenario_year] = @scenario_year;
    END


    PRINT '     Starting [fn_demographics] for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Demographics
    UPDATE
        [bca].[scenario_comparison]
    SET
        [persons]              = [base_persons]
        ,[persons_coc]         = [base_persons_coc]
        ,[persons_coc_race]    = [base_persons_minority]
        ,[persons_coc_age]     = [base_persons_senior]
	    ,[persons_coc_poverty] = [base_persons_low_income]
	    ,[coc_age_thresh]      = @coc_age_threshold
	    ,[coc_race_thresh]     = @coc_race_threshold
	    ,[coc_poverty_thresh]  = @coc_poverty_threshold
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN
            [bca].[fn_demographics](@base_scenario_id, @build_scenario_id)
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] = @scenario_year;

    
    PRINT '     Starting [fn_highway_link] calculator for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Highway link analysis for personal and commercial vehicle trips, safety
    UPDATE
        [bca].[scenario_comparison]
    SET 
        [ben_voc_auto]           = [cost_change_op_auto]
        ,[ben_voc_truck_lht]     = [cost_change_op_lhdt]
        ,[ben_voc_truck_med]     = [cost_change_op_mhdt]
        ,[ben_voc_truck_hvy]     = [cost_change_op_hhdt]
        ,[ben_relcost_auto]      = [cost_change_rel_auto]
        ,[ben_relcost_truck_lht] = [cost_change_rel_lhdt]
        ,[ben_relcost_truck_med] = [cost_change_rel_mhdt]
        ,[ben_relcost_truck_hvy] = [cost_change_rel_hhdt]
        ,[ben_crashcost_pdo]     = [cost_change_crashes_pdo]
        ,[ben_crashcost_inj]     = [cost_change_crashes_injury]
        ,[ben_crashcost_fat]     = [cost_change_crashes_fatal]
        ,[base_rel_cost]         = [base_cost_rel]
        ,[build_rel_cost]        = [build_cost_rel]
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN
            [bca].[fn_highway_link](
                @base_scenario_id
                ,@build_scenario_id
                ,@reliability_ratio
                ,@crash_cost_pdo
                ,@crash_cost_injury
                ,@crash_cost_fatal
                ,@crash_rate_pdo
                ,@crash_rate_injury
                ,@crash_rate_fatal
                ,@voc_auto
                ,@voc_lhdt
                ,@voc_mhdt
                ,@voc_hhdt
                ,@vor_auto
                ,@vor_lhdt
                ,@vor_mhdt
                ,@vor_hhdt)
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] = @scenario_year;
    

    -- Physical activity benefit calculator
    IF @scenario_year <> @ref_year
    BEGIN
        PRINT '     Starting [fn_physical_activity] calculator for year ' + CAST(@scenario_year AS VARCHAR);
        RAISERROR ('', 10, 1)
        WITH NOWAIT;

        UPDATE
            [bca].[scenario_comparison]
        SET
            [base_vot_bike]            = [fn_physical_activity].[base_vot_bike]
            ,[build_vot_bike]          = [fn_physical_activity].[build_vot_bike]
            ,[benefit_bike]            = [fn_physical_activity].[benefit_bike]
            ,[benefit_bike_coc]        = [fn_physical_activity].[benefit_bike_coc]
            ,[benefit_bike_senior]     = [fn_physical_activity].[benefit_bike_senior]
            ,[benefit_bike_minority]   = [fn_physical_activity].[benefit_bike_minority]
            ,[benefit_bike_low_income] = [fn_physical_activity].[benefit_bike_low_income]
            ,[base_vot_walk]           = [fn_physical_activity].[base_vot_walk]
            ,[build_vot_walk]          = [fn_physical_activity].[build_vot_walk]
            ,[benefit_walk]            = [fn_physical_activity].[benefit_walk]
            ,[benefit_walk_coc]        = [fn_physical_activity].[benefit_walk_coc]
            ,[benefit_walk_senior]     = [fn_physical_activity].[benefit_walk_senior]
            ,[benefit_walk_minority]   = [fn_physical_activity].[benefit_walk_minority]
            ,[benefit_walk_low_income] = [fn_physical_activity].[benefit_walk_low_income]
		FROM
            [bca].[scenario_comparison]
			CROSS JOIN
                [bca].[fn_physical_activity](
                    @base_scenario_id
                    ,@build_scenario_id
                    ,@bike_vot_recreational
                    ,@bike_vot_non_recreational
                    ,@walk_vot_recreational
                    ,@walk_vot_non_recreational)
        WHERE
            [analysis_id] = @analysis_id
		    AND [scenario_year] = @scenario_year;
    END

    -- Aggregate toll calculator
    IF @scenario_year <> @ref_year
    BEGIN
        PRINT '     Starting [fn_aggregate_toll] calculator for year ' + CAST(@scenario_year AS VARCHAR);
        RAISERROR ('', 10, 1)
        WITH NOWAIT;

         UPDATE
            [bca].[scenario_comparison]
         SET
            [toll_comm_base]    = [base_toll_ctm]
            ,[toll_truck_base]  = [base_toll_truck]
            ,[toll_comm_build]  = [build_toll_ctm]
            ,[toll_truck_build] = [build_toll_truck]
        FROM
            [bca].[scenario_comparison]
            CROSS JOIN [bca].[fn_aggregate_toll](@base_scenario_id, @build_scenario_id)
        WHERE
            [analysis_id] = @analysis_id
            AND [scenario_year] = @scenario_year;
    END

    PRINT '     Starting [fn_aggregate_trips] calculator for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Aggregate trips calculator
    UPDATE
        [bca].[scenario_comparison]
    SET
        [ben_tt_comm]     = [all_trips_benefit_vot_ctm]
        ,[ben_tt_truck]   = [all_trips_benefit_vot_truck]
        ,[base_tt_comm]   = [build_trips_base_vot_ctm]
        ,[build_tt_comm]  = [build_trips_build_vot_ctm]
        ,[base_tt_truck]  = [build_trips_build_vot_truck]
        ,[build_tt_truck] = [build_trips_build_vot_truck]
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN [bca].[fn_aggregate_trips](@base_scenario_id, @build_scenario_id, @vot_ctm, @vot_truck)
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] =  @scenario_year;

    PRINT '     Starting [fn_resident_trips_at] calculator for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Resident person trips AT
    UPDATE
        [bca].[scenario_comparison]
    SET
        [ben_tt_at_commute]                 = [work_benefit_vot]
        ,[ben_tt_at_commute_coc]            = [work_coc_benefit_vot]
		,[ben_tt_at_noncommute]             = [non_work_benefit_vot]
		,[ben_tt_at_noncommute_coc]         = [non_work_coc_benefit_vot]
		,[base_tt_person]                   = [base_vot]
		,[build_tt_person]                  = [build_vot]
		,[ben_tt_at_commute_coc_race]       = [work_minority_benefit_vot]
		,[ben_tt_at_noncommute_coc_race]    = [non_work_minority_benefit_vot]
		,[ben_tt_at_commute_coc_age]        = [work_senior_benefit_vot]
		,[ben_tt_at_noncommute_coc_age]     = [non_work_senior_benefit_vot]
		,[ben_tt_at_commute_coc_poverty]    = [work_low_income_benefit_vot]
		,[ben_tt_at_noncommute_coc_poverty] = [non_work_low_income_benefit_vot]
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN [bca].[fn_resident_trips_at](@base_scenario_id, @build_scenario_id, @vot_commute, @vot_noncommute)
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] =  @scenario_year;

    PRINT '     Starting [fn_resident_trips_auto] calculator for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Resident person trips auto
    UPDATE [bca].[scenario_comparison]
    SET
        [ben_tt_auto_commute]                 = work_benefit_vot
        ,[ben_tt_auto_commute_coc]            = work_coc_benefit_vot
        ,[ben_tt_auto_commute_coc_age]        = work_senior_benefit_vot
        ,[ben_tt_auto_commute_coc_poverty]    = work_low_income_benefit_vot
        ,[ben_tt_auto_commute_coc_race]       = work_minority_benefit_vot
        ,[ben_tt_auto_noncommute]             = non_work_benefit_vot
        ,[ben_tt_auto_noncommute_coc]         = non_work_coc_benefit_vot
        ,[ben_tt_auto_noncommute_coc_age]     = non_work_senior_benefit_vot
        ,[ben_tt_auto_noncommute_coc_poverty] = non_work_low_income_benefit_vot
        ,[ben_tt_auto_noncommute_coc_race]    = non_work_minority_benefit_vot
        ,[toll_auto_commute_base]             = [work_base_cost_toll]
		,[toll_auto_commute_build]            = [work_build_cost_toll]
		,[toll_auto_commute_base_coc]         = work_coc_base_cost_toll
		,[toll_auto_commute_build_coc]        = work_coc_build_cost_toll
		,[toll_auto_noncommute_base]          = non_work_base_cost_toll
		,[toll_auto_noncommute_build]         = non_work_build_cost_toll
		,[toll_auto_noncommute_base_coc]      = non_work_coc_base_cost_toll
		,[toll_auto_noncommute_build_coc]     = non_work_coc_build_cost_toll
        ,[base_tt_person]                     = base_tt_person
		,[build_tt_person]                    = build_tt_person
    FROM
        [bca].[scenario_comparison]
        CROSS JOIN [bca].[fn_resident_trips_auto](@base_scenario_id, @build_scenario_id, @vot_commute, @vot_noncommute)
    WHERE
        [analysis_id] = @analysis_id
        AND [scenario_year] =  @scenario_year;


    PRINT '     Starting [fn_resident_trips_transit] calculator for year ' + CAST(@scenario_year AS VARCHAR);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;

    -- Person trips (transit mode) benefit calculation
	UPDATE
        [bca].[scenario_comparison]
	SET 
        [ben_tt_transit_commute]                 = work_benefit_vot
        ,[ben_tt_transit_commute_coc]            = work_coc_benefit_vot
        ,[ben_tt_transit_commute_coc_age]        = work_senior_benefit_vot
        ,[ben_tt_transit_commute_coc_poverty]    = work_low_income_benefit_vot
        ,[ben_tt_transit_commute_coc_race]       = work_minority_benefit_vot
        ,[ben_tt_transit_noncommute]             = non_work_benefit_vot
        ,[ben_tt_transit_noncommute_coc]         = non_work_coc_benefit_vot
        ,[ben_tt_transit_noncommute_coc_age]     = non_work_senior_benefit_vot
        ,[ben_tt_transit_noncommute_coc_poverty] = non_work_low_income_benefit_vot
        ,[ben_tt_transit_noncommute_coc_race]    = non_work_minority_benefit_vot
        ,[fare_transit_commute_base]             = work_base_cost_transit
		,[fare_transit_commute_build]            = work_build_cost_transit
		,[fare_transit_commute_base_coc]         = work_coc_base_cost_transit
		,[fare_transit_commute_build_coc]        = work_coc_build_cost_transit
		,[fare_transit_noncommute_base]          = non_work_base_cost_transit
		,[fare_transit_noncommute_build]         = non_work_build_cost_transit
		,[fare_transit_noncommute_base_coc]      = non_work_coc_base_cost_transit
		,[fare_transit_noncommute_build_coc]     = non_work_coc_build_cost_transit
		,[base_tt_person]                        = base_tt_person
		,[build_tt_person]                       = build_tt_person

	FROM
        [bca].[scenario_comparison]
		CROSS JOIN
            [bca].[fn_resident_trips_transit](
			    @base_scenario_id
			    ,@build_scenario_id
			    ,@vot_commute
			    ,@vot_noncommute
			    ,@ovt_weight)
	WHERE
        [analysis_id] = @analysis_id
	    AND [scenario_year] = @scenario_year;


    PRINT 'Finished run_comparison_year(@analysis_id: ' + CAST(@analysis_id AS VARCHAR) + ', @scenario_year: ' + CAST(@scenario_year AS VARCHAR) + ') at ' + + 
	    CONVERT(VARCHAR, GETDATE(), 114) + '. Elapsed time: ' + CONVERT(VARCHAR, (GETDATE() - @overall_start_date), 114);
    RAISERROR ('', 10, 1)
    WITH NOWAIT;
GO


CREATE PROCEDURE [bca].[sp_run_analysis_full]  @analysis_id INT, @scenario_year_forced INT = NULL
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
	EXECUTE [bca].[sp_run_comparison_year] @analysis_id, @scenario_year;

	FETCH NEXT
	FROM cur
	INTO @scenario_year;
END

CLOSE cur;

DEALLOCATE cur;

--Compile multiyear results
EXECUTE [bca].[sp_run_multiyear_processor] @analysis_id;

GO




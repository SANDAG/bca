
CREATE PROCEDURE [bca].[sp_run_multiyear_processor] @analysis_id INT
	WITH EXECUTE AS CALLER
AS
SET NOCOUNT ON;

DECLARE 
	@year_start						integer
	,@year_present					integer
	,@year_intermediate_1			integer
	,@year_intermediate_2			float
	,@year_intermediate_3			float
	,@year_intermediate_4			float
	,@year_intermediate_5			integer
	,@year_end						integer
	,@year_reference				integer
	,@rate_discount					float
	,@annualization_factor			float
	,@current_year					integer
	,@current_iteration				integer		= 0
	,@count_scenario_comparison		integer		= 0
	,@max_scenario_comparison_year	integer		= 0;

--read analysis fields
SELECT 
	@year_start				= year_start
	,@year_present			= year_present
	,@year_intermediate_1	= year_intermediate_1
	,@year_intermediate_2	= year_intermediate_2
	,@year_intermediate_3	= year_intermediate_3
	,@year_intermediate_4	= year_intermediate_4
	,@year_intermediate_5	= year_intermediate_5
	,@year_end				= year_end
	,@year_reference		= year_reference
	,@rate_discount			= discount_rate
	,@annualization_factor	= annualization_factor
FROM analysis
WHERE analysis_id	= @analysis_id;

SET @current_year	= @year_start;

SELECT *
INTO #scenario_comparison
FROM scenario_comparison
WHERE analysis_id	= @analysis_id
	AND scenario_year IN (
		@year_start
		,@year_intermediate_1
		,@year_intermediate_2
		,@year_intermediate_3
		,@year_intermediate_4
		,@year_intermediate_5
	)
ORDER BY scenario_year

SELECT 
	@count_scenario_comparison		= count(*)
	,@max_scenario_comparison_year	= max(scenario_year)
FROM #scenario_comparison

--Create blank records in multiyear_results for all years from year_start to year_end
BEGIN TRANSACTION

DELETE multiyear_results
WHERE analysis_id	= @analysis_id;

WHILE @current_year <= @year_end
BEGIN
	INSERT INTO multiyear_results (
		scenario_id_base
		,scenario_id_build
		,analysis_id
		,comparison_year
		,period
		)
	VALUES (
		CASE 
			WHEN @current_year <= @year_present
				THEN 0
			WHEN @current_year <= @max_scenario_comparison_year
				THEN - 1
			ELSE - 2
			END,
		CASE 
			WHEN @current_year <= @year_present
				THEN 0
			WHEN @current_year <= @max_scenario_comparison_year
				THEN - 1
			ELSE - 2
			END,
		@analysis_id,
		@current_year,
		@current_iteration
		);

	SET @current_iteration = @current_iteration + 1;
	SET @current_year = @current_year + 1;
END

COMMIT TRANSACTION

-- populate individual benefit columns with annualized results for start_year
-- don't need this for most fields whose values are determined by fact that base and build are the same (so dif is 0 and ratio is 1)
BEGIN TRANSACTION

UPDATE multiyear_results
SET scenario_id_base		= sc.scenario_id_base
	,scenario_id_build		= sc.scenario_id_build
	
	-- these will end up NULL if no start_year run of person_trip_processor (which is their default value so no harm done)
	,base_tt_person			= @annualization_factor * sc.base_tt_person
	,build_tt_person		= @annualization_factor * sc.build_tt_person
	
	-- these will end up NULL if no start_year run of aggregate_trips_processor (which is their default value so no harm done)
	,base_tt_truck_comm		= @annualization_factor * (sc.base_tt_comm + sc.base_tt_truck)
	,build_tt_truck_comm	= @annualization_factor * (sc.build_tt_comm + sc.build_tt_truck)
	
	-- these will end up NULL if no start_year run of link_processor
	,base_rel_cost			= @annualization_factor * sc.base_rel_cost
	,build_rel_cost			= @annualization_factor * sc.build_rel_cost
	
	-- these will end up NULL if no start_year run of demographics_processor
	,persons				= sc.persons
	,persons_coc			= sc.persons_coc
	,persons_coc_race		= sc.persons_coc_race
	,persons_coc_age		= sc.persons_coc_age
	,persons_coc_poverty	= sc.persons_coc_poverty

FROM multiyear_results mr
INNER JOIN #scenario_comparison sc
	ON mr.analysis_id = sc.analysis_id
		AND mr.comparison_year = sc.scenario_year
WHERE mr.analysis_id = @analysis_id
	AND mr.comparison_year = @year_start

COMMIT TRANSACTION

--populate individual benefit columns with annualized results for intermediate years
BEGIN TRANSACTION

UPDATE multiyear_results
SET scenario_id_base = sc.scenario_id_base
	,scenario_id_build = sc.scenario_id_build
	
	,ben_co									= @annualization_factor * sc.ben_co
	,ben_co2								= @annualization_factor * sc.ben_co2
	,ben_nox								= @annualization_factor * sc.ben_nox
	,ben_pm10								= @annualization_factor * sc.ben_pm10
	,ben_pm25								= @annualization_factor * sc.ben_pm25
	,ben_rogs								= @annualization_factor * sc.ben_rogs
	,ben_so2								= @annualization_factor * sc.ben_so2
	
	,ben_autos_owned_coc					= sc.ben_autos_owned_coc
	,ben_autos_owned						= sc.ben_autos_owned
	,ben_crashcost_fat						= sc.ben_crashcost_fat
	,ben_crashcost_inj						= sc.ben_crashcost_inj
	,ben_crashcost_pdo						= sc.ben_crashcost_pdo
	--,ben_persons_phys_active_coc			= sc.ben_persons_phys_active_coc
	--,ben_persons_phys_active				= sc.ben_persons_phys_active
	
    ,ben_bike								= sc.benefit_bike
    ,ben_bike_coc							= sc.benefit_bike_coc
    ,ben_bike_coc_age						= sc.benefit_bike_senior
    ,ben_bike_coc_poverty					= sc.benefit_bike_low_income
    ,ben_bike_coc_race						= sc.benefit_bike_minority
	
    ,ben_walk								= sc.benefit_walk
    ,ben_walk_coc							= sc.benefit_walk_coc
    ,ben_walk_coc_age						= sc.benefit_walk_senior
    ,ben_walk_coc_poverty					= sc.benefit_walk_low_income
    ,ben_walk_coc_race						= sc.benefit_walk_minority
	
	,ben_relcost_auto						= @annualization_factor * sc.ben_relcost_auto
	,ben_relcost_truck_hvy					= @annualization_factor * sc.ben_relcost_truck_hvy
	,ben_relcost_truck_lht					= @annualization_factor * sc.ben_relcost_truck_lht
	,ben_relcost_truck_med					= @annualization_factor * sc.ben_relcost_truck_med
	,ben_tt_at_commute						= @annualization_factor * sc.ben_tt_at_commute
	,ben_tt_auto_commute					= @annualization_factor * sc.ben_tt_auto_commute
	,ben_tt_transit_commute					= @annualization_factor * sc.ben_tt_transit_commute
	,ben_tt_at_noncommute					= @annualization_factor * sc.ben_tt_at_noncommute
	,ben_tt_auto_noncommute					= @annualization_factor * sc.ben_tt_auto_noncommute
	,ben_tt_transit_noncommute				= @annualization_factor * sc.ben_tt_transit_noncommute
	,ben_tt_at_commute_coc					= @annualization_factor * sc.ben_tt_at_commute_coc
	,ben_tt_auto_commute_coc				= @annualization_factor * sc.ben_tt_auto_commute_coc
	,ben_tt_transit_commute_coc				= @annualization_factor * sc.ben_tt_transit_commute_coc
	,ben_tt_at_noncommute_coc				= @annualization_factor * sc.ben_tt_at_noncommute_coc
	,ben_tt_auto_noncommute_coc				= @annualization_factor * sc.ben_tt_auto_noncommute_coc
	,ben_tt_transit_noncommute_coc			= @annualization_factor * sc.ben_tt_transit_noncommute_coc
	,ben_tt_comm							= @annualization_factor * sc.ben_tt_comm
	,ben_tt_truck							= @annualization_factor * sc.ben_tt_truck
	,ben_voc_auto							= @annualization_factor * sc.ben_voc_auto
	,ben_voc_truck_lht						= @annualization_factor * sc.ben_voc_truck_lht
	,ben_voc_truck_med						= @annualization_factor * sc.ben_voc_truck_med
	,ben_voc_truck_hvy						= @annualization_factor * sc.ben_voc_truck_hvy
	,toll_rev_base							= @annualization_factor * (sc.toll_auto_commute_base + toll_auto_noncommute_base) + toll_comm_base + toll_truck_base
	,toll_rev_build							= @annualization_factor * (sc.toll_auto_commute_build + toll_auto_noncommute_build) + toll_comm_build + toll_truck_build
	,fare_rev_base							= @annualization_factor * (sc.fare_transit_commute_base + fare_transit_noncommute_base)
	,fare_rev_build							= @annualization_factor * (sc.fare_transit_commute_build + fare_transit_noncommute_build)
	
	-- totals_feature
	,base_tt_person							= @annualization_factor * sc.base_tt_person
	,build_tt_person						= @annualization_factor * sc.build_tt_person
	,base_tt_truck_comm						= @annualization_factor * (sc.base_tt_comm + sc.base_tt_truck)
	,build_tt_truck_comm					= @annualization_factor * (sc.build_tt_comm + sc.build_tt_truck)
	,ratio_tt_person						= sc.build_tt_person / sc.base_tt_person
	,ratio_tt_truck_comm					= (sc.build_tt_comm + sc.build_tt_truck) / (sc.base_tt_comm + sc.base_tt_truck)
	,base_rel_cost							= @annualization_factor * sc.base_rel_cost
	,build_rel_cost							= @annualization_factor * sc.build_rel_cost
	
	-- coc_detail
	,ben_autos_owned_coc_race				= sc.ben_autos_owned_coc_race
	--,ben_persons_phys_active_coc_race		= sc.ben_persons_phys_active_coc_race
	,ben_tt_at_commute_coc_race				= @annualization_factor * sc.ben_tt_at_commute_coc_race
	,ben_tt_auto_commute_coc_race			= @annualization_factor * sc.ben_tt_auto_commute_coc_race
	,ben_tt_transit_commute_coc_race		= @annualization_factor * sc.ben_tt_transit_commute_coc_race
	,ben_tt_at_noncommute_coc_race			= @annualization_factor * sc.ben_tt_at_noncommute_coc_race
	,ben_tt_auto_noncommute_coc_race		= @annualization_factor * sc.ben_tt_auto_noncommute_coc_race
	,ben_tt_transit_noncommute_coc_race		= @annualization_factor * sc.ben_tt_transit_noncommute_coc_race

	,ben_autos_owned_coc_age				= sc.ben_autos_owned_coc_age
	--,ben_persons_phys_active_coc_age		= sc.ben_persons_phys_active_coc_age
	,ben_tt_at_commute_coc_age				= @annualization_factor * sc.ben_tt_at_commute_coc_age
	,ben_tt_auto_commute_coc_age			= @annualization_factor * sc.ben_tt_auto_commute_coc_age
	,ben_tt_transit_commute_coc_age			= @annualization_factor * sc.ben_tt_transit_commute_coc_age
	,ben_tt_at_noncommute_coc_age			= @annualization_factor * sc.ben_tt_at_noncommute_coc_age
	,ben_tt_auto_noncommute_coc_age			= @annualization_factor * sc.ben_tt_auto_noncommute_coc_age
	,ben_tt_transit_noncommute_coc_age		= @annualization_factor * sc.ben_tt_transit_noncommute_coc_age

	,ben_autos_owned_coc_poverty			= sc.ben_autos_owned_coc_poverty
	--,ben_persons_phys_active_coc_poverty	= sc.ben_persons_phys_active_coc_poverty
	,ben_tt_at_commute_coc_poverty			= @annualization_factor * sc.ben_tt_at_commute_coc_poverty
	,ben_tt_auto_commute_coc_poverty		= @annualization_factor * sc.ben_tt_auto_commute_coc_poverty
	,ben_tt_transit_commute_coc_poverty		= @annualization_factor * sc.ben_tt_transit_commute_coc_poverty
	,ben_tt_at_noncommute_coc_poverty		= @annualization_factor * sc.ben_tt_at_noncommute_coc_poverty
	,ben_tt_auto_noncommute_coc_poverty		= @annualization_factor * sc.ben_tt_auto_noncommute_coc_poverty
	,ben_tt_transit_noncommute_coc_poverty	= @annualization_factor * sc.ben_tt_transit_noncommute_coc_poverty

	,persons								= sc.persons
	,persons_coc							= sc.persons_coc
	,persons_coc_race						= sc.persons_coc_race
	,persons_coc_age						= sc.persons_coc_age
	,persons_coc_poverty					= sc.persons_coc_poverty

FROM multiyear_results mr
INNER JOIN #scenario_comparison sc
	ON mr.analysis_id = sc.analysis_id
		AND mr.comparison_year = sc.scenario_year
WHERE mr.analysis_id = @analysis_id
	AND mr.comparison_year IN (
		@year_start
		,@year_intermediate_1
		,@year_intermediate_2
		,@year_intermediate_3
		,@year_intermediate_4
		,@year_intermediate_5
	)

-- from cost_inputs update multiyear_results
--UPDATE multiyear_results
--SET cost_capital = capital.cost_value,
--	cost_finance = finance.cost_value,
--	cost_om = om.cost_value
--FROM multiyear_results mr
--INNER JOIN [finance.netcosts] capital
--	ON mr.analysis_id = capital.bca_id
--		AND mr.comparison_year = capital.year
--INNER JOIN [finance.netcosts] finance
--	ON mr.analysis_id = finance.bca_id
--		AND mr.comparison_year = finance.year
--INNER JOIN [finance.netcosts] om
--	ON mr.analysis_id = om.bca_id
--		AND mr.comparison_year = om.year
--WHERE analysis_id = @analysis_id
--	AND capital.cost_id = 1
--	AND finance.cost_id = 2
--	AND om.cost_id = 3;

COMMIT TRANSACTION

-- begin interpolation process
SELECT 
	row_number() OVER (
		ORDER BY comparison_year
		) AS rownum
	,*
INTO #comparison_years
FROM multiyear_results
WHERE analysis_id = @analysis_id
	AND comparison_year IN (
		@year_start
		,@year_present
		,@year_intermediate_1
		,@year_intermediate_2
		,@year_intermediate_3
		,@year_intermediate_4
		,@year_intermediate_5
	);

SELECT *
INTO #comparison_years_start
FROM #comparison_years
WHERE rownum = 1;

SELECT *
INTO #comparison_years_i0
FROM #comparison_years
WHERE rownum = 2;

SELECT *
INTO #comparison_years_i1
FROM #comparison_years
WHERE rownum = 3;

SELECT *
INTO #comparison_years_i2
FROM #comparison_years
WHERE rownum = 4;

SELECT *
INTO #comparison_years_i3
FROM #comparison_years
WHERE rownum = 5;

SELECT *
INTO #comparison_years_i4
FROM #comparison_years
WHERE rownum = 6;

SELECT *
INTO #comparison_years_i5
FROM #comparison_years
WHERE rownum = 7;

BEGIN TRANSACTION

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_start
		) + 1;

WHILE @current_year <= (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i0
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co									= 0,
		ben_co2									= 0,
		ben_nox									= 0,
		ben_pm10								= 0,
		ben_pm25								= 0,
		ben_rogs								= 0,
		ben_so2									= 0,
		ben_autos_owned_coc						= 0,
		ben_autos_owned							= 0,
		ben_crashcost_fat						= 0,
		ben_crashcost_inj						= 0,
		ben_crashcost_pdo						= 0,
		--ben_persons_phys_active_coc				= 0,
		--ben_persons_phys_active					= 0,
        ben_bike								= 0,
        ben_bike_coc							= 0,
        ben_walk								= 0,
        ben_walk_coc							= 0,
		ben_relcost_auto						= 0,
		ben_relcost_truck_hvy					= 0,
		ben_relcost_truck_lht					= 0,
		ben_relcost_truck_med					= 0,
		ben_tt_at_commute						= 0,
		ben_tt_auto_commute						= 0,
		ben_tt_transit_commute					= 0,
		ben_tt_at_noncommute					= 0,
		ben_tt_auto_noncommute					= 0,
		ben_tt_transit_noncommute				= 0,
		ben_tt_at_commute_coc					= 0,
		ben_tt_auto_commute_coc					= 0,
		ben_tt_transit_commute_coc				= 0,
		ben_tt_at_noncommute_coc				= 0,
		ben_tt_auto_noncommute_coc				= 0,
		ben_tt_transit_noncommute_coc			= 0,
		ben_tt_comm								= 0,
		ben_tt_truck							= 0,
		ben_voc_auto							= 0,
		ben_voc_truck_lht						= 0,
		ben_voc_truck_med						= 0,
		ben_voc_truck_hvy						= 0,
		toll_rev_base							= (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		toll_rev_build							= (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		fare_rev_base							= (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		fare_rev_build							= (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		-- totals_feature
		base_tt_person							= (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		build_tt_person							= (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		base_tt_truck_comm						= (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		build_tt_truck_comm						= (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		ratio_tt_person							= (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		ratio_tt_truck_comm						= (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		base_rel_cost							= (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		build_rel_cost							= (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		-- coc detail
		ben_autos_owned_coc_race				= 0,
		--ben_persons_phys_active_coc_race		= 0,
        ben_bike_coc_race						= 0,
        ben_walk_coc_race						= 0,
		ben_tt_at_commute_coc_race				= 0,
		ben_tt_auto_commute_coc_race			= 0,
		ben_tt_transit_commute_coc_race			= 0,
		ben_tt_at_noncommute_coc_race			= 0,
		ben_tt_auto_noncommute_coc_race			= 0,
		ben_tt_transit_noncommute_coc_race		= 0,
		ben_autos_owned_coc_age					= 0,
		--ben_persons_phys_active_coc_age			= 0,
        ben_bike_coc_age						= 0,
        ben_walk_coc_age						= 0,
		ben_tt_at_commute_coc_age				= 0,
		ben_tt_auto_commute_coc_age				= 0,
		ben_tt_transit_commute_coc_age			= 0,
		ben_tt_at_noncommute_coc_age			= 0,
		ben_tt_auto_noncommute_coc_age			= 0,
		ben_tt_transit_noncommute_coc_age		= 0,
		ben_autos_owned_coc_poverty				= 0,
		--ben_persons_phys_active_coc_poverty		= 0,
        ben_bike_coc_poverty					= 0,
        ben_walk_coc_poverty					= 0,
		ben_tt_at_commute_coc_poverty			= 0,
		ben_tt_auto_commute_coc_poverty			= 0,
		ben_tt_transit_commute_coc_poverty		= 0,
		ben_tt_at_noncommute_coc_poverty		= 0,
		ben_tt_auto_noncommute_coc_poverty		= 0,
		ben_tt_transit_noncommute_coc_poverty	= 0,
		persons									= (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		persons_coc								= (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		persons_coc_race						= (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		persons_coc_age							= (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												),
		persons_coc_poverty						= (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
												)

	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i0
		) + 1;

WHILE @current_year < (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i1
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co = (
			(
				SELECT TOP 1 ben_co
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_co
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_co2 = (
			(
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_co2
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_nox = (
			(
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_nox
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_nox
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm10 = (
			(
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm10
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm25 = (
			(
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm25
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_rogs = (
			(
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_rogs
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_so2 = (
			(
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_so2
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_so2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc = (
			(
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned = (
			(
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_fat = (
			(
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_fat
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_inj = (
			(
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_inj
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_pdo = (
			(
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_pdo
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i1
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_start
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
		--ben_persons_phys_active = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i1
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_start
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike = (
			(
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_bike
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        ben_bike_coc = (
			(
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

        ben_walk = (
			(
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_walk
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        ben_walk_coc = (
			(
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),




		ben_relcost_auto = (
			(
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_hvy = (
			(
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_lht = (
			(
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_med = (
			(
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute = (
			(
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute = (
			(
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute = (
			(
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute = (
			(
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute_coc = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_comm = (
			(
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_truck = (
			(
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_truck
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_auto = (
			(
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_lht = (
			(
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_med = (
			(
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_hvy = (
			(
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_base = (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_build = (
			(
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_base = (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_build = (
			(
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- totals_feature
		base_tt_person = (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_person = (
			(
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_tt_truck_comm = (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_truck_comm = (
			(
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_person = (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_truck_comm = (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_rel_cost = (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_rel_cost = (
			(
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 build_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- coc detail
		ben_autos_owned_coc_race = (
			(
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_race = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i1
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_start
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_race
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike_coc_race = (
			(
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

        ben_walk_coc_race = (
			(
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),


		ben_tt_at_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_age = (
			(
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_age = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i1
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_start
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_age
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike_coc_age = (
			(
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        ben_walk_coc_age = (
			(
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_poverty = (
			(
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_poverty = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i1
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_start
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i0
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike_coc_poverty = (
			(
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        ben_walk_coc_poverty = (
			(
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i0
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons = (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc = (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_race = (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_age = (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_poverty = (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i1
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_start
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i1
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_start
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			)
	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i1
		) + 1;

WHILE @current_year < (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i2
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co = (
			(
				SELECT TOP 1 ben_co
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_co
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_co2 = (
			(
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_nox = (
			(
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_nox
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm10 = (
			(
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm10
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm25 = (
			(
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm25
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_rogs = (
			(
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_rogs
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_so2 = (
			(
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_so2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc = (
			(
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned = (
			(
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_fat = (
			(
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_fat
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_inj = (
			(
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_inj
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_pdo = (
			(
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_pdo
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i2
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i1
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
		--ben_persons_phys_active = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i2
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i1
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),


        ben_bike_coc = (
			(
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_bike = (
			(
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        ben_walk_coc = (
			(
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk = (
			(
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),




		ben_relcost_auto = (
			(
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_hvy = (
			(
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_lht = (
			(
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_med = (
			(
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute = (
			(
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute = (
			(
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute = (
			(
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute = (
			(
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute_coc = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_comm = (
			(
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_truck = (
			(
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_truck
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_auto = (
			(
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_lht = (
			(
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_med = (
			(
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_hvy = (
			(
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_base = (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_build = (
			(
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_base = (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_build = (
			(
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- totals_feature
		base_tt_person = (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_person = (
			(
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_tt_truck_comm = (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_truck_comm = (
			(
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_person = (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_truck_comm = (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_rel_cost = (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_rel_cost = (
			(
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 build_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- coc detail
		ben_autos_owned_coc_race = (
			(
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_race = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i2
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i1
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_race
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike_coc_race = (
			(
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

         ben_walk_coc_race = (
			(
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
        
		ben_tt_at_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_age = (
			(
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_age = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i2
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i1
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_age
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
        
         ben_bike_coc_age = (
			(
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

        ben_walk_coc_age = (
			(
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_poverty = (
			(
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_poverty = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i2
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i1
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i1
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

        ben_bike_coc_poverty = (
			(
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

        ben_walk_coc_poverty = (
			(
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons = (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc = (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_race = (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_age = (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_poverty = (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			)
	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i2
		) + 1;

WHILE @current_year < (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i3
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co = (
			(
				SELECT TOP 1 ben_co
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_co
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_co2 = (
			(
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_nox = (
			(
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_nox
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm10 = (
			(
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm10
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm25 = (
			(
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm25
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_rogs = (
			(
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_rogs
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_so2 = (
			(
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_so2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc = (
			(
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned = (
			(
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_fat = (
			(
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_fat
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_inj = (
			(
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_inj
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_pdo = (
			(
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_pdo
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i3
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i2
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
		--ben_persons_phys_active = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i3
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i2
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc = (
			(
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_bike = (
			(
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

			ben_walk_coc = (
			(
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk = (
			(
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),


		ben_relcost_auto = (
			(
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_hvy = (
			(
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_lht = (
			(
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_med = (
			(
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute = (
			(
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute = (
			(
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute = (
			(
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute = (
			(
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute_coc = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_comm = (
			(
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_truck = (
			(
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_truck
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_auto = (
			(
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_lht = (
			(
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_med = (
			(
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_hvy = (
			(
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_base = (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_build = (
			(
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_base = (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_build = (
			(
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- totals_feature
		base_tt_person = (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_person = (
			(
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_tt_truck_comm = (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_truck_comm = (
			(
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_person = (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_truck_comm = (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_rel_cost = (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_rel_cost = (
			(
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i2
				) - (
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i1
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i2
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i1
							)
						)
				END
			) + (
			SELECT TOP 1 build_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- coc detail
		ben_autos_owned_coc_race = (
			(
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_race = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i3
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i2
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_race
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_race = (
			(
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
			ben_walk_coc_race = (
			(
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_age = (
			(
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_age = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i3
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i2
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_age
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_age = (
			(
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_walk_coc_age = (
			(
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),


		ben_tt_at_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_poverty = (
			(
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_poverty = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i3
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i2
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i2
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_poverty = (
			(
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
			
		ben_walk_coc_poverty = (
			(
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),




		ben_tt_at_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons = (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc = (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_race = (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_age = (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_poverty = (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i3
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i2
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i3
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i2
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			)
	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i3
		) + 1;

WHILE @current_year < (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i4
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co = (
			(
				SELECT TOP 1 ben_co
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_co
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_co2 = (
			(
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_nox = (
			(
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_nox
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm10 = (
			(
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm10
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm25 = (
			(
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm25
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_rogs = (
			(
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_rogs
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_so2 = (
			(
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_so2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc = (
			(
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned = (
			(
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_fat = (
			(
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_fat
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_inj = (
			(
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_inj
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_pdo = (
			(
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_pdo
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i4
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i3
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
		--ben_persons_phys_active = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i4
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i3
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc = (
			(
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_bike = (
			(
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

			ben_walk_coc = (
			(
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk = (
			(
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),




		ben_relcost_auto = (
			(
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_hvy = (
			(
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_lht = (
			(
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_med = (
			(
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute = (
			(
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute = (
			(
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute = (
			(
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute = (
			(
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute_coc = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_comm = (
			(
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_truck = (
			(
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_truck
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_auto = (
			(
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_lht = (
			(
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_med = (
			(
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_hvy = (
			(
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_base = (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_build = (
			(
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_base = (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_build = (
			(
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- totals_feature
		base_tt_person = (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_person = (
			(
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_tt_truck_comm = (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_truck_comm = (
			(
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_person = (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_truck_comm = (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_rel_cost = (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_rel_cost = (
			(
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 build_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- coc detail
		ben_autos_owned_coc_race = (
			(
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_race = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i4
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i3
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_race
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_race = (
			(
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_walk_coc_race = (
			(
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_age = (
			(
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_age = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i4
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i3
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_age
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_age = (
			(
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk_coc_age = (
			(
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_poverty = (
			(
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_poverty = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i4
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i3
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i3
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_poverty = (
			(
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk_coc_poverty = (
			(
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons = (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc = (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_race = (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_age = (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_poverty = (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i4
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i3
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i4
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i3
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			)
	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

SET @current_year = (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i4
		) + 1;

WHILE @current_year < (
		SELECT TOP 1 comparison_year
		FROM #comparison_years_i5
		)
BEGIN
	UPDATE multiyear_results
	SET ben_co = (
			(
				SELECT TOP 1 ben_co
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_co
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_co2 = (
			(
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_co2
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_co2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_nox = (
			(
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_nox
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_nox
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm10 = (
			(
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_pm10
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm10
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_pm25 = (
			(
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_pm25
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_pm25
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_rogs = (
			(
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_rogs
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_rogs
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_so2 = (
			(
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_so2
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_so2
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc = (
			(
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_autos_owned_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned = (
			(
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_autos_owned
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_fat = (
			(
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_crashcost_fat
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_fat
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_inj = (
			(
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_crashcost_inj
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_inj
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_crashcost_pdo = (
			(
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_crashcost_pdo
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_crashcost_pdo
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i5
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc
		--		FROM #comparison_years_i4
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),
		--ben_persons_phys_active = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i5
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active
		--		FROM #comparison_years_i4
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc = (
			(
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_bike_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_bike = (
			(
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_bike
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_walk_coc = (
			(
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_walk_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk = (
			(
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_walk
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),


		ben_relcost_auto = (
			(
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_relcost_auto
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_hvy = (
			(
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_relcost_truck_hvy
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_lht = (
			(
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_relcost_truck_lht
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_relcost_truck_med = (
			(
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_relcost_truck_med
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_relcost_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute = (
			(
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_commute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute = (
			(
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_commute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute = (
			(
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_commute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute = (
			(
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_noncommute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_commute_coc = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_comm = (
			(
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_comm
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_truck = (
			(
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_truck
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_truck
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_auto = (
			(
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_voc_auto
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_auto
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_lht = (
			(
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_voc_truck_lht
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_lht
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_med = (
			(
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_voc_truck_med
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_med
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_voc_truck_hvy = (
			(
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_voc_truck_hvy
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_voc_truck_hvy
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_base = (
			(
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 toll_rev_base
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		toll_rev_build = (
			(
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 toll_rev_build
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 toll_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_base = (
			(
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 fare_rev_base
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_base
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		fare_rev_build = (
			(
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 fare_rev_build
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 fare_rev_build
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- totals_feature
		base_tt_person = (
			(
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 base_tt_person
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_person = (
			(
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 build_tt_person
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_tt_truck_comm = (
			(
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 base_tt_truck_comm
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 base_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_tt_truck_comm = (
			(
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 build_tt_truck_comm
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 build_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_person = (
			(
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ratio_tt_person
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_person
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ratio_tt_truck_comm = (
			(
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ratio_tt_truck_comm
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ratio_tt_truck_comm
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		base_rel_cost = (
			(
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 base_rel_cost
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 base_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		build_rel_cost = (
			(
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 build_rel_cost
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 build_rel_cost
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		-- coc detail
		ben_autos_owned_coc_race = (
			(
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_autos_owned_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_race = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i5
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_race
		--		FROM #comparison_years_i4
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_race
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_race = (
			(
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_bike_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk_coc_race = (
			(
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_walk_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_race = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_age = (
			(
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_autos_owned_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_age = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i5
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_age
		--		FROM #comparison_years_i4
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_age
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_age = (
			(
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_bike_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk_coc_age = (
			(
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_walk_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_age = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_autos_owned_coc_poverty = (
			(
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_autos_owned_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_autos_owned_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		--ben_persons_phys_active_coc_poverty = (
		--	(
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i5
		--		) - (
		--		SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--		FROM #comparison_years_i4
		--		)
		--	) / (
		--	CASE 
		--		WHEN (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				) = 0
		--			THEN 1
		--		ELSE (
		--				(
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i5
		--					) - (
		--					SELECT TOP 1 period
		--					FROM #comparison_years_i4
		--					)
		--				)
		--		END
		--	) + (
		--	SELECT TOP 1 ben_persons_phys_active_coc_poverty
		--	FROM multiyear_results
		--	WHERE analysis_id = @analysis_id
		--		AND period = mr.period - 1
		--	),

		ben_bike_coc_poverty = (
			(
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_bike_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_bike_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_walk_coc_poverty = (
			(
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_walk_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_walk_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),

		ben_tt_at_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_commute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_commute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_commute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_commute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_commute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_at_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_auto_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		ben_tt_transit_noncommute_coc_poverty = (
			(
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons = (
			(
				SELECT TOP 1 persons
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 persons
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 persons
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc = (
			(
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 persons_coc
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_race = (
			(
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 persons_coc_race
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_race
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_age = (
			(
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 persons_coc_age
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_age
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			),
		persons_coc_poverty = (
			(
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i5
				) - (
				SELECT TOP 1 persons_coc_poverty
				FROM #comparison_years_i4
				)
			) / (
			CASE 
				WHEN (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						) = 0
					THEN 1
				ELSE (
						(
							SELECT TOP 1 period
							FROM #comparison_years_i5
							) - (
							SELECT TOP 1 period
							FROM #comparison_years_i4
							)
						)
				END
			) + (
			SELECT TOP 1 persons_coc_poverty
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
				AND period = mr.period - 1
			)
	FROM multiyear_results mr
	WHERE mr.analysis_id = @analysis_id
		AND mr.comparison_year = @current_year;

	SET @current_year += 1;
END;

COMMIT TRANSACTION

-- begin extrapolation process
SELECT *
INTO #comparison_years_previous_1
FROM multiyear_results
WHERE analysis_id = @analysis_id
	AND comparison_year = @max_scenario_comparison_year;

SELECT *
INTO #comparison_years_previous_2
FROM multiyear_results
WHERE analysis_id = @analysis_id
	AND comparison_year = @max_scenario_comparison_year - 1;

/* To disable extrapolation, comment out from here down to line 18120 */
	BEGIN TRANSACTION

	SET @current_year = @max_scenario_comparison_year + 1;

	WHILE @current_year <= @year_end
	BEGIN
		UPDATE multiyear_results
		SET ben_co = (
				(
					SELECT TOP 1 ben_co
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_co
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_co
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_co2 = (
				(
					SELECT TOP 1 ben_co2
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_co2
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_co2
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_nox = (
				(
					SELECT TOP 1 ben_nox
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_nox
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_nox
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_pm10 = (
				(
					SELECT TOP 1 ben_pm10
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_pm10
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_pm10
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_pm25 = (
				(
					SELECT TOP 1 ben_pm25
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_pm25
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_pm25
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_rogs = (
				(
					SELECT TOP 1 ben_rogs
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_rogs
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_rogs
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_so2 = (
				(
					SELECT TOP 1 ben_so2
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_so2
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_so2
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_autos_owned_coc = (
				(
					SELECT TOP 1 ben_autos_owned_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_autos_owned_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_autos_owned_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_autos_owned = (
				(
					SELECT TOP 1 ben_autos_owned
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_autos_owned
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_autos_owned
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_crashcost_fat = (
				(
					SELECT TOP 1 ben_crashcost_fat
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_crashcost_fat
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_crashcost_fat
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_crashcost_inj = (
				(
					SELECT TOP 1 ben_crashcost_inj
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_crashcost_inj
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_crashcost_inj
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_crashcost_pdo = (
				(
					SELECT TOP 1 ben_crashcost_pdo
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_crashcost_pdo
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_crashcost_pdo
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
	--		ben_persons_phys_active_coc = (
	--			(
	--				SELECT TOP 1 ben_persons_phys_active_coc
	--				FROM #comparison_years_previous_1
	--				) - (
	--				SELECT TOP 1 ben_persons_phys_active_coc
	--				FROM #comparison_years_previous_2
	--				) + (
	--				SELECT TOP 1 ben_persons_phys_active_coc
	--				FROM multiyear_results
	--				WHERE analysis_id = @analysis_id
	--					AND period = mr.period - 1
	--				)
	--			),
	--		ben_persons_phys_active = (
	--			(
	--				SELECT TOP 1 ben_persons_phys_active
	--				FROM #comparison_years_previous_1
	--				) - (
	--				SELECT TOP 1 ben_persons_phys_active
	--				FROM #comparison_years_previous_2
	--				) + (
	--				SELECT TOP 1 ben_persons_phys_active
	--				FROM multiyear_results
	--				WHERE analysis_id = @analysis_id
	--					AND period = mr.period - 1
	--				)
	--			),
			ben_bike_coc = (
				(
					SELECT TOP 1 ben_bike_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_bike_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_bike_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_bike = (
				(
					SELECT TOP 1 ben_bike
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_bike
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_bike
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
		
			ben_walk_coc = (
				(
					SELECT TOP 1 ben_walk_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_walk_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_walk_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_walk = (
				(
					SELECT TOP 1 ben_walk
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_walk
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_walk
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_relcost_auto = (
				(
					SELECT TOP 1 ben_relcost_auto
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_relcost_auto
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_relcost_auto
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_relcost_truck_hvy = (
				(
					SELECT TOP 1 ben_relcost_truck_hvy
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_relcost_truck_hvy
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_relcost_truck_hvy
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_relcost_truck_lht = (
				(
					SELECT TOP 1 ben_relcost_truck_lht
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_relcost_truck_lht
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_relcost_truck_lht
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_relcost_truck_med = (
				(
					SELECT TOP 1 ben_relcost_truck_med
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_relcost_truck_med
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_relcost_truck_med
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_commute = (
				(
					SELECT TOP 1 ben_tt_at_commute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_commute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_commute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_commute = (
				(
					SELECT TOP 1 ben_tt_auto_commute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_commute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_commute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_commute = (
				(
					SELECT TOP 1 ben_tt_transit_commute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_commute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_commute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_noncommute = (
				(
					SELECT TOP 1 ben_tt_at_noncommute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_noncommute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_noncommute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_noncommute = (
				(
					SELECT TOP 1 ben_tt_auto_noncommute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_noncommute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_noncommute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_noncommute = (
				(
					SELECT TOP 1 ben_tt_transit_noncommute
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_noncommute
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_noncommute
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_commute_coc = (
				(
					SELECT TOP 1 ben_tt_at_commute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_commute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_commute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_commute_coc = (
				(
					SELECT TOP 1 ben_tt_auto_commute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_commute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_commute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_commute_coc = (
				(
					SELECT TOP 1 ben_tt_transit_commute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_commute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_commute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_noncommute_coc = (
				(
					SELECT TOP 1 ben_tt_at_noncommute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_noncommute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_noncommute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_noncommute_coc = (
				(
					SELECT TOP 1 ben_tt_auto_noncommute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_noncommute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_noncommute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_noncommute_coc = (
				(
					SELECT TOP 1 ben_tt_transit_noncommute_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_noncommute_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_noncommute_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_comm = (
				(
					SELECT TOP 1 ben_tt_comm
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_comm
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_comm
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_truck = (
				(
					SELECT TOP 1 ben_tt_truck
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_truck
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_truck
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_voc_auto = (
				(
					SELECT TOP 1 ben_voc_auto
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_voc_auto
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_voc_auto
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_voc_truck_lht = (
				(
					SELECT TOP 1 ben_voc_truck_lht
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_voc_truck_lht
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_voc_truck_lht
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_voc_truck_med = (
				(
					SELECT TOP 1 ben_voc_truck_med
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_voc_truck_med
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_voc_truck_med
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_voc_truck_hvy = (
				(
					SELECT TOP 1 ben_voc_truck_hvy
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_voc_truck_hvy
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_voc_truck_hvy
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			toll_rev_base = (
				(
					SELECT TOP 1 toll_rev_base
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 toll_rev_base
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 toll_rev_base
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			toll_rev_build = (
				(
					SELECT TOP 1 toll_rev_build
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 toll_rev_build
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 toll_rev_build
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			fare_rev_base = (
				(
					SELECT TOP 1 fare_rev_base
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 fare_rev_base
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 fare_rev_base
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			fare_rev_build = (
				(
					SELECT TOP 1 fare_rev_build
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 fare_rev_build
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 fare_rev_build
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			-- totals_feature
			base_tt_person = (
				(
					SELECT TOP 1 base_tt_person
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 base_tt_person
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 base_tt_person
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			build_tt_person = (
				(
					SELECT TOP 1 build_tt_person
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 build_tt_person
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 build_tt_person
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			base_tt_truck_comm = (
				(
					SELECT TOP 1 base_tt_truck_comm
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 base_tt_truck_comm
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 base_tt_truck_comm
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			build_tt_truck_comm = (
				(
					SELECT TOP 1 build_tt_truck_comm
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 build_tt_truck_comm
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 build_tt_truck_comm
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ratio_tt_person = (
				(
					SELECT TOP 1 ratio_tt_person
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ratio_tt_person
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ratio_tt_person
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ratio_tt_truck_comm = (
				(
					SELECT TOP 1 ratio_tt_truck_comm
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ratio_tt_truck_comm
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ratio_tt_truck_comm
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			base_rel_cost = (
				(
					SELECT TOP 1 base_rel_cost
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 base_rel_cost
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 base_rel_cost
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			build_rel_cost = (
				(
					SELECT TOP 1 build_rel_cost
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 build_rel_cost
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 build_rel_cost
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			-- coc detail
			ben_autos_owned_coc_race = (
				(
					SELECT TOP 1 ben_autos_owned_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_autos_owned_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_autos_owned_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
	--		ben_persons_phys_active_coc_race = (
	--			(
	--				SELECT TOP 1 ben_persons_phys_active_coc_race
	--				FROM #comparison_years_previous_1
	--				) - (
	--				SELECT TOP 1 ben_persons_phys_active_coc_race
	--				FROM #comparison_years_previous_2
	--				) + (
	--				SELECT TOP 1 ben_persons_phys_active_coc_race
	--				FROM multiyear_results
	--				WHERE analysis_id = @analysis_id
	--					AND period = mr.period - 1
	--				)
	--			),
			ben_bike_coc_race = (
				(
					SELECT TOP 1 ben_bike_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_bike_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_bike_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_walk_coc_race = (
				(
					SELECT TOP 1 ben_walk_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_walk_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_walk_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_commute_coc_race = (
				(
					SELECT TOP 1 ben_tt_at_commute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_commute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_commute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_commute_coc_race = (
				(
					SELECT TOP 1 ben_tt_auto_commute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_commute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_commute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_commute_coc_race = (
				(
					SELECT TOP 1 ben_tt_transit_commute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_commute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_commute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_noncommute_coc_race = (
				(
					SELECT TOP 1 ben_tt_at_noncommute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_noncommute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_noncommute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_noncommute_coc_race = (
				(
					SELECT TOP 1 ben_tt_auto_noncommute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_noncommute_coc_race = (
				(
					SELECT TOP 1 ben_tt_transit_noncommute_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_autos_owned_coc_age = (
				(
					SELECT TOP 1 ben_autos_owned_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_autos_owned_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_autos_owned_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
	--		ben_persons_phys_active_coc_age = (
	--			(
	--				SELECT TOP 1 ben_persons_phys_active_coc_age
	--				FROM #comparison_years_previous_1
	--				) - (
	--				SELECT TOP 1 ben_persons_phys_active_coc_age
	--				FROM #comparison_years_previous_2
	--				) + (
	--				SELECT TOP 1 ben_persons_phys_active_coc_age
	--				FROM multiyear_results
	--				WHERE analysis_id = @analysis_id
	--					AND period = mr.period - 1
	--				)
	--			),
			ben_bike_coc_age = (
				(
					SELECT TOP 1 ben_bike_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_bike_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_bike_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_walk_coc_age = (
				(
					SELECT TOP 1 ben_walk_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_walk_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_walk_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_commute_coc_age = (
				(
					SELECT TOP 1 ben_tt_at_commute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_commute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_commute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_commute_coc_age = (
				(
					SELECT TOP 1 ben_tt_auto_commute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_commute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_commute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_commute_coc_age = (
				(
					SELECT TOP 1 ben_tt_transit_commute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_commute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_commute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_noncommute_coc_age = (
				(
					SELECT TOP 1 ben_tt_at_noncommute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_noncommute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_noncommute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_noncommute_coc_age = (
				(
					SELECT TOP 1 ben_tt_auto_noncommute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_noncommute_coc_age = (
				(
					SELECT TOP 1 ben_tt_transit_noncommute_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_autos_owned_coc_poverty = (
				(
					SELECT TOP 1 ben_autos_owned_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_autos_owned_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_autos_owned_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
	--		ben_persons_phys_active_coc_poverty = (
	--			(
	--				SELECT TOP 1 ben_persons_phys_active_coc_poverty
	--				FROM #comparison_years_previous_1
	--				) - (
	--				SELECT TOP 1 ben_persons_phys_active_coc_poverty
	--				FROM #comparison_years_previous_2
	--				) + (
	--				SELECT TOP 1 ben_persons_phys_active_coc_poverty
	--				FROM multiyear_results
	--				WHERE analysis_id = @analysis_id
	--					AND period = mr.period - 1
	--				)
	--			),
			ben_bike_coc_poverty = (
				(
					SELECT TOP 1 ben_bike_coc_poverty 
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_bike_coc_poverty 
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_bike_coc_poverty 
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_walk_coc_poverty = (
				(
					SELECT TOP 1 ben_walk_coc_poverty 
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_walk_coc_poverty 
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_walk_coc_poverty 
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),

			ben_tt_at_commute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_at_commute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_commute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_commute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_commute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_auto_commute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_commute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_commute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_commute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_transit_commute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_commute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_commute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_at_noncommute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_at_noncommute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_auto_noncommute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_auto_noncommute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			ben_tt_transit_noncommute_coc_poverty = (
				(
					SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 ben_tt_transit_noncommute_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			persons = (
				(
					SELECT TOP 1 persons
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 persons
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 persons
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			persons_coc = (
				(
					SELECT TOP 1 persons_coc
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 persons_coc
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 persons_coc
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			persons_coc_race = (
				(
					SELECT TOP 1 persons_coc_race
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 persons_coc_race
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 persons_coc_race
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			persons_coc_age = (
				(
					SELECT TOP 1 persons_coc_age
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 persons_coc_age
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 persons_coc_age
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				),
			persons_coc_poverty = (
				(
					SELECT TOP 1 persons_coc_poverty
					FROM #comparison_years_previous_1
					) - (
					SELECT TOP 1 persons_coc_poverty
					FROM #comparison_years_previous_2
					) + (
					SELECT TOP 1 persons_coc_poverty
					FROM multiyear_results
					WHERE analysis_id = @analysis_id
						AND period = mr.period - 1
					)
				)
		FROM multiyear_results mr
		WHERE mr.analysis_id = @analysis_id
			AND mr.comparison_year = @current_year;

		SET @current_year += 1;
	END;

	COMMIT TRANSACTION
/* End extrapolation section */

BEGIN TRANSACTION

UPDATE multiyear_results
SET ben_tt_at_commute						= ben_tt_at_commute							* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_commute						= ben_tt_auto_commute						* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_commute					= ben_tt_transit_commute					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_noncommute					= ben_tt_at_noncommute						* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_noncommute					= ben_tt_auto_noncommute					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_noncommute				= ben_tt_transit_noncommute					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_commute_coc					= ben_tt_at_commute_coc						* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_commute_coc					= ben_tt_auto_commute_coc					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_commute_coc				= ben_tt_transit_commute_coc				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_noncommute_coc				= ben_tt_at_noncommute_coc					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_noncommute_coc				= ben_tt_auto_noncommute_coc				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_noncommute_coc			= ben_tt_transit_noncommute_coc				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_comm								= ben_tt_comm								* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_truck							= ben_tt_truck								* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_commute_coc_race				= ben_tt_at_commute_coc_race				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_commute_coc_age				= ben_tt_at_commute_coc_age					* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_commute_coc_poverty			= ben_tt_at_commute_coc_poverty				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_commute_coc_race			= ben_tt_auto_commute_coc_race				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_commute_coc_age				= ben_tt_auto_commute_coc_age				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_commute_coc_poverty			= ben_tt_auto_commute_coc_poverty			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_commute_coc_race			= ben_tt_transit_commute_coc_race			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_commute_coc_age			= ben_tt_transit_commute_coc_age			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_commute_coc_poverty		= ben_tt_transit_commute_coc_poverty		* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_noncommute_coc_race			= ben_tt_at_noncommute_coc_race				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_noncommute_coc_age			= ben_tt_at_noncommute_coc_age				* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_at_noncommute_coc_poverty		= ben_tt_at_noncommute_coc_poverty			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_noncommute_coc_race			= ben_tt_auto_noncommute_coc_race			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_noncommute_coc_age			= ben_tt_auto_noncommute_coc_age			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_auto_noncommute_coc_poverty		= ben_tt_auto_noncommute_coc_poverty		* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_noncommute_coc_race		= ben_tt_transit_noncommute_coc_race		* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_noncommute_coc_age		= ben_tt_transit_noncommute_coc_age			* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start),
	ben_tt_transit_noncommute_coc_poverty	= ben_tt_transit_noncommute_coc_poverty		* power(annual_vot_growth, multiyear_results.comparison_year - analysis.year_start)
FROM multiyear_results
INNER JOIN analysis
	ON multiyear_results.analysis_id = analysis.analysis_id
WHERE multiyear_results.analysis_id = @analysis_id;

COMMIT TRANSACTION

-- compute the subtotal columns
BEGIN TRANSACTION

UPDATE multiyear_results
SET subtotal_ben_emissions					= isnull(ben_co, 0) + isnull(ben_co2, 0) + isnull(ben_nox, 0) + isnull(ben_pm10, 0) + isnull(ben_pm25, 0) + isnull(ben_rogs, 0) + isnull(ben_so2, 0),
	subtotal_ben_safety						= isnull(ben_crashcost_fat, 0) + isnull(ben_crashcost_inj, 0) + isnull(ben_crashcost_pdo, 0),
	subtotal_ben_reliability				= isnull(ben_relcost_auto, 0) + isnull(ben_relcost_truck_hvy, 0) + isnull(ben_relcost_truck_lht, 0) + isnull(ben_relcost_truck_med, 0),
	subtotal_ben_tt_commute					= isnull(ben_tt_at_commute, 0) + isnull(ben_tt_auto_commute, 0) + isnull(ben_tt_transit_commute, 0),
	subtotal_ben_tt_noncommute				= isnull(ben_tt_at_noncommute, 0) + isnull(ben_tt_auto_noncommute, 0) + isnull(ben_tt_transit_noncommute, 0),
	subtotal_ben_tt_commute_coc				= isnull(ben_tt_at_commute_coc, 0) + isnull(ben_tt_auto_commute_coc, 0) + isnull(ben_tt_transit_commute_coc, 0),
	subtotal_ben_tt_noncommute_coc			= isnull(ben_tt_at_noncommute_coc, 0) + isnull(ben_tt_auto_noncommute_coc, 0) + isnull(ben_tt_transit_noncommute_coc, 0),
	subtotal_ben_tt_commute_coc_race		= isnull(ben_tt_at_commute_coc_race, 0) + isnull(ben_tt_auto_commute_coc_race, 0) + isnull(ben_tt_transit_commute_coc_race, 0),
	subtotal_ben_tt_noncommute_coc_race		= isnull(ben_tt_at_noncommute_coc_race, 0) + isnull(ben_tt_auto_noncommute_coc_race, 0) + isnull(ben_tt_transit_noncommute_coc_race, 0),
	subtotal_ben_tt_commute_coc_age			= isnull(ben_tt_at_commute_coc_age, 0) + isnull(ben_tt_auto_commute_coc_age, 0) + isnull(ben_tt_transit_commute_coc_age, 0),
	subtotal_ben_tt_noncommute_coc_age		= isnull(ben_tt_at_noncommute_coc_age, 0) + isnull(ben_tt_auto_noncommute_coc_age, 0) + isnull(ben_tt_transit_noncommute_coc_age, 0),
	subtotal_ben_tt_commute_coc_poverty		= isnull(ben_tt_at_commute_coc_poverty, 0) + isnull(ben_tt_auto_commute_coc_poverty, 0) + isnull(ben_tt_transit_commute_coc_poverty, 0),
	subtotal_ben_tt_noncommute_coc_poverty	= isnull(ben_tt_at_noncommute_coc_poverty, 0) + isnull(ben_tt_auto_noncommute_coc_poverty, 0) + isnull(ben_tt_transit_noncommute_coc_poverty, 0),
	subtotal_ben_freight					= isnull(ben_tt_comm, 0) + isnull(ben_tt_truck, 0),
	subtotal_ben_voc						= isnull(ben_voc_auto, 0) + isnull(ben_voc_truck_lht, 0) + isnull(ben_voc_truck_med, 0) + isnull(ben_voc_truck_hvy, 0)
FROM multiyear_results
WHERE analysis_id = @analysis_id;

-- compute yearly analysis columns
UPDATE multiyear_results
SET benefit_total_undiscounted = isnull(subtotal_ben_emissions, 0) + isnull(ben_autos_owned, 0) + isnull(subtotal_ben_safety, 0) + isnull(
		--ben_persons_phys_active, 0) + isnull(
		ben_bike, 0) + isnull(
		ben_walk, 0) +isnull(subtotal_ben_reliability, 0) + isnull(subtotal_ben_tt_commute, 0) + isnull(subtotal_ben_tt_noncommute, 0) 
	+ isnull(subtotal_ben_freight, 0) + isnull(subtotal_ben_voc, 0),
	--cost_total_undiscounted = isnull(cost_capital, 0) + isnull(cost_om, 0) + isnull(cost_finance, 0),
	toll_rev_base_discounted = isnull(toll_rev_base, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	toll_rev_build_discounted = isnull(toll_rev_build, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	fare_rev_base_discounted = isnull(fare_rev_base, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	fare_rev_build_discounted = isnull(fare_rev_build, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	-- totals_feature
	base_tt_person_discounted = isnull(base_tt_person, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	build_tt_person_discounted = isnull(build_tt_person, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	base_tt_truck_comm_discounted = isnull(base_tt_truck_comm, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	build_tt_truck_comm_discounted = isnull(build_tt_truck_comm, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	base_rel_cost_discounted = isnull(base_rel_cost, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	build_rel_cost_discounted = isnull(build_rel_cost, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		)
FROM multiyear_results
WHERE analysis_id = @analysis_id;

UPDATE multiyear_results
SET benefit_emissions_discounted = isnull(subtotal_ben_emissions, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_autos_owned_discounted = isnull(ben_autos_owned, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_autos_owned_discounted_coc = isnull(ben_autos_owned_coc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_safety_discounted = isnull(subtotal_ben_safety, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	--benefit_phys_active_discounted = isnull(ben_persons_phys_active, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	--benefit_phys_active_discounted_coc = isnull(ben_persons_phys_active_coc, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	benefit_bike_discounted = isnull(ben_bike, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_bike_discounted_coc = isnull(ben_bike_coc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	
	benefit_walk_discounted = isnull(ben_walk, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_walk_discounted_coc = isnull(ben_walk_coc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_reliability_discounted = isnull(subtotal_ben_reliability, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_commute_discounted = isnull(subtotal_ben_tt_commute, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_commute_discounted_coc = isnull(subtotal_ben_tt_commute_coc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_noncommute_discounted = isnull(subtotal_ben_tt_noncommute, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_noncommute_discounted_coc = isnull(subtotal_ben_tt_noncommute_coc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_freight_discounted = isnull(subtotal_ben_freight, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_voc_discounted = isnull(subtotal_ben_voc, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_total_discounted = isnull(benefit_total_undiscounted, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	--cost_capital_discounted = isnull(cost_capital, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	--cost_om_discounted = isnull(cost_om, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	--cost_finance_discounted = isnull(cost_finance, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	--cost_total_discounted = isnull(cost_total_undiscounted, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	net_annual_undiscounted = isnull(benefit_total_undiscounted, 0) /*- isnull(cost_total_undiscounted, 0)*/,
	-- coc detail
	benefit_autos_owned_discounted_coc_race = isnull(ben_autos_owned_coc_race, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	--benefit_phys_active_discounted_coc_race = isnull(ben_persons_phys_active_coc_race, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	benefit_bike_discounted_coc_race = isnull(ben_bike_coc_race, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
		
	benefit_walk_discounted_coc_race = isnull(ben_walk_coc_race, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_commute_discounted_coc_race = isnull(subtotal_ben_tt_commute_coc_race, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_noncommute_discounted_coc_race = isnull(subtotal_ben_tt_noncommute_coc_race, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_autos_owned_discounted_coc_age = isnull(ben_autos_owned_coc_age, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	--benefit_phys_active_discounted_coc_age = isnull(ben_persons_phys_active_coc_age, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	benefit_bike_discounted_coc_age = isnull(ben_bike_coc_age, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_walk_discounted_coc_age = isnull(ben_walk_coc_age, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_commute_discounted_coc_age = isnull(subtotal_ben_tt_commute_coc_age, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_noncommute_discounted_coc_age = isnull(subtotal_ben_tt_noncommute_coc_age, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_autos_owned_discounted_coc_poverty = isnull(ben_autos_owned_coc_poverty, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	--benefit_phys_active_discounted_coc_poverty = isnull(ben_persons_phys_active_coc_poverty, 0) / (
	--	CASE 
	--		WHEN power(1 + @rate_discount, period) = 0
	--			THEN 1
	--		ELSE power(1 + @rate_discount, period)
	--		END
	--	),
	benefit_bike_discounted_coc_poverty = isnull(ben_bike_coc_poverty, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_walk_discounted_coc_poverty = isnull(ben_walk_coc_poverty, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_commute_discounted_coc_poverty = isnull(subtotal_ben_tt_commute_coc_poverty, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		),
	benefit_tt_noncommute_discounted_coc_poverty = isnull(subtotal_ben_tt_noncommute_coc_poverty, 0) / (
		CASE 
			WHEN power(1 + @rate_discount, period) = 0
				THEN 1
			ELSE power(1 + @rate_discount, period)
			END
		)
FROM multiyear_results
WHERE analysis_id = @analysis_id;

UPDATE multiyear_results
SET net_annual_discounted = isnull(benefit_total_discounted, 0) /*- isnull(cost_total_discounted, 0)*/
FROM multiyear_results
WHERE analysis_id = @analysis_id;

COMMIT TRANSACTION

-- calculate irr
DECLARE @fx FLOAT,
	@fx_der FLOAT,
	@irr_current FLOAT,
	@irr_previous FLOAT,
	@irr FLOAT,
	@irr_estimation FLOAT = 0.00001,
	@guess1 FLOAT = 0.01,
	@guess2 FLOAT = 0.02,
	@npv1 FLOAT,
	@npv2 FLOAT,
	@iteration INT = 0;

--start guessing
SET @npv1 = (
		SELECT sum(net_annual_undiscounted * power(1 + @guess1, - 1 * period))
		FROM multiyear_results
		WHERE analysis_id = @analysis_id
		)
SET @npv2 = (
		SELECT sum(net_annual_undiscounted * power(1 + @guess2, - 1 * period))
		FROM multiyear_results
		WHERE analysis_id = @analysis_id
		)

WHILE (
		(
			@npv1 > 0
			AND @npv2 > 0
			)
		OR (
			@npv1 < 0
			AND @npv2 < 0
			)
		)
	AND @guess1 <= 1
BEGIN
	SET @guess1 += 0.01;
	SET @guess2 += 0.01;
	SET @npv1 = (
			SELECT sum(net_annual_undiscounted * power(1 + @guess1, - 1 * period))
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
			)
	SET @npv2 = (
			SELECT sum(net_annual_undiscounted * power(1 + @guess2, - 1 * period))
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
			)
END

--start the newton formula
IF @guess1 < 1.0 --if the guess was not even valid don't process
BEGIN
	SET @irr_previous = @guess1;
	SET @fx = (
			SELECT sum(net_annual_undiscounted * power(1 + @irr_previous, - 1 * period))
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
			)
	SET @fx_der = (
			SELECT sum((net_annual_undiscounted * (- 1 * period)) * (power(1 + @irr_previous, (- 1 * period) - 1)
						))
			FROM multiyear_results
			WHERE analysis_id = @analysis_id
			)
	SET @irr_current = @irr_previous - CASE 
			WHEN (@fx / @fx_der) = 0
				THEN 0.00001
			ELSE (@fx / @fx_der)
			END;
	SET @irr = @irr_current;

	WHILE (@irr_current - @irr_previous) > @irr_estimation
		AND @fx_der <> 0
	BEGIN
		SET @irr_previous = @irr_current
		SET @fx = (
				SELECT sum(net_annual_undiscounted * power(1 + @irr_previous, - 1 * period))
				FROM multiyear_results
				WHERE analysis_id = @analysis_id
				)
		SET @fx_der = (
				SELECT sum((net_annual_undiscounted * (- 1 * period)) * power(1 + 
							@irr_previous, (- 1 * period) - 1))
				FROM multiyear_results
				WHERE analysis_id = @analysis_id
				)
		SET @irr_current = @irr_previous - CASE 
				WHEN (@fx / @fx_der) = 0
					THEN 0.00001
				ELSE (@fx / @fx_der)
				END;
		SET @irr = @irr_current;
	END
END
ELSE
BEGIN
	SET @irr = - 999999999;
END

-- compute npv, irr, bc ration, ....
SELECT analysis_id,
	sum(benefit_total_discounted) AS benefit_npv,
	--sum(cost_total_discounted) AS cost_npv,
	@irr AS irr,
	sum(net_annual_discounted) AS net_npv,
	sum(toll_rev_base_discounted) AS toll_revenue_base_npv,
	sum(toll_rev_build_discounted) AS toll_revenue_build_npv,
	sum(fare_rev_base_discounted) AS fare_revenue_base_npv,
	sum(fare_rev_build_discounted) AS fare_revenue_build_npv
INTO #multiyear_final_results
FROM multiyear_results
WHERE analysis_id = @analysis_id
GROUP BY analysis_id;

BEGIN TRANSACTION

UPDATE multiyear_final_results
SET benefit_npv = mr.benefit_npv,
	--cost_npv = mr.cost_npv,
	irr = @irr,
	net_npv = mr.net_npv,
	toll_revenue_base_npv = mr.toll_revenue_base_npv,
	toll_revenue_build_npv = mr.toll_revenue_build_npv,
	fare_revenue_base_npv = mr.fare_revenue_base_npv,
	fare_revenue_build_npv = mr.fare_revenue_build_npv
FROM multiyear_final_results mfr
INNER JOIN #multiyear_final_results AS mr
	ON mfr.analysis_id = mr.analysis_id
WHERE mr.analysis_id = @analysis_id

IF @@ROWCOUNT = 0
	INSERT INTO multiyear_final_results (
		analysis_id,
		benefit_npv,
		--cost_npv,
		irr,
		net_npv,
		toll_revenue_base_npv,
		toll_revenue_build_npv,
		fare_revenue_base_npv,
		fare_revenue_build_npv
		)
	SELECT @analysis_id,
		benefit_npv,
		--cost_npv,
		irr,
		net_npv,
		toll_revenue_base_npv,
		toll_revenue_build_npv,
		fare_revenue_base_npv,
		fare_revenue_build_npv
	FROM #multiyear_final_results
	WHERE analysis_id = @analysis_id;

--UPDATE multiyear_final_results
--SET bc_ratio = (
--		CASE 
--			WHEN cost_npv = 0
--				THEN - 999999999
--			ELSE benefit_npv / cost_npv
--			END
--		)
--WHERE analysis_id = @analysis_id;

COMMIT TRANSACTION

GO



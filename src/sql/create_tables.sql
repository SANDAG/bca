-- Create analysis definition table
CREATE TABLE [bca].[analysis] (
	[analysis_id]			integer IDENTITY(1,1) NOT NULL
	,[title]				varchar(255)	NULL
	,[description]			varchar(255)	NULL
	,[year_reference]		integer			NULL
	,[year_start]			smallint		NULL
	,[year_present]			smallint		NULL
	,[year_intermediate_1]	integer			NULL
	,[year_intermediate_2]	integer			NULL
	,[year_intermediate_3]	integer			NULL
	,[year_intermediate_4]	integer			NULL
	,[year_intermediate_5]	integer			NULL
	,[year_end]				smallint		NULL
	--,[inflation_rate]		float			NULL
	,[discount_rate]		float			NULL
	,[annualization_factor]	float			NULL
	,[last_update_date]		date			NOT NULL
	,[annual_vot_growth]	float			NULL
    ,CONSTRAINT [pk_analysis] PRIMARY KEY ([analysis_id])
)
GO

-- Create analysis parameters table
CREATE TABLE [bca].[analysis_parameters] (
	[id]							integer IDENTITY(1,1) NOT NULL
    ,[analysis_id]					integer		NOT NULL
	,[comparison_year]				smallint	NOT NULL
	,[scenario_id_base]				integer		NOT NULL
	,[scenario_id_build]			integer		NOT NULL
	,[vot_commute]					float		NULL
	,[vot_noncommute]				float		NULL
	,[vot_work]						float		NULL
	,[vot_truck_light]				float		NULL
	,[vot_truck_medium]				float		NULL
	,[vot_truck_heavy]				float		NULL
	,[vor_auto]						float		NULL
	,[vor_work]						float		NULL
	,[vor_truck_light]				float		NULL
	,[vor_truck_medium]				float		NULL
	,[vor_truck_heavy]				float		NULL
	--,[vot_uniform]					float		NULL -- always null?
	,[ovt_weight]					float		NULL
	,[ovt_time_multiplier]			float		NULL
	,[voc_auto]						float		NULL
	,[voc_truck_light]				float		NULL
	,[voc_truck_medium]				float		NULL
	,[voc_truck_heavy]				float		NULL
	,[aoc_auto]						float		NULL
	--,[phys_activity_threshold]		float		NULL -- not used with walk/bike and continuous?
	--,[cost_phys_activ]				float		NULL -- not used?
	,[crash_rate_fatal]				float		NULL
	,[crash_rate_injury]			float		NULL
	,[crash_rate_pdo]				float		NULL
	,[crash_fatal_cost]				float		NULL
	,[crash_injury_cost]			float		NULL
	,[crash_pdo_cost]				float		NULL
	,[co2_value]					float		NULL
	,[pm2_5_value]					float		NULL
	/*,[pm10_value]					float		NULL*/ --removed duplicate, other entry is used
	,[nox_value]					float		NULL
	,[rog_value]					float		NULL
	,[so2_value]					float		NULL
	,[co_value]						float		NULL
	,[pm_10_value]					float		NULL
	--,[inflation_rate]				float		NULL -- always 0? costs external
    ,[discount_rate]				float		NULL -- not sure if used
	,[coc_age_thresh]				integer		NULL
	,[coc_race_thresh]				integer		NULL
	,[coc_hinc_thresh]				integer		NULL
	,[coc_poverty_thresh]			float		NULL
	,[coc_hisp_thresh]				integer		NULL
	,[rel_ratio]					float		NULL
	,[bike_vot_recreational]		float		NULL
	,[bike_vot_non_recreational]	float		NULL
	,[walk_vot_recreational]		float		NULL
	,[walk_vot_non_recreational]	float		NULL
    ,CONSTRAINT [pk_analysis_parameters] PRIMARY KEY ([id])
    ,CONSTRAINT [fk_analysis_parameters_analysis] FOREIGN KEY ([analysis_id]) REFERENCES [bca].[analysis] ON UPDATE CASCADE ON DELETE CASCADE
    ,CONSTRAINT [uq_analysis_parameters] UNIQUE NONCLUSTERED ([analysis_id], [comparison_year])
)
GO

-- Create emfac output table
CREATE TABLE [bca].[emfac_output] (
	[scenario_id]	integer		NOT NULL
	,[Season]		nchar(10)	NOT NULL
	,[Veh_Tech]		nchar(50)	NOT NULL
	,[CO_TOTEX]		float		NOT NULL
	,[CO2_TOTEX]	float		NOT NULL
	,[NOx_TOTEX]	float		NOT NULL
	,[PM2_5_TOTAL]	float		NOT NULL
	,[PM10_TOTAL]	float		NOT NULL
	,[ROG_TOTAL]	float		NOT NULL
	,[SOx_TOTEX]	float		NOT NULL
	,CONSTRAINT pk_emfacoutput PRIMARY KEY ([scenario_id], [Season], [Veh_Tech])
WITH (
	DATA_COMPRESSION = PAGE
	)
)
GO

-- Create scenario comparison table
CREATE TABLE [bca].[scenario_comparison] (
	[id]										integer IDENTITY(1,1) NOT NULL
	,[analysis_id]								integer		NOT NULL
	,[scenario_year]							integer		NOT NULL
	,[scenario_id_base]							integer		NOT NULL
	,[scenario_id_build]						integer		NOT NULL
	,[vot_commute]								float		NULL
	,[vot_noncommute]							float		NULL
	,[vot_work]									float		NULL
	,[vot_truck_light]							float		NULL
	,[vot_truck_medium]							float		NULL
	,[vot_truck_heavy]							float		NULL
	,[vor_auto]									float		NULL
	,[vor_work]									float		NULL
	,[vor_truck_light]							float		NULL
	,[vor_truck_medium]							float		NULL
	,[vor_truck_heavy]							float		NULL
	--,[vot_uniform]								float		NULL
	,[ovt_weight]								float		NULL
	,[ovt_time_multiplier]						float		NULL
	,[voc_auto]									float		NULL
	,[voc_truck_light]							float		NULL
	,[voc_truck_medium]							float		NULL
	,[voc_truck_heavy]							float		NULL
	,[aoc_auto]									float		NULL
	--,[phys_activity_threshold]					float		NULL
	--,[cost_phys_activ]							float		NULL
	,[crash_rate_fatal]							float		NULL
    ,[crash_rate_injury]						float		NULL
    ,[crash_rate_pdo]							float		NULL
    ,[crash_fatal_cost]							float		NULL
    ,[crash_injury_cost]						float		NULL
    ,[crash_pdo_cost]							float		NULL
    ,[co2_value]								float		NULL
    ,[pm2_5_value]								float		NULL
    ,[nox_value]								float		NULL
    ,[rog_value]								float		NULL
    ,[so2_value]								float		NULL
    ,[co_value]									float		NULL
    ,[pm10_value]								float		NULL
    --,[rate_inflation]							float		NULL
    ,[rate_discount]							float		NULL
    ,[coc_age_thresh]							integer		NULL
    ,[coc_race_thresh]							integer		NULL
    ,[coc_hinc_thresh]							integer		NULL
    ,[coc_poverty_thresh]						float		NULL
    ,[coc_hisp_thresh]							integer		NULL
    ,[rel_ratio]								float		NULL
    ,[toll_auto_commute_base]					float		NULL
    ,[toll_auto_commute_build]					float		NULL
    ,[toll_auto_commute_base_coc]				float		NULL
    ,[toll_auto_commute_build_coc]				float		NULL
    ,[ben_tt_auto_commute]						float		NULL
    ,[ben_tt_equity_auto_commute]				float		NULL
    ,[ben_tt_auto_commute_coc]					float		NULL
    ,[ben_tt_equity_auto_commute_coc]			float		NULL
    ,[toll_auto_noncommute_base]				float		NULL
    ,[toll_auto_noncommute_build]				float		NULL
    ,[toll_auto_noncommute_base_coc]			float		NULL
    ,[toll_auto_noncommute_build_coc]			float		NULL
    ,[ben_tt_auto_noncommute]					float		NULL
    ,[ben_tt_equity_auto_noncommute]			float		NULL
    ,[ben_tt_auto_noncommute_coc]				float		NULL
    ,[ben_tt_equity_auto_noncommute_coc]		float		NULL
    ,[fare_transit_commute_base]				float		NULL
    ,[fare_transit_commute_build]				float		NULL
    ,[fare_transit_commute_base_coc]			float		NULL
    ,[fare_transit_commute_build_coc]			float		NULL
    ,[ben_tt_transit_commute]					float		NULL
    ,[ben_tt_equity_transit_commute]			float		NULL
    ,[ben_tt_transit_commute_coc]				float		NULL
    ,[ben_tt_equity_transit_commute_coc]		float		NULL
    ,[fare_transit_noncommute_base]				float		NULL
    ,[fare_transit_noncommute_build]			float		NULL
    ,[fare_transit_noncommute_base_coc]			float		NULL
    ,[fare_transit_noncommute_build_coc]		float		NULL
    ,[ben_tt_transit_noncommute]				float		NULL
    ,[ben_tt_equity_transit_noncommute]			float		NULL
    ,[ben_tt_transit_noncommute_coc]			float		NULL
    ,[ben_tt_equity_transit_noncommute_coc]		float		NULL
    ,[ben_tt_at_commute]						float		NULL
    ,[ben_tt_equity_at_commute]					float		NULL
    ,[ben_tt_at_noncommute]						float		NULL
    ,[ben_tt_equity_at_noncommute]				float		NULL
    ,[ben_tt_at_commute_coc]					float		NULL
    ,[ben_tt_equity_at_commute_coc]				float		NULL
    ,[ben_tt_at_noncommute_coc]					float		NULL
    ,[ben_tt_equity_at_noncommute_coc]			float		NULL
    ,[diff_tt_auto_commute]						float		NULL
    ,[diff_tt_auto_noncommute]					float		NULL
    ,[diff_tt_transit_commute]					float		NULL
    ,[diff_tt_transit_noncommute]				float		NULL
    ,[diff_toll_commercial]						float		NULL
    ,[diff_toll_truck]							float		NULL
    ,[ben_tt_comm]								float		NULL
    ,[ben_tt_truck]								float		NULL
    ,[diff_reliability]							float		NULL
    ,[ben_reliability]							float		NULL
    ,[diff_autos_owned]							integer		NULL
    ,[ben_autos_owned]							float		NULL
    ,[diff_autos_owned_coc]						integer		NULL
    ,[ben_autos_owned_coc]						float		NULL
    --,[diff_persons_phys_active]					integer		NULL
    --,[ben_persons_phys_active]					float		NULL
    --,[diff_persons_phys_active_coc]				integer		NULL
    --,[ben_persons_phys_active_coc]				float		NULL
    ,[diff_co2]									integer		NULL
    ,[diff_pm25]								integer		NULL
    ,[diff_nox]									integer		NULL
    ,[diff_rogs]								integer		NULL
    ,[diff_so2]									integer		NULL
    ,[diff_co]									integer		NULL
    ,[diff_pm10]								integer		NULL
    ,[ben_co2]									float		NULL
    ,[ben_pm25]									float		NULL
    ,[ben_nox]									float		NULL
    ,[ben_rogs]									float		NULL
    ,[ben_so2]									float		NULL
    ,[ben_co]									float		NULL
    ,[ben_pm10]									float		NULL
    ,[ben_voc_auto]								float		NULL
    ,[ben_voc_truck_lht]						float		NULL
    ,[ben_voc_truck_med]						float		NULL
    ,[ben_voc_truck_hvy]						float		NULL
    ,[ben_relcost_auto]							float		NULL
    ,[ben_relcost_truck_lht]					float		NULL
    ,[ben_relcost_truck_med]					float		NULL
    ,[ben_relcost_truck_hvy]					float		NULL
    ,[ben_crashcost_pdo]						float		NULL
    ,[ben_crashcost_inj]						float		NULL
    ,[ben_crashcost_fat]						float		NULL
    ,[toll_comm_base]							float		NULL
    ,[toll_truck_base]							float		NULL
    ,[toll_comm_build]							float		NULL
    ,[toll_truck_build]							float		NULL
    ,[last_update_date]							date		NOT NULL
    ,[base_tt_comm]								float		NULL
    ,[build_tt_comm]							float		NULL
    ,[base_tt_truck]							float		NULL
    ,[build_tt_truck]							float		NULL
    ,[base_tt_person]							float		NULL
    ,[build_tt_person]							float		NULL
    --,[base_cost_persons_phys_active]			float		NULL
    --,[build_cost_persons_phys_active]			float		NULL
    ,[base_cost_autos_owned]					float		NULL
    ,[build_cost_autos_owned]					float		NULL
    --,[diff_persons_phys_active_coc_race]		float		NULL
    --,[ben_persons_phys_active_coc_race]			float		NULL
    --,[diff_persons_phys_active_coc_age]			float		NULL
    --,[ben_persons_phys_active_coc_age]			float		NULL
    --,[diff_persons_phys_active_coc_poverty]		float		NULL
    --,[ben_persons_phys_active_coc_poverty]		float		NULL
    ,[ben_autos_owned_coc_race]					float		NULL
    ,[ben_autos_owned_coc_age]					float		NULL
    ,[ben_autos_owned_coc_poverty]				float		NULL
    ,[ben_tt_at_commute_coc_race]				float		NULL
    ,[ben_tt_auto_commute_coc_race]				float		NULL
    ,[ben_tt_transit_commute_coc_race]			float		NULL
    ,[ben_tt_at_noncommute_coc_race]			float		NULL
    ,[ben_tt_auto_noncommute_coc_race]			float		NULL
    ,[ben_tt_transit_noncommute_coc_race]		float		NULL
    ,[ben_tt_at_commute_coc_age]				float		NULL
    ,[ben_tt_auto_commute_coc_age]				float		NULL
    ,[ben_tt_transit_commute_coc_age]			float		NULL
    ,[ben_tt_at_noncommute_coc_age]				float		NULL
    ,[ben_tt_auto_noncommute_coc_age]			float		NULL
    ,[ben_tt_transit_noncommute_coc_age]		float		NULL
    ,[ben_tt_at_commute_coc_poverty]			float		NULL
    ,[ben_tt_auto_commute_coc_poverty]			float		NULL
    ,[ben_tt_transit_commute_coc_poverty]		float		NULL
    ,[ben_tt_at_noncommute_coc_poverty]			float		NULL
    ,[ben_tt_auto_noncommute_coc_poverty]		float		NULL
    ,[ben_tt_transit_noncommute_coc_poverty]	float		NULL
    ,[persons]									integer		NULL
    ,[persons_coc]								integer		NULL
    ,[persons_coc_race]							integer		NULL
    ,[persons_coc_age]							integer		NULL
    ,[persons_coc_poverty]						integer		NULL
    ,[base_rel_cost]							float		NULL
    ,[build_rel_cost]							float		NULL
	,[base_vot_bike]							float		NULL
	,[build_vot_bike]							float		NULL
	,[benefit_bike]								float		NULL
	,[benefit_bike_coc]							float		NULL
	,[benefit_bike_senior]						float		NULL
	,[benefit_bike_minority]					float		NULL
	,[benefit_bike_low_income]					float		NULL
	,[base_vot_walk]							float		NULL
	,[build_vot_walk]							float		NULL
	,[benefit_walk]								float		NULL
	,[benefit_walk_coc]							float		NULL
	,[benefit_walk_senior]						float		NULL
	,[benefit_walk_minority]					float		NULL
	,[benefit_walk_low_income]					float		NULL
    ,CONSTRAINT [pk_scenario_comparison] PRIMARY KEY ([id])
    ,CONSTRAINT [uq_scenario_comparison] UNIQUE NONCLUSTERED ([analysis_id], [scenario_year])
)

-- Add metadata for [bca].[analysis]
EXECUTE [db_meta].[add_xp] 'bca.analysis', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.analysis', 'MS_Description', 'table to specify and define BCA analyses'
EXECUTE [db_meta].[add_xp] 'bca.analysis.analysis_id', 'MS_Description', 'analysis identifier'
EXECUTE [db_meta].[add_xp] 'bca.analysis.title', 'MS_Description', 'analysis title'
EXECUTE [db_meta].[add_xp] 'bca.analysis.description', 'MS_Description', 'description of analysis'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_reference', 'MS_Description', 'reference year'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_start', 'MS_Description', 'analysis start year'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_intermediate_1', 'MS_Description', 'intermediate year 1'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_intermediate_2', 'MS_Description', 'intermediate year 2'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_intermediate_3', 'MS_Description', 'intermediate year 3'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_intermediate_4', 'MS_Description', 'intermediate year 4'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_intermediate_5', 'MS_Description', 'intermediate year 5'
EXECUTE [db_meta].[add_xp] 'bca.analysis.year_end', 'MS_Description', 'analysis end year'
--EXECUTE [db_meta].[add_xp] 'bca.analysis.inflation_rate', 'MS_Description', 'inflation rate'
EXECUTE [db_meta].[add_xp] 'bca.analysis.discount_rate', 'MS_Description', 'discount rate'
EXECUTE [db_meta].[add_xp] 'bca.analysis.annualization_factor', 'MS_Description', 'abm scenario annualization factor'
EXECUTE [db_meta].[add_xp] 'bca.analysis.last_update_date', 'MS_Description', 'date of last analysis update'
EXECUTE [db_meta].[add_xp] 'bca.analysis.annual_vot_growth', 'MS_Description', 'annual value of time growth rate'

-- Add metadata for [bca].[analysis_parameters]
EXECUTE [db_meta].[add_xp] 'bca.analysis_parameters', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.analysis_parameters', 'MS_Description', 'table to specify parameters for an analysis'

-- Add metadata for [bca].[emfac_output]
EXECUTE [db_meta].[add_xp] 'bca.emfac_output', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output', 'MS_Description', 'table to hold emfac output data for abm scenarios from emfac output xlsx files'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.scenario_id', 'MS_Description', 'abm scenario identifier'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.Season', 'MS_Description', 'emfac season - Annual,Summer,Winter'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.Veh_Tech', 'MS_Description', 'emfac vehicle class'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.CO_TOTEX', 'MS_Description', 'Carbon Monoxide - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.CO2_TOTEX', 'MS_Description', 'Carbon Dioxide - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.NOx_TOTEX', 'MS_Description', 'Nitrogen Dioxide - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.PM2_5_TOTAL', 'MS_Description', 'Fine Particulate Matter (<2.5 microns) - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.PM10_TOTAL', 'MS_Description', 'Fine Particulate Matter (<10 microns) - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.ROG_TOTAL', 'MS_Description', 'Reactive Organic Gases - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.SOx_TOTEX', 'MS_Description', 'Sulfur Oxides - Tons Per Day - Total'

-- Add metadata for [bca].[scenario_comparison]
EXECUTE [db_meta].[add_xp] 'bca.scenario_comparison', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.scenario_comparison', 'MS_Description', 'table to store differences in base/build scenarios for an analysis'
GO

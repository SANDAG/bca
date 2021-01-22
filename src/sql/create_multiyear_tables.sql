
CREATE TABLE [bca].[multiyear_final_results] (
	[id]						integer IDENTITY(1,1) NOT NULL
	,[analysis_id]				integer		NOT NULL
	--,[bc_ratio]					float		NULL
	,[benefit_npv]				float		NULL
	--,[cost_npv]					float		NULL
	,[irr]						float		NULL
	,[net_npv]					float		NULL
	,[toll_revenue_base_npv]	float		NULL
	,[toll_revenue_build_npv]	float		NULL
	,[fare_revenue_base_npv]	float		NULL
	,[fare_revenue_build_npv]	float		NULL
	,PRIMARY KEY CLUSTERED ([id] ASC) 
		WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
		ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [bca].[multiyear_final_results] WITH CHECK ADD FOREIGN KEY([analysis_id])
REFERENCES [bca].[analysis] ([analysis_id])
GO

CREATE TABLE [bca].[multiyear_results] (
	[id]												integer IDENTITY(1,1) NOT NULL
	,[scenario_id_base]									integer		NOT NULL
	,[scenario_id_build]								integer		NOT NULL
	,[analysis_id]										integer		NOT NULL
	,[comparison_year]									integer		NOT NULL
	,[period]											integer		NOT NULL
	,[ben_co]											float		NULL
	,[ben_co2]											float		NULL
	,[ben_nox]											float		NULL
	,[ben_pm10]											float		NULL
	,[ben_pm25]											float		NULL
	,[ben_rogs]											float		NULL
	,[ben_so2]											float		NULL
	,[subtotal_ben_emissions]							float		NULL
	,[ben_autos_owned_coc]								float		NULL
	,[ben_autos_owned]									float		NULL
	,[ben_crashcost_fat]								float		NULL
	,[ben_crashcost_inj]								float		NULL
	,[ben_crashcost_pdo]								float		NULL
	,[subtotal_ben_safety]								float		NULL
	--,[ben_persons_phys_active_coc]						float		NULL
	--,[ben_persons_phys_active]							float		NULL
	,[ben_relcost_auto]									float		NULL
	,[ben_relcost_truck_hvy]							float		NULL
	,[ben_relcost_truck_lht]							float		NULL
	,[ben_relcost_truck_med]							float		NULL
	,[subtotal_ben_reliability]							float		NULL
	,[ben_tt_at_commute]								float		NULL
	,[ben_tt_auto_commute]								float		NULL
	,[ben_tt_transit_commute]							float		NULL
	,[subtotal_ben_tt_commute]							float		NULL
	,[ben_tt_at_noncommute]								float		NULL
	,[ben_tt_auto_noncommute]							float		NULL
	,[ben_tt_transit_noncommute]						float		NULL
	,[subtotal_ben_tt_noncommute]						float		NULL
	,[ben_tt_at_commute_coc]							float		NULL
	,[ben_tt_auto_commute_coc]							float		NULL
	,[ben_tt_transit_commute_coc]						float		NULL
	,[subtotal_ben_tt_commute_coc]						float		NULL
	,[ben_tt_at_noncommute_coc]							float		NULL
	,[ben_tt_auto_noncommute_coc]						float		NULL
	,[ben_tt_transit_noncommute_coc]					float		NULL
	,[subtotal_ben_tt_noncommute_coc]					float		NULL
	,[ben_tt_comm]										float		NULL
	,[ben_tt_truck]										float		NULL
	,[subtotal_ben_freight]								float		NULL
	,[ben_voc_auto]										float		NULL
	,[ben_voc_truck_lht]								float		NULL
	,[ben_voc_truck_med]								float		NULL
	,[ben_voc_truck_hvy]								float		NULL
	,[subtotal_ben_voc]									float		NULL
	,[toll_rev_base]									float		NULL
	,[toll_rev_build]									float		NULL
	,[fare_rev_base]									float		NULL
	,[fare_rev_build]									float		NULL
	--,[cost_capital]										float		NULL
	--,[cost_om]											float		NULL
	--,[cost_finance]										float		NULL
	,[benefit_total_undiscounted]						float		NULL
	--,[cost_total_undiscounted]							float		NULL
	,[benefit_total_discounted]							float		NULL
	--,[cost_total_discounted]							float		NULL
	,[net_annual_undiscounted]							float		NULL
	,[net_annual_discounted]							float		NULL
	,[toll_rev_base_discounted]							float		NULL
	,[toll_rev_build_discounted]						float		NULL
	,[fare_rev_base_discounted]							float		NULL
	,[fare_rev_build_discounted]						float		NULL
	,[benefit_emissions_discounted]						float		NULL
	,[benefit_autos_owned_discounted]					float		NULL
	,[benefit_safety_discounted]						float		NULL
	--,[benefit_phys_active_discounted]					float		NULL
	,[benefit_reliability_discounted]					float		NULL
	,[benefit_tt_commute_discounted]					float		NULL
	,[benefit_tt_noncommute_discounted]					float		NULL
	,[benefit_tt_freight_discounted]					float		NULL
	,[benefit_voc_discounted]							float		NULL
	--,[cost_capital_discounted]							float		NULL
	--,[cost_om_discounted]								float		NULL
	--,[cost_finance_discounted]							float		NULL
	,[benefit_autos_owned_discounted_coc]				float		NULL
	--,[benefit_phys_active_discounted_coc]				float		NULL
	,[benefit_tt_commute_discounted_coc]				float		NULL
	,[benefit_tt_noncommute_discounted_coc]				float		NULL
	,[base_tt_person]									float		NULL
	,[base_tt_person_discounted]						float		NULL
	,[build_tt_person]									float		NULL
	,[build_tt_person_discounted]						float		NULL
	,[base_tt_truck_comm]								float		NULL
	,[build_tt_truck_comm]								float		NULL
	,[ratio_tt_person]									float		NULL
	,[ratio_tt_truck_comm]								float		NULL
	,[base_tt_truck_comm_discounted]					float		NULL
	,[build_tt_truck_comm_discounted]					float		NULL
	,[ben_autos_owned_coc_race]							float		NULL
	,[ben_autos_owned_coc_age]							float		NULL
	,[ben_autos_owned_coc_poverty]						float		NULL
	--,[ben_persons_phys_active_coc_race]					float		NULL
	--,[ben_persons_phys_active_coc_age]					float		NULL
	--,[ben_persons_phys_active_coc_poverty]				float		NULL
	,[ben_tt_at_commute_coc_race]						float		NULL
	,[ben_tt_at_commute_coc_age]						float		NULL
	,[ben_tt_at_commute_coc_poverty]					float		NULL
	,[ben_tt_auto_commute_coc_race]						float		NULL
	,[ben_tt_auto_commute_coc_age]						float		NULL
	,[ben_tt_auto_commute_coc_poverty]					float		NULL
	,[ben_tt_transit_commute_coc_race]					float		NULL
	,[ben_tt_transit_commute_coc_age]					float		NULL
	,[ben_tt_transit_commute_coc_poverty]				float		NULL
	,[subtotal_ben_tt_commute_coc_race]					float		NULL
	,[subtotal_ben_tt_commute_coc_age]					float		NULL
	,[subtotal_ben_tt_commute_coc_poverty]				float		NULL
	,[ben_tt_at_noncommute_coc_race]					float		NULL
	,[ben_tt_at_noncommute_coc_age]						float		NULL
	,[ben_tt_at_noncommute_coc_poverty]					float		NULL
	,[ben_tt_auto_noncommute_coc_race]					float		NULL
	,[ben_tt_auto_noncommute_coc_age]					float		NULL
	,[ben_tt_auto_noncommute_coc_poverty]				float		NULL
	,[ben_tt_transit_noncommute_coc_race]				float		NULL
	,[ben_tt_transit_noncommute_coc_age]				float		NULL
	,[ben_tt_transit_noncommute_coc_poverty]			float		NULL
	,[subtotal_ben_tt_noncommute_coc_race]				float		NULL
	,[subtotal_ben_tt_noncommute_coc_age]				float		NULL
	,[subtotal_ben_tt_noncommute_coc_poverty]			float		NULL
	,[benefit_autos_owned_discounted_coc_race]			float		NULL
	,[benefit_autos_owned_discounted_coc_age]			float		NULL
	,[benefit_autos_owned_discounted_coc_poverty]		float		NULL
	--,[benefit_phys_active_discounted_coc_race]			float		NULL
	--,[benefit_phys_active_discounted_coc_age]			float		NULL
	--,[benefit_phys_active_discounted_coc_poverty]		float		NULL
	,[benefit_tt_commute_discounted_coc_race]			float		NULL
	,[benefit_tt_commute_discounted_coc_age]			float		NULL
	,[benefit_tt_commute_discounted_coc_poverty]		float		NULL
	,[benefit_tt_noncommute_discounted_coc_race]		float		NULL
	,[benefit_tt_noncommute_discounted_coc_age]			float		NULL
	,[benefit_tt_noncommute_discounted_coc_poverty]		float		NULL
	,[persons]											integer		NULL
	,[persons_coc]										integer		NULL
	,[persons_coc_race]									integer		NULL
	,[persons_coc_age]									integer		NULL
	,[persons_coc_poverty]								integer		NULL
	,[base_rel_cost]									float		NULL
	,[build_rel_cost]									float		NULL
	,[base_rel_cost_discounted]							float		NULL
	,[build_rel_cost_discounted]						float		NULL
	,[ben_bike]											float		NULL
	,[ben_bike_coc_age]									float		NULL
	,[ben_bike_coc_poverty]								float		NULL
	,[ben_bike_coc_race]								float		NULL
	,[ben_walk]											float		NULL
	,[ben_walk_coc_age]									float		NULL
	,[ben_walk_coc_poverty]								float		NULL
	,[ben_walk_coc_race]								float		NULL
	,[ben_bike_coc]										float		NULL
	,[ben_walk_coc]										float		NULL
	,[benefit_bike_discounted]							float		NULL
	,[benefit_bike_discounted_coc]						float		NULL
	,[benefit_walk_discounted]							float		NULL
	,[benefit_walk_discounted_coc]						float		NULL
	,[benefit_bike_discounted_coc_age]					float		NULL
	,[benefit_bike_discounted_coc_poverty]				float		NULL
	,[benefit_bike_discounted_coc_race]					float		NULL
	,[benefit_walk_discounted_coc_age]					float		NULL
	,[benefit_walk_discounted_coc_poverty]				float		NULL
	,[benefit_walk_discounted_coc_race]					float		NULL
	,PRIMARY KEY CLUSTERED ([id] ASC) 
		WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
		ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [bca].[multiyear_results] WITH CHECK ADD FOREIGN KEY([analysis_id])
REFERENCES [bca].[analysis] ([analysis_id])
GO
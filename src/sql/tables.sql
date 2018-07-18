-- Create emfac output table
CREATE TABLE [bca].[emfac_output] (
	[scenario_id] integer NOT NULL
	,[Season] nchar(10) NOT NULL
	,[Veh_Tech] nchar(50) NOT NULL
	,[CO2_TOTEX] float NOT NULL
	,[NOx_TOTEX] float NOT NULL
	,[PM2_5_TOTAL] float NOT NULL
	,[PM10_TOTAL] float NOT NULL
	,[ROG_TOTAL] float NOT NULL
	,[SOx_TOTEX] float NOT NULL
	,CONSTRAINT pk_emfacoutput PRIMARY KEY ([scenario_id], [Season], [Veh_Tech])
WITH (
	DATA_COMPRESSION = PAGE
	)
)
GO

-- Add metadata for [bca].[emfac_output]
EXECUTE [db_meta].[add_xp] 'bca.emfac_output', 'SUBSYSTEM', 'bca'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output', 'MS_Description', 'table to hold emfac output data for abm scenarios from emfac output xlsx files'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.scenario_id', 'MS_Description', 'abm scenario identifier'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.Season', 'MS_Description', 'emfac season - Annual,Summer,Winter'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.Veh_Tech', 'MS_Description', 'emfac vehicle class'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.CO2_TOTEX', 'MS_Description', 'Carbon Dioxide - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.NOx_TOTEX', 'MS_Description', 'Nitrogen Dioxide - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.PM2_5_TOTAL', 'MS_Description', 'Fine Particulate Matter (<2.5 microns) - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.PM10_TOTAL', 'MS_Description', 'Fine Particulate Matter (<10 microns) - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.ROG_TOTAL', 'MS_Description', 'Reactive Organic Gases - Tons Per Day - Total'
EXECUTE [db_meta].[add_xp] 'bca.emfac_output.SOx_TOTEX', 'MS_Description', 'Sulfur Oxides - Tons Per Day - Total'
GO
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
GO
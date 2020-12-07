CREATE PROCEDURE [bca].[sp_clear_analysis] @analysis_id INT
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM bca.multiyear_final_results
    WHERE analysis_id = @analysis_id;

    DELETE FROM bca.multiyear_results
    WHERE analysis_id = @analysis_id;

    DELETE FROM bca.scenario_comparison
    WHERE analysis_id = @analysis_id;

    DELETE FROM bca.analysis_parameters
    WHERE analysis_id = @analysis_id;

    DELETE FROM bca.analysis
    WHERE analysis_id = @analysis_id;    
END
GO



CREATE PROC [dbo].[Sync_Dim_MagnitudeType]
AS
BEGIN
    -- Check if the Dim_MagnitudeType table exists, and if not, create it
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'geo' AND TABLE_NAME = 'Dim_MagnitudeType')
    BEGIN
        CREATE TABLE geo.Dim_MagnitudeType(
            MAGNITUDETYPE_ID INT,
            MAGNITUDETYPE VARCHAR(20)
        );
    END

    -- Insert new records
    INSERT INTO geo.Dim_MagnitudeType (MAGNITUDETYPE, MAGNITUDETYPE_ID)
    SELECT DISTINCT
        SOURCE.magnitudeType,
        ROW_NUMBER() OVER (ORDER BY target.MAGNITUDETYPE) + ISNULL((SELECT MAX(MAGNITUDETYPE_ID) FROM geo.Dim_MagnitudeType), 0) AS MAGNITUDETYPE_ID
    FROM [LH].[dbo].[Earthquake_Data] AS SOURCE
    LEFT JOIN geo.Dim_MagnitudeType AS target
    ON SOURCE.magnitudeType = target.MAGNITUDETYPE
    WHERE target.MAGNITUDETYPE IS NULL;
END;
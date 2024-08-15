CREATE PROC [dbo].[InsertPipelineLog]
    @Workspace_ID VARCHAR(255),
    @Pipeline_ID VARCHAR(255),
    @Pipeline_Name VARCHAR(255),
    @Pipeline_Run_ID VARCHAR(255),
    @Pipeline_Trigger_Time DATETIME2(6),
    @Pipeline_Trigger_Type VARCHAR(255)
AS
BEGIN
    -- Check if the Pipeline_Logs table exists, create it if it doesn't
    IF OBJECT_ID('Pipeline_Logs', 'U') IS NULL
    BEGIN
        CREATE TABLE Pipeline_Logs (
            Workspace_ID VARCHAR(255),
            Pipeline_ID VARCHAR(255),
            Pipeline_Name VARCHAR(255),
            Pipeline_Run_ID VARCHAR(255),
            Pipeline_Trigger_Time DATETIME2(6),
            Pipeline_Trigger_Type VARCHAR(255)
        );
    END;

    -- Insert the data into the Pipeline_Logs table
    INSERT INTO Pipeline_Logs (Workspace_ID, Pipeline_ID, Pipeline_Name, Pipeline_Run_ID, Pipeline_Trigger_Time, Pipeline_Trigger_Type)
    VALUES (@Workspace_ID, @Pipeline_ID, @Pipeline_Name, @Pipeline_Run_ID, @Pipeline_Trigger_Time, @Pipeline_Trigger_Type);
END;
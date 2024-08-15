CREATE TABLE [dbo].[Pipeline_Logs] (

	[Workspace_ID] varchar(255) NULL, 
	[Pipeline_ID] varchar(255) NULL, 
	[Pipeline_Name] varchar(255) NULL, 
	[Pipeline_Run_ID] varchar(255) NULL, 
	[Pipeline_Trigger_Time] datetime2(6) NULL, 
	[Pipeline_Trigger_Type] varchar(255) NULL
);


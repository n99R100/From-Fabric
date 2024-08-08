CREATE TABLE [dbo].[Earthquake_Data] (

	[magnitude] float NULL, 
	[felt] bigint NULL, 
	[cdi] float NULL, 
	[mmi] float NULL, 
	[significance] bigint NULL, 
	[nst] bigint NULL, 
	[dmin] float NULL, 
	[rms] float NULL, 
	[gap] float NULL, 
	[magnitudeType] varchar(8000) NULL, 
	[longitude] float NULL, 
	[latitude] float NULL, 
	[depth] float NULL, 
	[id] varchar(8000) NULL, 
	[time] datetime2(6) NULL, 
	[MagnitudeID] bigint NULL
);


/*
	--change permissions to allow me to set Collation (otherwise won't allow it)

		--USE master
		--GO

		--ALTER DATABASE API_Testing 
		--SET SINGLE_USER WITH ROLLBACK IMMEDIATE 

	--check the Collation.
		--SELECT name, collation_name 
		--FROM sys.databases
		--WHERE name = 'API_Testing' 

	---- What: Change Collation from CI (Case Insensitive) to CS (Case Sensitive)
	---- Why: Album_Row_Key
		--ALTER DATABASE 'API_Testing'
		--COLLATE SQL_Latin1_General_CP1_CS_AS

	--set back to Multi User, otherwise not usable
		ALTER DATABASE API_TESTING
		SET MULTI_USER
		WITH ROLLBACK IMMEDIATE
		GO
	*/



--ALTER PROCEDURE [dbo].[Spotify_Top_Ten_New_Releases__Remove_Duplicates] AS

----CREATE TABLE [dbo].[Spotify_API_Top_Ten_New_Albums]
----(
----	  Album_SKey INT IDENTITY(1,1)
----	, Album_ID_NatKey VARCHAR(25) NOT NULL
----	, Album_Type VARCHAR(6) NOT NULL
----	, Album_Name VARCHAR(255) NOT NULL
----	, Release_Date DATE NOT NULL
----	, Release_Date_Precision VARCHAR(6)
----	, Total_Tracks INT NOT NULL
----	, Spotify_URL VARCHAR(MAX)

----	  CONSTRAINT PK_Album_SKey PRIMARY KEY CLUSTERED (Album_SKey)
----)


--INSERT INTO [dbo].[Spotify_API_Top_Ten_New_Albums]
--(
--	  [Album_ID_NatKey]
--	, [Album_Type]
--	, [Album_Name]
--	, [Release_Date]
--	, [Release_Date_Precision]
--	, [Total_Tracks]
--	, [Spotify_URL]
--)

--SELECT DISTINCT 
--  [Album_ID]
--, [Album_Type]
--, [Album_Name]
--, [Album_Release_Date]
--, [Album_Release_Date_Precision]
--, [Album_Total_Tracks]
--, [Album_Spotify_URL]

--FROM [API_Testing].[dbo].[Spotify_Top_Ten_New_Releases]

------------------------------------------------------------------------------------------------
USE API_Testing
GO

--remove duplicates from the Python ETL portion
EXEC [dbo].[Spotify_Top_Ten_New_Releases__Remove_Duplicates]
GO

SELECT * FROM [dbo].[Spotify_API_Top_Ten_New_Albums]

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

------------------------------------------------------------------------------------------------
--ALTER PROCEDURE [dbo].[Spotify_API_ETL__Populate_Warehouse_Table_With_Unique_Records] AS

--CREATE TABLE [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]
--(
--	  Album_SKey INT IDENTITY(1,1)
--	, Album_ID_NatKey VARCHAR(25) NOT NULL
--	, Album_Type VARCHAR(6) NOT NULL
--	, Album_Name VARCHAR(255) NOT NULL
--	, Release_Date DATE NOT NULL
--	, Release_Date_Precision VARCHAR(6)
--	, Total_Tracks INT NOT NULL
--	, Spotify_URL VARCHAR(MAX) NULL
--	, Record_Count INT 

--	  CONSTRAINT PK_Album_SKey PRIMARY KEY CLUSTERED (Album_SKey)
--)

--INSERT INTO [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]
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
--, CAST([Album_Release_Date] AS DATE) AS [Album_Release_Date]
--, [Album_Release_Date_Precision]
--, [Album_Total_Tracks]
--, [Album_Spotify_URL]

--FROM [dbo].[Spotify_API_ETL_SQL_LandingZone]

--TRUNCATE TABLE [dbo].[Spotify_API_ETL_SQL_LandingZone]
--UPDATE STATISTICS [dbo].[Spotify_API_ETL_SQL_LandingZone]

------------------------------------------------------------------------------------------------



--TRUNCATE TABLE [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]

/*
	SELECT
	  A.Album_ID_NatKey
	, A.Release_Date
	, count(*) AS 'ALBUM_COUNT'
	FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable] AS A

	GROUP BY   
	  A.Album_ID_NatKey
	, A.Release_Date
*/


--CREATE PROCEDURE [dbo].[Spotify_API_ETL__Deduplicate_Warehouse_Table] AS

-------------------------------------------------------------------------------

SELECT *

INTO #temp

FROM
(
	SELECT 
	 count(*) as 'RECORD_COUNT'
	, Album_ID_NatKey
	, Album_Type	
	, Album_Name	
	,Release_Date	
	, Release_Date_Precision	
	, Total_Tracks	
	, Spotify_URL

	FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]

	GROUP BY 
	  Album_ID_NatKey
	, Album_Type	
	, Album_Name	
	,Release_Date	
	, Release_Date_Precision	
	, Total_Tracks	
	, Spotify_URL

) AS DERIVED

DELETE FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]
WHERE Album_SKey NOT IN
(
	SELECT W.Album_SKey

	FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable] AS W

	JOIN #temp AS D
	ON W.Album_ID_NatKey = D.Album_ID_NatKey
	AND D.RECORD_COUNT = 1
)

USE API_Testing
GO

SELECT *  FROM [dbo].[Spotify_API_ETL_SQL_LandingZone]

SELECT * FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]
ORDER BY Album_ID_NatKey

-------------------------------------------------------------------------------

--EXEC [dbo].[Spotify_API_ETL__Populate_Warehouse_Table_With_Unique_Records] 
--GO

--EXEC [dbo].[Spotify_API_ETL__Deduplicate_Warehouse_Table] 
--GO

-------------------------------------------------------------------------------

SELECT *  FROM [dbo].[Spotify_API_ETL_SQL_LandingZone]

SELECT * FROM [dbo].[Spotify_API_ETL__New_Releases_WarehouseTable]
ORDER BY Album_SKey




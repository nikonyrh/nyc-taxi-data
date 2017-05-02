-- Imagine how trivial this is in Elasticsearch with mapping templates and aliases!

ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2009]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2010]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2011]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2012]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2013]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2014]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2015]
GO
ALTER DATABASE Niko_test ADD FILEGROUP [Niko_2016]
GO

ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2009', FILENAME = N'F:\SQLServer\Data\niko_2009.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2009]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2010', FILENAME = N'F:\SQLServer\Data\niko_2010.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2010]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2011', FILENAME = N'F:\SQLServer\Data\niko_2011.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2011]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2012', FILENAME = N'F:\SQLServer\Data\niko_2012.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2012]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2013', FILENAME = N'F:\SQLServer\Data\niko_2013.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2013]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2014', FILENAME = N'F:\SQLServer\Data\niko_2014.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2014]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2015', FILENAME = N'F:\SQLServer\Data\niko_2015.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2015]
GO
ALTER DATABASE Niko_test ADD FILE (NAME = N'niko_2016', FILENAME = N'F:\SQLServer\Data\niko_2016.ndf', SIZE = 100MB, MAXSIZE = 100000MB, FILEGROWTH = 500MB) TO FILEGROUP [Niko_2016]
GO

CREATE PARTITION FUNCTION FullOrderDateKeyRangePFN(DATETIME2)  AS
  RANGE LEFT FOR VALUES
 ('20091231 23:59:59.997', '20101231 23:59:59.997', '20111231 23:59:59.997', '20121231 23:59:59.997', '20131231 23:59:59.997', '20141231 23:59:59.997', '20151231 23:59:59.997', '20161231 23:59:59.997')

CREATE PARTITION SCHEME FullOrderDateRangePScheme  AS
  PARTITION FullOrderDateKeyRangePFN  TO
 ([Niko_2009], [Niko_2010], [Niko_2011], [Niko_2012], [Niko_2013], [Niko_2014], [Niko_2015], [Niko_2016], [PRIMARY])

CREATE TABLE [dbo].[taxi_trips](
	[company] [varchar](64) NOT NULL,
	[dlat_km] [float] NULL,
	[dlon_km] [float] NULL,
	[dropoff_dt] [datetime2](7) NULL,
	[dropoff_lat] [float] NULL,
	[dropoff_lon] [float] NULL,
	[dropoff_time] [float] NULL,
	[n_passengers] [int] NULL,
	[paid_fare] [float] NULL,
	[paid_tax] [float] NULL,
	[paid_tip] [float] NULL,
	[paid_tolls] [float] NULL,
	[paid_total] [float] NULL,
	[pickup_day] [int] NULL,
	[pickup_dt] [datetime2](7) NOT NULL,
	[pickup_lat] [float] NULL,
	[pickup_lon] [float] NULL,
	[pickup_time] [float] NULL,
	[speed_kmph] [float] NULL,
	[travel_h] [float] NULL,
	[travel_km] [float] NULL,
	[weather_AWND] [float] NULL,
	[weather_PRCP] [float] NULL,
	[weather_SNOW] [float] NULL,
	[weather_SNWD] [float] NULL,
	[weather_TMAX] [float] NULL,
	[weather_TMIN] [float] NULL
) ON FullOrderDateRangePScheme(pickup_dt)

-- Adding page compression if row-store is used
ALTER TABLE taxi_trips REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);

-- Clustered columnstore commands
CREATE CLUSTERED COLUMNSTORE INDEX [cci-niko-test] ON [dbo].[taxi_trips] WITH (DROP_EXISTING = OFF)
ALTER INDEX idx_cci_target ON cci_target REORGANIZE

-- Inserted in 873377088 rows in 3h 41m to clustered column-store index, 40.5 GB

SELECT *
  FROM hrishi.hrishi.dbo.netflix_row where show_id='s5023';

  SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'netflix_row' AND COLUMN_NAME = 'title';

create TABLE [dbo].[netflix_row](
	[show_id] [varchar](10) NOT NULL,
	[type] [varchar](10) NULL,
	[title] [nvarchar](200) NULL,
	[director] [varchar](250) NULL,
	[cast] [varchar](1000) NULL,
	[country] [varchar](150) NULL,
	[date_added] [varchar](20) NULL,
	[release_year] [int] NULL,
	[rating] [varchar](10) NULL,
	[duration] [varchar](10) NULL,
	[listed_in] [varchar](100) NULL,
	[description] [varchar](500) NULL
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [dbo].[netflix_row] ADD PRIMARY KEY CLUSTERED 
(
	[show_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

-- Handling foreign characters
SELECT title 
FROM hrishi.dbo.netflix_row
WHERE show_id = 's5023';

select * from hrishi.dbo.netflix_row  order by title;

select * from hrishi.dbo.netflix_row  where show_id='s8795';

UPDATE hrishi.dbo.netflix_row
SET title = N'اشتباك'
WHERE show_id = 's8795';
commit;-- updated foreign character individually but it has to autofix with nvarchar datatype


--Handling Duplicates

Select show_id, count(*) from hrishi.dbo.netflix_row group by show_id having count(*)>1;--No duplicate with 

Select title, count(*) from hrishi.dbo.netflix_row group by title having count(*)>1;

select * from hrishi.dbo.netflix_row where concat(title,type) in (select concat(title,type) from hrishi.hrishi.dbo.netflix_row group by title,type having count(*)>1) order by title;--duplicate record check wihin same type 

with Cte
AS
(select *, row_number() over (partition by title,type order by show_id) rn from hrishi.dbo.netflix_row)
select * from cte where rn=1;--shows 3 less records means we can delete  duplicate

----------------------- new table for listed_in, director, country, cast-------------------

select * from hrishi.dbo.netflix_row;--as we can see many directors in derector column in one row so it is quite difficult to filter out with one director name so we create separate table

select show_id, trim(value) as Director into hrishi.dbo.netflix_directors from hrishi.dbo.netflix_row cross apply string_split(director,',');--create new table netflix_directors

select show_id, trim(value) as country into hrishi.dbo.netflix_country from hrishi.dbo.netflix_row cross apply string_split(country,',');--create new table netflix_country

select show_id, trim(value) as cast into hrishi.dbo.netflix_cast from hrishi.dbo.netflix_row cross apply string_split(cast,',');--create new table netflix_cast

select show_id, trim(value) as genre into hrishi.dbo.netflix_genre from hrishi.dbo.netflix_row cross apply string_split(listed_in,',');--create new table netflix_genre



-- data type conversions for date added
-- populate missing values in country, duration columns
-- populate rest of the nulls as not_available
-- drop columns director , listed_in, country, cast

SELECT SCHEMA_NAME(schema_id) AS SchemaName, name AS TableName
FROM sys.tables
WHERE name = 'netflix_row';

EXEC sp_rename '[hrishi].[dbo].[netflix_row]]]', 'netflix_row';
















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
with cte
AS
(select *, row_number() over (partition by title, type order by show_id) as rn 
from hrishi.dbo.netflix_row)
select show_id,type,title,cast(date_added as date) as date_added,release_year,rating,duration,description from cte where rn=1;
-- populate missing values in country, duration columns
select * from hrishi.dbo.netflix_row where country is null;
select * from hrishi.dbo.netflix_row where show_id='s1001';
select * from hrishi.dbo.netflix_country where show_id='s3';

insert into netflix_country
select show_id,m.country
from hrishi.dbo.netflix_row nr
inner join (
	select director, country from netflix_country nc inner join netflix_directors nd on nc.show_id=nd.show_id
group by director, country
) m on nr.director=m.director 
where nr.country is null;--Inserting show_id and country in netflix_country

select * --show_id,country
from hrishi.dbo.netflix_row
where director='Ahishor Solomon';

select director, country from netflix_country nc inner join netflix_directors nd on nc.show_id=nd.show_id
group by director, country order by director;

select * from netflix_row where duration is null;--Actually in rating duration is showing so same thing need to update in duration
with cte
AS
(select *, row_number() over (partition by title, type order by show_id) as rn 
from hrishi.dbo.netflix_row)
select show_id,type,title,cast(date_added as date) as date_added,release_year,rating,case when duration is null then rating else duration end as duration,description into hrishi.dbo.netflix from cte where rn=1;--New netflix table with all most duration column is filled 

select * from hrishi.dbo.netflix;
------------netflix data analysis question-----------

/*1 
for each director count the no of movies and tv shows created by them in separate columns
for directors who have created tv shows and movies both 
*/
select nd.director
, count(distinct case when n.type='Movie' then n.show_id end) as no_of_movie,
count(distinct case when n.type='TV Show' then n.show_id end) as no_of_tv_show 
from hrishi.dbo.netflix n inner join netflix_directors nd on n.show_id=nd.show_id group by nd.director
having count(distinct n.type)>1; 

-- 2 which country has highest number of comedy movies
select top 1 count(n.title) movie_count,c.country from hrishi.dbo.netflix n inner join hrishi.dbo.netflix_genre g on n.show_id=g.show_id inner join hrishi.dbo.netflix_country c on n.show_id=c.show_id  where n.type='movie' and g.genre='Comedies' group by c.country order by movie_count desc;
-- 3 for each year (as per date added to netflix), which director has maximum number of movies released
with Cte
as
(select d.director,year(n.date_added) yearly_release, count(n.show_id) movie_count  from hrishi.dbo.netflix n inner join hrishi.dbo.netflix_directors d on n.show_id=d.show_id where n.type='movie' group by d.director,year(n.date_added)),
cte2 
as
(select *, row_number() over (partition by yearly_release order by movie_count desc, director) rn from cte) 
--order by yearly_release, movie_count desc)
select * from cte2 where rn=1; 

-- 4 what is average duration of movies in each genre
select avg(cast(replace(n.duration,' min','') as int)) avg_dur,g.genre from hrishi.dbo.netflix n inner join hrishi.dbo.netflix_genre g on n.show_id=g.show_id where n.type='movie' group by g.genre;

-- 5 find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them
with cte
as
(select d.director,count(case when g.genre='Horror Movies' then 1 end) Horror_Movies, count( case when g.genre='Comedies' then 1 end) Comedies from hrishi.dbo.netflix n inner join hrishi.dbo.netflix_genre g on n.show_id=g.show_id inner join hrishi.dbo.netflix_directors d on n.show_id=d.show_id where g.genre ='Horror Movies' or g.genre ='Comedies' and n.type='movie'
group by d.director)
select * from cte;

-- populate rest of the nulls as not_available
-- drop columns director , listed_in, country, cast

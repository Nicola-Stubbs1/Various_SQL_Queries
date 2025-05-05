library(duckdb)

# Sets up DuckDB Connection
con <- dbConnect(duckdb())

# Sets the path to the csv file - dummy Data
path <- 'SQL Dummy data.csv'

# reads the csv file - Uses the connection, sets a table name, and uses the path to csv file
duckdb_read_csv(con, "open_pathways", path)

# brings the data into console
dbReadTable(con, "open_pathways")

# Runs SQL Query with open_pathways tables - in duckdb memory
dbGetQuery(con, "SELECT * FROM open_pathways")

# This is based on a piece of work I did to get a summary table for missing orgs
# SQL Query to get max date available for each org_code
# Using the latest date availble to estimate waiting list size, split by weeks wait.

data <- dbGetQuery(con,
"
WITH OpenPath AS 
(
SELECT 
CAST(date AS DATE) AS date,
org_code,
org_name,
MAX(date) as max_date,
--- Groups pathways in to weeks sections
CASE
WHEN CAST(weeks as float) < 0 THEN 'Unknown Clock Start Date'
WHEN CAST(weeks as float) <=18 THEN '0 to 18 weeks'
WHEN CAST(weeks as float) <=26 THEN '18 to 26 weeks'
WHEN CAST(weeks as float) <=40 THEN '26 to 40 weeks'
WHEN CAST(weeks as float) <=52 THEN '40 to 52 weeks'
WHEN CAST(weeks as float) <=65 THEN '52 to 65 weeks'
WHEN CAST(weeks as float) <=78 THEN '65 to 78 weeks'
WHEN CAST(weeks as float) <=104 THEN '78 to 104 weeks'
WHEN CAST(weeks as float) >104 THEN '104 plus weeks'
ELSE  'Unknown Clock Start Date'
END AS weeks,
COUNT(id) AS wl_count
FROM open_pathways
WHERE 
org_code IN ('RAE','RAL','RL4')
AND latest_file = '1'
AND waiting_list_type = 'RTT'
AND commisioner_type = 'English'
GROUP BY
CAST(date AS DATE) ,
org_code,
org_name,
CASE
WHEN CAST(weeks as float) < 0 THEN 'Unknown Clock Start Date'
WHEN CAST(weeks as float) <=18 THEN '0 to 18 weeks'
WHEN CAST(weeks as float) <=26 THEN '18 to 26 weeks'
WHEN CAST(weeks as float) <=40 THEN '26 to 40 weeks'
WHEN CAST(weeks as float) <=52 THEN '40 to 52 weeks'
WHEN CAST(weeks as float) <=65 THEN '52 to 65 weeks'
WHEN CAST(weeks as float) <=78 THEN '65 to 78 weeks'
WHEN CAST(weeks as float) <=104 THEN '78 to 104 weeks'
WHEN CAST(weeks as float) >104 THEN '104 plus weeks'
ELSE  'Unknown Clock Start Date'
END )
, MaxDate as
(
-- Creates a table where only matches with the Max available date
SELECT * FROM OpenPath as open 
INNER JOIN 
(
SELECT org_code
,MAX(date) AS Max_avail_date 
FROM OpenPath
GROUP BY org_code) AS d --- Nested query to get maximum dates for each org
ON open.org_code = d.org_code
WHERE date = Max_avail_date)

-- Basically creating a pivot table of results for maximum week, split by weeks group
SELECT
date,
    org_code,
    org_name,
    SUM(wl_count) AS TotalWL,
    SUM(CASE WHEN weeks = '0 to 18 weeks' THEN wl_count ELSE 0 END) AS '0 to 18 weeks',
    SUM(CASE WHEN weeks = '18 to 26 weeks' THEN wl_count ELSE 0 END) AS '18 to 26 weeks',
    SUM(CASE WHEN weeks = '26 to 40 weeks' THEN wl_count ELSE 0 END) AS '26 to 40 weeks',
    SUM(CASE WHEN weeks = '40 to 52 weeks' THEN wl_count ELSE 0 END) AS '40 to 52 weeks',
    SUM(CASE WHEN weeks = '52 to 65 weeks' THEN wl_count ELSE 0 END) AS '52 to 65 weeks',
    SUM(CASE WHEN weeks = '65 to 78 weeks' THEN wl_count ELSE 0 END) AS '65 to 78 weeks',
    SUM(CASE WHEN weeks = '78 to 104 weeks' THEN wl_count ELSE 0 END) AS '78 to 104 weeks',
    SUM(CASE WHEN weeks = '104 plus weeks' THEN wl_count ELSE 0 END) AS '104 plus weeks',
    SUM(CASE WHEN weeks = 'Unknown Clock Start Date' THEN wl_count ELSE 0 END) AS 'Unknown Clock Start Date'
FROM
    MaxDate  
GROUP BY
  date,
  org_code,
  org_name;
"
)
                  
#disconnects session
dbDisconnect(con)
                  
                  
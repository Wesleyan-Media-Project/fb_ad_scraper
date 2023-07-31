## run with the following command:
## nohup R CMD BATCH --no-save --no-restore insert_ad_ids_into_queue.R  ./Logs/insert_ad_ids_$(date +%Y-%m-%d).txt &

library(dplyr)
library(RMySQL)
library(readr)
library(bigrquery)

## open a connection to the MySQL server
## this is on wesmedia3
conn = dbConnect(RMySQL::MySQL(), host="localhost",
                 user="xxxxx", password="xxxxx",
                 dbname="dbase1")

## this query will insert all available ad_ids into the ad_queue table
q = "INSERT INTO ad_queue SELECT distinct page_id, id, 'zzzz' as date from race2022"
q = gsub('zzzz', strftime(Sys.Date(), "%Y-%m-%d"), q)
x = dbGetQuery(conn, q)

## then, we delete the rows for ads that have already been scraped
## their ids are present in the fb_ads_media table
q = "drop table if exists tmp_ad_queue"
x = dbGetQuery(conn, q)

## this is the query
## it uses a temporary table
q = "create temporary table tmp_ad_queue as 
  select * from (
  with a as (select distinct ad_id, page_id, date from ad_queue),
	b as (select ad_id, 1 as was_found from fb_ads_media where import_time is not null),
	c as (select a.ad_id, a.page_id, a.date, b.was_found from a left join b using (ad_id))
	select ad_id, page_id, max(date) as date from c where was_found is null
	  group by ad_id, page_id
  ) as y
"
x = dbGetQuery(conn, q)

q = "truncate ad_queue"
x = dbGetQuery(conn, q)

q = "INSERT INTO ad_queue SELECT page_id, ad_id, date from tmp_ad_queue"
x = dbGetQuery(conn, q)

dbDisconnect(conn)

quit('no')

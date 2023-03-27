library(dplyr)
library(RMySQL)
library(readr)
library(bigrquery)

## open a connection to the MySQL server
## this is on wesmedia3
conn = dbConnect(RMySQL::MySQL(), host="localhost",
                 user="xxxxx", password="xxxxx",
                 dbname="ad_media")

## authorize the project to enable access to BigQuery
bq_auth(path="/home/poleinikov/wmp-laura.json")

q = "select page_id, ad_id, date from fbri.ad_queue where date = 'zzz';"
q = gsub('zzz', Sys.Date(), q)

d = bq_table_download(
      bq_project_query(
          x = 'wmp-laura',
          query = q),
      bigint = "integer64")

d %>% mutate(ad_id = as.character(ad_id),
             page_id = as.character(page_id)) -> d2

dbWriteTable(conn, name="ad_queue", value=d2,
             overwrite=F, append=T, row.names=F)

q = "INSERT INTO ad_queue SELECT distinct page_id, id, 'zzzz' as date from textsim_new.race2022"
q = gsub('zzzz', strftime(Sys.Date(), "%Y-%m-%d"), q)
x = dbGetQuery(conn, q)

##
## this part was added after I redirected back-pull to write into textsim_utf8.race2022_utf8
##
q = "INSERT INTO ad_queue SELECT distinct page_id, id, 'zzzz' as date from textsim_utf8.race2022_utf8"
q = gsub('zzzz', strftime(Sys.Date(), "%Y-%m-%d"), q)
x = dbGetQuery(conn, q)

# dbDisconnect(conn)
# quit('no')

## the part below was used to prevent repeated downloads
## of the same ad, but now I do not need it.
q = "drop table if exists tmp_ad_queue"
x = dbGetQuery(conn, q)

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


## in images - 479 bytes - letter "f" for Facebook. Probably says that you need to log in



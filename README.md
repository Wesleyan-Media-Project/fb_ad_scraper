# fb_ad_scraper
Files associated with the pipeline for scraping Facebook ads

## The ad queue
The scraper relies on a database maintained in MySQL server. This database, named `ad_media` stores information about scraped ads and also has the queue for the scraper.

An ad is placed into a queue if it has not been scraped before. There are three sources for the ads:

* a table in BigQuery. This functionality was introduced when we needed to scrape a large number of ads whose ids came from an outside archive.
* table `race2020_new` in database `textsim_new` residing in MySQL server. This table contains the ads that were retrieved by the Facebook Ads API using the keyword-based search.
* table `race2020_utf8` from the database `textsim_utf8` in MySQL server. This table (and its parent database) were created to properly handle UTF-8 encoding in the ads. The `textsim_new` database does not UTF8 collation and thus it damages the Unicode that is present in the ads (such as emoji, or accented characters). To remedy this problem without risking losing the data, we created a new database, with the UTF-8 friendly collation. A separate script `backpull` retrieves the ads from the pages whose ads were "caught" using the keyword based searches.

Sometimes we want to repeat the scraping, even though the ad has been scraped before. This happens in the scenarios when we have reasons to think that the Ad Renderer server maintained by Meta had a "bad day" and was generating transient errors. When that is the case, we use a shortcut in the script - a commented out line that ignores the three sources of ads and proceeds to work only with the ads that are already in the `ad_queue` table.

## The scraper
The scraper is a Python script that uses Google Chrome in headless mode together with Selenium.

As a first step, the script downloads a list of ad ids to scrape. The ids come from the `ad_queue` table in the `ad_media` database on the wesmedia3 server.

The script will accept one command-line argument: `offset`. This argument controls the `offset` parameter in the query to the server. The query will retrieve 20,000 ad ids and normally the offset parameter is set to 20,000. This parameter was introduced to enable scraping by several scripts in parallel. Each script is launched in a bash file, and the difference is the offset value.

We have discovered that the Ad Renderer server has a limit on the number of page visits, and if we launched three scraper threads, the limit would be exceeded. Thus, we launch at most two threads.

The ads are accessed through their URLs provided by the FB API. A special feature, not available to the public, is that if we add the access token to the URL, we are shown a page that contains only one ad, and no other information. This is a different behavior from the public access - entering an ad URL leads to a redirect to the page that shows several ads from the same page.

The access token is stored in a separate file on the server, so that there is one central copy of the token. The token is generated through the Meta's Graph API Explorer console https://developers.facebook.com/tools/explorer. The token is valid for 60 days and has to be manually renewed.

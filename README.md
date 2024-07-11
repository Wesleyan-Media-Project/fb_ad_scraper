# CREATIVE --- fb_ad_scraper

Welcome! This repo provides scripts to collect ad media (images and video) from political ads on Facebook. The scripts can be used to continuously scrape Facebook ad media and store the results in a database. There are a number of interesting use cases. For instance, the ads can be scraped for cross-platform analysis to compare advertising strategies on Facebook to those of other platforms such as Google to identify their overlaps and discrepancies.

This repo is a part of the [Cross-platform Election Advertising Transparency Initiative (CREATIVE)](https://www.creativewmp.com/). CREATIVE has the goal of providing the public with analysis tools for more transparency of political ads across online platforms. In particular, CREATIVE provides cross-platform integration and standardization of political ads collected from Google and Facebook. CREATIVE is a joint project of the [Wesleyan Media Project (WMP)](https://mediaproject.wesleyan.edu/) and the [privacy-tech-lab](https://privacytechlab.org/) at [Wesleyan University](https://www.wesleyan.edu).

To analyze the different dimensions of political ad transparency we have developed an analysis pipeline. The scripts in this repo are part of the Data Collection step in our pipeline.

![A picture of the repo pipeline with this repo highlighted](Creative_Pipelines.png)

**Note**: Before you can run the scripts in this repo, you will also need the data from the [fb_ads_import repo](https://github.com/Wesleyan-Media-Project/fb_ads_import).

## Table of Contents

- [1. Overview](#1-overview)
- [2. Setup](#2-setup)
  - [SQL Backend](#sql-backend)
  - [Directories to store the files](#directories-to-store-the-files)
  - [The ad queue](#the-ad-queue)
  - [The scraper](#the-scraper)
  - [Facebook Ad Library API](#facebook-ad-library-api)
- [3. Thank You!](#3-thank-you)

## 1. Overview

This repository contains three main scripts that are used to scrape Facebook ad media and store the results in a database.

1. The SQL statements in `table_setup.sql` are used to create the necessary tables to store the ad information. The database created by this script contains three tables:

   - `ad_queue`: stores the ad_id, its page_id, and the date when an ad was placed into the queue. The date is used to prioritize more recent ads over older ads.
   - `fb_ads_media`: stores the information about the media. The file names of the images or videos downloaded from the ad, the byte size of the file, the duration of an audio file extracted from the video, dimensions of an image as (width x height), page_id, ad_id, all URLs that were present in the ad, and the checksum of the media file. The checksum, along with the media asset URL, help us identify duplicates.
   - `fb_scrape_msg`: table to store error messages. If an ad triggered an error, there will be a message written into this table.

2. The R script `insert_ad_ids_into_queue.R` is used to populate the `ad_queue` table with new ad_ids that need to be scraped:

   - First, it checks for new ads and makes a list of them. The script looks at a master list of all ads and compares it to the ads that are already saved in `fb_ads_media`.
   - The master list of all ads is stored in the `race2022` table. This table is populated by the scripts `race2022.R` and `backpull2022.R` that are available and described in the [`fb_ads_import` repository](https://github.com/Wesleyan-Media-Project/fb_ads_import)
   - Then it creates a queue (a waiting list) of the new ads that need to be processed in the `ad_queue` table. This de-duplication process ensures that only new, unscraped ads are present in the queue.

   This script is run daily to keep the queue updated with the latest ads. For more information about this script, check the [ad_queue](#the-ad-queue) in the Setup section.

3. The Python script `fb_ad_scraper.py` is used to scrape the media from the ads. It continuously reads ad_ids and page_ids from the `ad_queue` table in the database. For each ad, the script uses Selenium to navigate to an ad's URL on Facebook, extracts the media elements (images, videos, audio), downloads the media locally, and saves their metadata (URL, size, duration, etc.) into the `fb_ads_media` table in the database. If any errors occur during the scraping process, the script logs messages to the `fb_scrape_msg` table for debugging purposes. For more information about this script, check the [scraper](#the-scraper) in the Setup section.

## 2. Setup

### Facebook Ad Library API

Scraping ads on Facebook requires an access token from the FB Ad Library API. For a description of the steps that you need to undertake to be approved for access to the API, you can refer to [Meta's API documentation](https://www.facebook.com/ads/library/api/?source=nav-header). The token is valid for 60 days and has to be manually renewed.

If your goal is to download the media from a small batch of Facebook political ads and you do not have the access token, you can use the [`FBAdLibrarian` R package written by Michael Bossetta and Rasmus Schmoekel](https://github.com/schmokel/FBAdLibrarian).

Note that in either case, you will need to have the metadata --- the `ad_shortcut_url` field from the ad record --- as a starting point. You can find this field in the header of each ad when you search them in the [Facebook Ads Liabray](https://www.facebook.com/ads/library).

### SQL Backend

Please run the SQL statements from the `table_setup.sql` file to create the necessary tables. Ideally, we recommend you to run the SQL statements from the same database you used for [fb_ads_import](https://github.com/Wesleyan-Media-Project/fb_ads_imports), for more information about setting up the SQL backend, you can look at this [Readme](https://github.com/Wesleyan-Media-Project/fb_ads_import?tab=readme-ov-file#sql-backend). After you run the statements, you will have three new tables:

- `ad_queue`
- `fb_ads_media`
- `fb_scrape_msg`

### Directories to store the files

When run continuously, the script will generate a large number of files including screenshots, videos, images, and audio. For this reason, we order the files on the basis of the media type and also by the month and year when the files were downloaded. Without this, the number of files can become so large that it will substantially slow down, or even preclude, normal operations on files (e.g. list files, or find a specific file and copy it).

Create the following tree of subdirectories inside the folder where you will be running the script. The `m...` folder will need to match the month and year of when you are launching your scripts. Here is an example:

```bash
AdMedia
└── FB
    ├── audio
    │   ├── m07_2023
    │   └── m08_2023
    ├── image
    │   ├── m07_2023
    │   └── m08_2023
    ├── screenshots
    │   ├── m07_2023
    │   └── m08_2023
    └── video
        ├── m07_2023
        └── m08_2023
```

The `m07_2023` and `m08_2023` are example folders used to separate the ad media by date. For instance, if you were to run the scripts in July 2023, then the script will look for a folder named `m07_2023`. The folders are matched to the date **when the script is run**, not to any ad-related dates (e.g. ad delivery start time, ad creation time).

### The ad queue

The scraper relies on a database maintained in a MySQL server. In this repo, the name of this database is `dbase1`. The tables in this database store information about scraped ads and also have the queue for the scraper.

An ad is placed into a queue if it has not been scraped before.

The queue is populated by running the `insert_ad_ids_into_queue.R` script. To run the script from the command line, you should first change the directory in your terminal to this repository, then run the following code:

```{bash}
nohup R CMD BATCH --no-save --no-restore insert_ad_ids_into_queue.R  ./Logs/insert_ad_ids_$(date +%Y-%m-%d).txt &
```

### The scraper

The scraper is a Python script that uses Google Chrome in headless mode together with Selenium.

To run the script, first installed the required packages. You can do this by running the following command:

```bash
pip install sqlalchemy pymysql requests pandas numpy selenium librosa pillow
```

Second, you should install Google Chrome and the ChromeDriver that matches your Chrome version. You can install the ChromeDriver [here](https://developer.chrome.com/docs/chromedriver/downloads). After you download the ChromeDriver, you should add it to your System's PATH.

For Mac and Linux users, you can do this by running the following command (Please modify the below path `~/Downloads/chromedriver` to the correct path where your downloaded ChromeDriver):

```bash
mv ~/Downloads/chromedriver /usr/local/bin/
```

Third, you should prepare the necessary directories to store the files. You can follow the instructions in the [Directories to store the files](#directories-to-store-the-files) section.

Fourth, you should create a file named `tokens.txt` and put it in your Facebook API token. After you have created this file, please update the script with the correct path to your `tokens.txt` file in line:

```python
with open('/data/1/wesmediafowler/projects/FB/tokens.txt', 'r') as file:
```

Fifth, update the `db_connection_str` with your actual database credentials in line:

```python
db_connection_str = 'mysql+pymysql://username:password@localhost/ad_media'
```

Finally, you can run the script by running the following command:

```bash
python3 fb_ad_media_scrape.py
```

When the script runs, it will first connect to your MySQL database and start to download a list of ad IDs to scrape. The ids come from the `ad_queue` table in your `dbase1` database if you follow the [set up](https://github.com/Wesleyan-Media-Project/fb_ads_import?tab=readme-ov-file#3-setup) from repo [fb_ads_import](https://github.com/Wesleyan-Media-Project/fb_ads_imports).

The script will accept one command-line argument: `offset`. This argument controls the `offset` parameter in the query to the server. The query will retrieve 20,000 ad IDs and normally the offset parameter is set to 20,000. This parameter was introduced to enable scraping by several scripts in parallel. Each script is launched in a bash file, and the difference is the offset value. For example, if you want to set the offset parameter to 20,000, you would run the script as follows:

```bash
python3 fb_ad_media_scrape.py 20000
```

We have discovered that Facebook's Ad Renderer server has a limit on the number of page visits, and if we launched three scrapers at the same time, the limit would be exceeded. Thus, we recommend you launch at most two scrapers at the same time.

The ads are accessed through their URLs provided by the FB API. A special feature, not available to the public, is that if we add the access token to the URL, we are shown a page that contains only one ad, and no other information. This is a different behavior from public access --- entering an ad URL leads to a redirect to the page that shows several ads from the same page. From the standpoint of scraping, having multiple ads per page would cause difficulty for scrapers to identify the right ad, since the web page is constructed via the React framework in JavaScript and every ad has the same class strings.

## 3. Thank You

<p align="center"><strong>We would like to thank our supporters!</strong></p><br>

<p align="center">This material is based upon work supported by the National Science Foundation under Grant Numbers 2235006, 2235007, and 2235008.</p>

<p align="center" style="display: flex; justify-content: center; align-items: center;">
  <a href="https://www.nsf.gov/awardsearch/showAward?AWD_ID=2235006">
    <img class="img-fluid" src="nsf.png" height="150px" alt="National Science Foundation Logo">
  </a>
</p>

<p align="center">The Cross-Platform Election Advertising Transparency Initiative (CREATIVE) is a joint infrastructure project of the Wesleyan Media Project and privacy-tech-lab at Wesleyan University in Connecticut.

<p align="center" style="display: flex; justify-content: center; align-items: center;">
  <a href="https://www.creativewmp.com/">
    <img class="img-fluid" src="CREATIVE_logo.png"  width="220px" alt="CREATIVE Logo">
  </a>
</p>

<p align="center" style="display: flex; justify-content: center; align-items: center;">
  <a href="https://mediaproject.wesleyan.edu/">
    <img src="wmp-logo.png" width="218px" height="100px" alt="Wesleyan Media Project logo">
  </a>
</p>

<p align="center" style="display: flex; justify-content: center; align-items: center;">
  <a href="https://privacytechlab.org/" style="margin-right: 20px;">
    <img src="./plt_logo.png" width="200px" alt="privacy-tech-lab logo">
  </a>
</p>

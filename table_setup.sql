use dbase1;

create table ad_queue
(
    page_id TEXT,
    ad_id TEXT,
    date CHAR(20)
);

create table fb_ads_media 
(
  file_name TEXT, 
  type TEXT, 
  url TEXT, 
  size TEXT, 
  duration TEXT, 
  dims TEXT, 
  text_content1 TEXT, 
  text_content2 TEXT, 
  page_id TEXT, 
  ad_id TEXT, 
  all_urls TEXT, 
  import_time TEXT, 
  checksum TEXT
);

create table fb_scrape_msg
(
    msg_time TEXT,
    message TEXT
);


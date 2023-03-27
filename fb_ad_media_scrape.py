
from sqlalchemy import create_engine
import pymysql
import requests
import hashlib

import pandas as pd
import numpy as np

import datetime as dt 
from datetime import date

import subprocess
import sys

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

import time
import os
import subprocess

import librosa
from PIL import Image

import os

def get_sha256_string(filename):
  with open(filename,"rb") as f:
      bytes = f.read() # read entire file as bytes
      readable_hash = hashlib.sha256(bytes).hexdigest()
  return readable_hash

today = date.today()
today_folder = today.strftime('m%m_%Y')

offset = 0
if len(sys.argv) == 2:
  offset = int(sys.argv[1])

db_connection_str = 'mysql+pymysql://xxxxx:xxxxx@localhost/ad_media'
db_connection = create_engine(db_connection_str).connect()

query1 = f'''
    with a as (select page_id, ad_id, date from ad_queue order by date desc),
    b as (select ad_id, 1 as was_scraped from fb_ads_media where import_time is not null),
    c as (select a.page_id, a.ad_id, a.date, b.was_scraped from a left join b using (ad_id)),
    c1 as (select * from c where was_scraped is null)
    select distinct page_id, ad_id from c1 order by date desc limit 20000 offset {offset}
    '''

d = pd.read_sql(query1, con=db_connection)

file_check = '''
f = os.listdir('/data/1/wesmediafowler/projects/AdMedia/FB/image')
file_df = pd.DataFrame({'filename' : f})
file_df['exists'] = 1

k = file_df['filename'].str.contains('_1.jpg')
file_df = file_df[k].copy()
file_df['ad_id'] = file_df['filename'].str.replace('(x_)|(_1.jpg)', '')

x = pd.merge(d, file_df, how='left', on=['ad_id'])
k2 = x['exists'].isna()

d = x[k2].reset_index()
'''

sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920x1080')

with open('/data/1/wesmediafowler/projects/FB/tokens.txt', 'r') as file:
    tk = file.readlines()

clean_tokens = list(map(lambda x: x.replace('\n', ''), tk))

wd = webdriver.Chrome('/home/poleinikov/chromedriver', options=chrome_options)
N = d.shape[0]
offset = 1

for row in range(N):
  # terminate in the evening
  t = dt.datetime.now()
  t_str = t.strftime("%Y-%m-%d %H:%M:%S")
  if t.hour == 21 and t.minute < 5:
      break

  print("Row is {}, ad id is {}".format(row, d.loc[row, 'ad_id']))
  url = 'https://www.facebook.com/ads/archive/render_ad?id=' + str(d.loc[row, 'ad_id']) + '&access_token=' + clean_tokens[0]

  wd.get(url)
  wd.save_screenshot('/data/1/wesmediafowler/projects/AdMedia/FB/screenshots/' + today_folder + '/x_' + str(d.loc[row, 'ad_id']) + '.png')

  html = wd.page_source

  ## if we got a page that says You have been temporarily suspended for making too many requests
  ## we break out of the loop
  if 'too many requests' in html:
      msg_query = f'''
      insert into fb_scrape_msg (msg_time, message) 
      VALUES ('{t_str}', "Got the message about being suspended for too many requests. Terminating...")
      '''

      db_connection.execute(msg_query)
      print("Got the message about being suspended for too many requests. Terminating...")
      break

  time.sleep(np.random.randint(2, 5, 1)[0])

  all_urls = ""
  try:
    xpath = '//a'
    url_refs = wd.find_elements_by_xpath(xpath)
    r = len(url_refs)
    url_refs_list = []

    for j in range(r):
        v = url_refs[j].get_attribute('href')
        all_urls = all_urls + v + " "

  except:
    pass

  ## test for a video element. If present, download it
  tmp_df = None
  try: 
    xpath = '//video'
    elem = wd.find_element_by_xpath(xpath)
    v = elem.get_attribute('src')
    fb_video_file = 'x_' + str(d.loc[row, 'ad_id']) + '.mp4'
    fb_audio_file = 'x_' + str(d.loc[row, 'ad_id']) + '.wav'

    proc = subprocess.run(['aria2c', v, '-d /data/1/wesmediafowler/projects/AdMedia/FB/video/' + today_folder, '-o ' + fb_video_file])
    proc_code = proc.returncode
    print(proc_code)

    proc = subprocess.run(['ffmpeg', '-i', '/data/1/wesmediafowler/projects/AdMedia/FB/video/' + today_folder + '/' + fb_video_file, 
        '/data/1/wesmediafowler/projects/AdMedia/FB/audio/' + today_folder + '/' + fb_audio_file])
    proc_code = proc.returncode
    print(proc_code)

    ## if proc codes are okay, then insert the new data
    duration = librosa.get_duration(filename = '/data/1/wesmediafowler/projects/AdMedia/FB/audio/' + today_folder + '/' + fb_audio_file)
    vid_size = os.path.getsize('/data/1/wesmediafowler/projects/AdMedia/FB/video/' + today_folder + '/' + fb_video_file)
    audio_size = os.path.getsize('/data/1/wesmediafowler/projects/AdMedia/FB/audio/' + today_folder + '/' + fb_audio_file)

    checksums = ['', '']
    try:
      vid_checksum = get_sha256_string('/data/1/wesmediafowler/projects/AdMedia/FB/video/' + today_folder + '/' + fb_video_file)
      audio_checksum = get_sha256_string('/data/1/wesmediafowler/projects/AdMedia/FB/audio/' + today_folder + '/' + fb_audio_file)
      checksums = [vid_checksum, audio_checksum]
    except:
      pass

    tmp_df = pd.DataFrame({'file_name': [fb_video_file, fb_audio_file], 'type' : ["video", "audio"],
        'url' : [v, v], 
        'size' : [vid_size, audio_size], 'duration' : [duration, duration], 'dims' : ['', ''], 
        "text_content1" : ["", ""], "text_content2" : ["", ""], 'page_id': [d.loc[row, 'page_id'], d.loc[row, 'page_id'] ],
        'ad_id' : [d.loc[row, 'ad_id'], d.loc[row, 'ad_id']], 
        'all_urls' : [all_urls, all_urls],
        'import_time' : [t_str, t_str],
        'checksum': checksums})
    
    tmp_df.to_sql(name = 'fb_ads_media', con=db_connection, if_exists = "append", index = False, method='multi')

  except:
    pass

  image_df = None
  try:
    xpath = '//img'
    img_elements = wd.find_elements_by_xpath(xpath)
    r = len(img_elements)
    image_df_list = []

    for j in range(r):
        v = img_elements[j].get_attribute('src')
        fb_image_file = 'x_' + str(d.loc[row, 'ad_id']) + "_{}".format(j + 1) + '.jpg'
        proc = subprocess.run(['aria2c', v, '-d /data/1/wesmediafowler/projects/AdMedia/FB/image/' + today_folder, '-o ' + fb_image_file])
        proc_code = proc.returncode
        print(proc_code)

        image_size = os.path.getsize('/data/1/wesmediafowler/projects/AdMedia/FB/image/' + today_folder + '/' + fb_image_file)
        img_pointer = Image.open('/data/1/wesmediafowler/projects/AdMedia/FB/image/' + today_folder + '/' + fb_image_file)
        img_dims = img_pointer.size
        img_pointer.close()

        checksums = ['']
        try:
          img_checksum = get_sha256_string('/data/1/wesmediafowler/projects/AdMedia/FB/image/' + today_folder + '/' + fb_image_file)
          checksums = [img_checksum]
        except:
          pass

        image_df_list.append(pd.DataFrame({'file_name' : [fb_image_file], 'type' : ['image'], 'url' : [v],
            'size' : [image_size], 'duration': [0], 
            'dims' : ["{}x{}".format(img_dims[0], img_dims[1])],  
            'text_content1' : [""], 'text_content2' : [""],
            'page_id': [ d.loc[row, 'page_id'] ],
            'ad_id' : [ d.loc[row, 'ad_id'] ], 
            'all_urls' : [all_urls],
            'import_time' : [t_str],
            'checksum': checksums }))

    
    image_df = pd.concat(image_df_list)

    image_df.to_sql(name="fb_ads_media", con=db_connection, if_exists = "append", index=False, method="multi")

  except:
    pass

  if (tmp_df is None) and (image_df is None):
    bad_ad = f'''
    insert into fb_scrape_msg (msg_time, message)
    VALUES
    ('{t_str}', 'Ad {d.loc[row, "ad_id"]} did not have any media')
    '''
    db_connection.execute(bad_ad)

  db_connection.execute("delete from ad_queue where ad_id = {}".format(d.loc[row, "ad_id"]))


wd.close()
db_connection.close()

quit()


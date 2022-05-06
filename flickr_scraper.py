from random import random
import threading
from flickrapi import FlickrAPI
import time, json,os, json_downloader,_thread

from regex import P
import pandas as pd
import cProfile
import io
import pstats

def get_photos_json(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, part_num):
    '''
    In one page there are 500 photos.
    For license, see: https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html and https://www.flickr.com/creativecommons/
    For 500 photos, this function will take around 0.8 seconds to run.
    For 120 million photos, this function will take around 2.2 days to run.
    For 470 million photos, this function will take around 8.7 days to run.
    '''
    global timeo
    flickr = FlickrAPI(FLICKR_PUBLIC, FLICKR_SECRET, format='parsed-json')
    extras='license,owner_name,url_o,o_dims,tags,description,date_upload,date_taken'
    #make rand num 1 to 10000
    filename = f'flickr_photos_set_license{license}_{str(page_num * 500)}}_{part_num}.parquet'
    df = pd.DataFrame(columns=['owner_name', 'title', 'height', 'width', 'url', 'id', 'description','tags',  'date_upload', 'date_taken', 'license'])
    counter = 0
    page_num = 1
    for page in range(min_pages,max_pages):
        start = time.time()
        try:
            dict = {}
            photos = flickr.photos.search(per_page=500, extras=extras, license=license, page=page)
            page_num += 1
            for photo in photos['photos']['photo']:
                data = {
                    'owner_name': photo['ownername'],
                    'title': photo['title'],
                    'height': str(photo['height_o']),
                    'width': str(photo['width_o']),
                    'url': photo['url_o'],
                    'id': str(photo['id']),
                    'description': str(photo['description']['_content'].replace('\n', ' ').replace('\r', '').replace('\t', '').replace("&quot;", "").replace("&amp;", "").replace("&#39;", "").replace("&#039;", "").replace('\"', "")),
                    'tags': str(photo['tags']),
                    #date upload is unix time so convert to date faster
                    'date_upload': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(photo['dateupload']))),
                    #'date_upload': str(photo['dateupload'].),
                    'date_taken': str(photo['datetaken']),
                    'license': str(photo['license'])
                }
                #add data to a python dictionary called dict
                dict.update(data)
                del data


            counter += 500
            df.loc[counter] = dict
            del dict
            print(str(time.time() - start))
        except Exception as e:
            print('error with' + str(page*500) + ' photos scraped, and exception:'  + str(e))
            break
    df.to_parquet(filename, compression='snappy')
    return filename



def createNewDownloadThread(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, part_num):
    download_thread = threading.Thread(target=get_photos_json, args=(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license,part_num))
    download_thread.start() 
    download_thread.join()


#createNewDownloadThread('649835cc098f813e4cd6b27f0efdcbb4', '5896052cf18c8854', 1, 10, '1', 1)
get_photos_json('649835cc098f813e4cd6b27f0efdcbb4', '5896052cf18c8854', 1, 75, '1', 1)

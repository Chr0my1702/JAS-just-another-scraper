from random import random
import threading
from flickrapi import FlickrAPI
import time, json,os, json_downloader,_thread

def get_photos_json(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, rand_num):
    '''
    In one page there are 500 photos.
    For license, see: https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html and https://www.flickr.com/creativecommons/
    For 500 photos, this function will take around 0.8 seconds to run.
    For 120 million photos, this function will take around 2.2 days to run.
    For 470 million photos, this function will take around 8.7 days to run.
    '''
    flickr = FlickrAPI(FLICKR_PUBLIC, FLICKR_SECRET, format='parsed-json')
    extras='license,owner_name,url_o,o_dims,tags,description,date_upload,date_taken'
    #make rand num 1 to 10000
    rand_num = str(rand_num)
    with open(f'flickr_photos_set_license{license}_{rand_num}.json', 'a', encoding="utf-8") as f:
        f.write('[')
        for page in range(min_pages,max_pages):
            start = time.time()
            try:
                photos = flickr.photos.search(per_page=500, extras=extras, license=license, page=page)
                for photo in photos['photos']['photo']:
                    #print(photo['tags'])
                    #remove all unicode characters and replace with space, photo['description']
                    data = {
                        'owner_name': photo['ownername'],
                        'title': photo['title'],
                        'height': str(photo['height_o']),
                        'width': str(photo['width_o']),
                        'url': photo['url_o'],
                        'id': str(photo['id']),
                        'description': str(photo['description']['_content'].replace('\n', ' ').replace('\r', '').replace('\t', '').replace("&quot;", "").replace("&amp;", "").replace("&#39;", "").replace("&#039;", "").replace('\"', "")),
                        'tags': str(photo['tags']),
                        'date_upload': str(photo['dateupload']),
                        'date_taken': str(photo['datetaken']),
                        'license': str(photo['license'])
                    }
                    json.dump(data, f)
                    f.write(', \n')
                print('time taken = '+ str(time.time() - start) + ' '+ str(page*500) + ' photos scraped')
            except Exception as e:
                print('error with' + str(page*500) + ' photos scraped, and exception:'  + str(e))
                break
        f.seek(0, 2)
        f.seek(f.tell() - 4, 0)
        f.truncate()
        f.write(']')

        num = sum(1 for line in open(f'flickr_photos_set_license{license}_{rand_num}.json'))
        print(num)
        f.close()
        os.rename(f'flickr_photos_set_license{license}_{rand_num}.json', f'flickr_photos_set_license{license}_{rand_num}_{num}.json')
    return f'flickr_photos_set_license{license}_{rand_num}_{num}.json'


def createNewDownloadThread(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, rand_num):
    download_thread = threading.Thread(target=get_photos_json, args=(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license,rand_num))
    download_thread.start() 

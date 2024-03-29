import threading
from flickrapi import FlickrAPI
import time
import pandas as pd


def get_photos_json(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, part_num):
    '''
    In one page there are 500 photos.
    For license, see: https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html and https://www.flickr.com/creativecommons/
    For 500 photos, this function will take around 0.2 seconds to run.
    For 120 million photos, this function will take around 1.2 days to run. (Anecdotal evidence) (Roughly)
    '''
    flickr = FlickrAPI(FLICKR_PUBLIC, FLICKR_SECRET, format='parsed-json')
    extras = 'license,owner_name,url_o,o_dims,tags,description,date_upload,date_taken'
    counter = 0
    page_num = 1
    array = []
    for page in range(min_pages, max_pages):
        start = time.time()
        try:
            photos = flickr.photos.search(
                per_page=500, extras=extras, license=license, page=page)
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
                    # date upload is unix time so convert to date faster
                    'date_upload': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(photo['dateupload']))),
                    # 'date_upload': str(photo['dateupload'].),
                    'date_taken': str(photo['datetaken']),
                    'license': str(photo['license'])
                }
                # add to array
                array.append(data)
                del data
            counter += 500
            print(str(time.time() - start), part_num)
        except Exception as e:
            print('error with' + str(page*500) +
                  ' photos scraped, and exception:' + str(e))
            pass
    print("finished scraping " + str(counter) + " photos")
    filename = f'flickr_photos_set_license{license}_{str(page_num * 500)}_{part_num}.parquet'
    # this is to make dataframe with those colums below, make the data from array into a dataframe and save it to parquet
    df = pd.DataFrame(array, columns=['owner_name', 'title', 'height', 'width',
                      'url', 'id', 'description', 'tags', 'date_upload', 'date_taken', 'license'])
    df.to_parquet(filename)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    return filename


def createNewDownloadThread(FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, part_num):
    download_thread = threading.Thread(target=get_photos_json, args=(
        FLICKR_PUBLIC, FLICKR_SECRET, min_pages, max_pages, license, part_num))
    download_thread.start()


# EXAMPLE CODE TO RUN THIS CODE:

# download_thread1 = createNewDownloadThread('API_KEY', 'API_SECRET', 1, 500, '1', 1) # part 1
# download_thread2 = createNewDownloadThread('API_KEY', 'API_SECRET', 501, 1000, '1', 2) # part 2
# download_thread3 =createNewDownloadThread('API_KEY', 'API_SECRET', 1001, 1500, '1', 3) # part 3

# download_tread1.start()
# download_tread2.start()
# download_tread3.start()

# download_thread1.join()
# download_thread2.join()
# download_thread3.join()

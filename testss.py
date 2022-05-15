import requests
from bs4 import BeautifulSoup
from dataclasses import replace
from itertools import count
import json
import os
import random
import requests
import time
import pandas as pd


def scrape_api(from_page, to_page, set_num, foldername):
    array = []
    token_array = ["NWSXi5mBpsQa2nxBUGAsvwivLeqfMkYViIzDdQkH","FJ2mZ2F00JrPwmbPMdUZQm4tSM2On9RkUjFuxpVv","W70vEDD3Lq9B8FW9kRHlsnxQXPnkajgrzy3eFZG6","8eAPEf8ne7m0rmCPuJmAssDtFHA4EAIG2cT6bf7W","oFoXRW79w6HXmp2oe7K2ESM9tdjN9GDhUMFs3kUb","iAm82OozU9a7PnrlAfeN8Bd6QUK8TrlDzpzmp8J8","SrtKVt9LHd7mrYhMXfonNxMkYUpQphp3Q4TcMznj","PtNC50PHTfGfWONTNqGMvh1vPvvlWYN4LogIjvnX"]
    params ={
        'token': token_array[1],
        'page': from_page,
        'group_by_pack': 0,
        'sort':"created_desc",
        'page_size': 150,
        'fields':'username,id,name,download,description,tags',
    }
    for i in range(from_page, to_page):
        start=time.time()

        try:
            r = requests.get("https://freesound.org/apiv2/search/text", params=params)
        except Exception as e:
            print("EXCEPTION:", e)
        
        try:
            if r.json()["detail"]:
                position = token_array.index(params["token"])
                params["token"] = token_array[position+1]

                try:
                    r = requests.get("https://freesound.org/apiv2/search/text", params=params)
                except Exception as e:
                    print("EXCEPTION:", e)
        except:
            pass


        for j in range(0,150):
            json_data = r.json()["results"][j]
            
            tags = ""
            for i in range(0,len(json_data["tags"])):
                tags += str(json_data["tags"][i] + ",")
            data = {
                "id": json_data["id"],
                "title": json_data["name"],
                "tags:": str(tags),
                "description": str(json_data["description"]).replace('\n','').replace('\r','').replace('\t',''),
                "username": json_data["username"],
                "download_url": json_data["download"],
            }
            array.append(data)
            del data

        del json_data
        
        params['page'] = i+1

        print(str(set_num)+": "+str(time.time()-start))

        if i == to_page/2:            
            df = pd.DataFrame(array) 
            df.to_parquet(foldername + "/" + f"freesound_parquet_{str(to_page)}_{str(set_num)}_halfsets.parquet")


    start=time.time()

    df = pd.DataFrame(array) 
    df.to_parquet(foldername + "/" + f"freesound_parquet_{str(to_page)}_{str(set_num)}.parquet")

    print("Number "+str(set_num)+ "has been convertedonvert to parquet")

import multiprocessing
if __name__ == '__main__':
    total_pages = 3700
    half_pages = int(total_pages/2)
    p =  multiprocessing.Process(target= scrape_api, args = (1, 10, 1, "freesound_parquet_real"))
    p.start()
    p.join()
    p2 = multiprocessing.Process(target= scrape_api, args = (10, 1000, 2, "freesound_parquet_real"))
    p3 = multiprocessing.Process(target= scrape_api, args = (1000, 2000, 3, "freesound_parquet_real"))
    p2.start()
    p3.start()
    p3.join()
    p4 = multiprocessing.Process(target= scrape_api, args = (2000, 3000, 4, "freesound_parquet_real"))
    p5 = multiprocessing.Process(target= scrape_api, args = (3000, 3700, 5, "freesound_parquet_real"))
    p4.start()
    p5.start()
import multiprocessing
import requests
import requests
import time
import pandas as pd


def scrape_api(from_page, to_page, set_num, foldername):
    array = []
    token_array = ["INSERT TOKEN HERE"]
    params = {
        'token': token_array[1],
        'page': from_page,
        'group_by_pack': 0,
        'sort': "created_desc",
        'page_size': 150,
        'fields': 'username,id,name,download,description,tags',
    }
    for i in range(from_page, to_page):
        start = time.time()
        params["page"] = i
        try:
            r = requests.get(
                "https://freesound.org/apiv2/search/text", params=params)
        except Exception as e:
            print("EXCEPTION:", e)

        try:
            if r.json()["detail"]:
                position = token_array.index(params["token"])
                params["token"] = token_array[position+1]

                try:
                    r = requests.get(
                        "https://freesound.org/apiv2/search/text", params=params)
                except Exception as e:
                    print("EXCEPTION:", e)
        except:
            pass

        for j in range(0, 150):
            json_data = r.json()["results"][j]

            tags = ""
            for i in range(0, len(json_data["tags"])):
                tags += str(json_data["tags"][i] + ",")
            data = {
                "id": json_data["id"],
                "title": json_data["name"],
                "tags:": str(tags),
                "description": str(json_data["description"]).replace('\n', '').replace('\r', '').replace('\t', ''),
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
            df.to_parquet(
                foldername + "/" + f"freesound_parquet_{str(to_page)}_{str(set_num)}_halfsets.parquet")

    start = time.time()

    df = pd.DataFrame(array)
    df.to_parquet(foldername + "/" +
                  f"freesound_parquet_{str(to_page)}_{str(set_num)}.parquet")

    print("Number "+str(set_num) + "has been converted to parquet")


# TO RUN THIS SCRIPT:

if __name__ == '__main__':
    p = multiprocessing.Process(
        target=scrape_api, args=(1, 10, 1, "freesound_sets"))
    p.start()
    p.join()

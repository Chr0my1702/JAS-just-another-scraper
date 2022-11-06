import requests
import pandas as pd
link_headers = ["Acoustic", "Blues", "brass%20%26%20marching%20band", "Children", "Circus%20%26%20Funfair", "Classical", "Comedy", "Country", "Drones", "Electronica%20%26%20Dance", "Fanfares", "Film", "Hip%20Hop",
                "Jazz", "Latin", "Muzak", "Pop", "Reggae", "Rnb%20%26%20Soul", "Rock", "Small%20Emotions", "Special%20Occasions",                "Spiritual%20Music", "Traditional%20Dance", "World%20%26%20Countries"]

# TO REFRESH THE HEADER RUN THIS:
#num_list = []
# for i in range(0,len(link_headers)):
#   num_link_headers = get_total_page_num_for_header(link_headers[i])
#   print(link_headers[i], num_link_headers)
#   num_list.append(num_link_headers)
# print(num_list)


def get_num_tracks(json_data):
    return len(json_data["entities"]["tracks"])


def get_total_page_num_for_header(header):
    request_url = f"https://www.epidemicsound.com/json/search/tracks/?genres={header}&order=desc&page=1"
    response = requests.get(request_url)
    json_data = response.json()
    return json_data["meta"]["totalPages"]


def get_all_tracks():
    for header in link_headers:
        total_pages = get_total_page_num_for_header(header)
        df = pd.DataFrame(pd.np.empty((0, 11)))
        # set column names
        df.columns = ["title", "id", "added", "length",
                      "bpm", "isSfx", "hasVocals", "energyLevel", "genres", "url", "metadataTags"]
        print(f"{header} has {total_pages} pages")
        for page in range(1, total_pages + 1):

            request_url = f"https://www.epidemicsound.com/json/search/tracks/?genres={header}&order=desc&page={page}"
            response = requests.get(request_url)
            json_data = response.json()

            for track in json_data["entities"]["tracks"]:

                moods = []

                for i in range(0, len(json_data["entities"]["tracks"][track]["moods"])):
                    moods.append(json_data["entities"]
                                 ["tracks"][track]["moods"][i]["slug"])

                genres = []

                for i in range(0, len(json_data["entities"]["tracks"][track]["genres"])):
                    genres.append(
                        json_data["entities"]["tracks"][track]["genres"][i]["slug"])

                data = {
                    "title": json_data["entities"]["tracks"][track]["title"],
                    "id": json_data["entities"]["tracks"][track]["id"],
                    "added": json_data["entities"]["tracks"][track]["added"],
                    "length": json_data["entities"]["tracks"][track]["length"],
                    "bpm": json_data["entities"]["tracks"][track]["bpm"],
                    "isSfx": json_data["entities"]["tracks"][track]["isSfx"],
                    "hasVocals": json_data["entities"]["tracks"][track]["hasVocals"],
                    "energyLevel": json_data["entities"]["tracks"][track]["energyLevel"],
                    "genres": genres,
                    "url": json_data["entities"]["tracks"][track]["stems"]["full"]["lqMp3Url"],
                    "metadataTags": json_data["entities"]["tracks"][track]["metadataTags"],
                    "moods": moods
                }

                df = df.append(data, ignore_index=True)
        # MAKE SURE THIS LEADS TO A FULL PATH
        df.to_parquet(f"C:\Epimusic\{header}.parquet")

# TO RUN:
# get_all_tracks()

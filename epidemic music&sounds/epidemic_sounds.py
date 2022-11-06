import requests
import pandas as pd
link_headers = ["Aircraft", "Ambience", "Animals", "Birds", "Boats", "Body", "Chairs", "Crashes", "Crowds", "Debris", "Doors", "Electric", "Fight", "Fire", "Foley", "Glass", "Gore", "Guns", "Household", "Industrial", "Insects", "Lasers",
                "Machines", "Materials", "Mechanical", "Metal", "Musical", "Noise", "Robots", "Rocks", "Storytelling", "Technology", "Telephone", "Tools", "Trains", "Vehicles", "Voices", "Water", "Weapons", "Weather", "Whistles", "Whoosh", "Wood"]

# TO REFRESH THE NUMLIST RUN THIS:
#num_list = []
# for i in range(0,len(link_headers)):
#   num_link_headers = get_total_page_num_for_header(link_headers[i])
#   print(link_headers[i], num_link_headers)
#   num_list.append(num_link_headers)
# print(num_list)

num_link_headers = [5, 34, 38, 8, 6, 16, 6, 15, 6, 9, 36, 26, 16, 12, 166, 29, 24, 96, 11,
                    34, 23, 22, 18, 58, 26, 66, 115, 54, 5, 19, 74, 64, 10, 26, 4, 73, 72, 13, 21, 9, 4, 63, 15]


def get_num_tracks(json_data):
    return len(json_data["entities"]["tracks"])


def get_total_page_num_for_header(header):
    request_url = f"https://www.epidemicsound.com/json/search/sfx/?genres={header}&order=desc&page=1"
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

        for page in range(1, total_pages + 1):

            # get number of tracks in json data
            request_url = f"https://www.epidemicsound.com/json/search/sfx/?genres={header}&order=desc&page={page}"
            response = requests.get(request_url)

            json_data = response.json()
            # print the names of the element within track
            for track in json_data["entities"]["tracks"]:
                # print track title
                print(track)
                data = {
                    "title": json_data["entities"]["tracks"][track]["title"],
                    "id": json_data["entities"]["tracks"][track]["id"],
                    "added": json_data["entities"]["tracks"][track]["added"],
                    "length": json_data["entities"]["tracks"][track]["length"],
                    "bpm": json_data["entities"]["tracks"][track]["bpm"],
                    "isSfx": json_data["entities"]["tracks"][track]["isSfx"],
                    "hasVocals": json_data["entities"]["tracks"][track]["hasVocals"],
                    "energyLevel": json_data["entities"]["tracks"][track]["energyLevel"],
                    "genres": json_data["entities"]["tracks"][track]["genres"][0]["slug"],
                    "url": json_data["entities"]["tracks"][track]["stems"]["full"]["lqMp3Url"],
                    "metadataTags": json_data["entities"]["tracks"][track]["metadataTags"],
                }
                print(data)
                df = df.append(data, ignore_index=True)

        df.to_parquet(f"{header}.parquet")


get_all_tracks()

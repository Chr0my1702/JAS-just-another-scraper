from bs4 import BeautifulSoup
import json, time, threading
from requests_html import HTMLSession
import pandas as pd

def scrape_pages_parquet(min_pages, max_pages, set_num):
    session = HTMLSession()
    counter = 0
    array = []
    thousandCounter = 0
    for page_num in range(min_pages, max_pages):
        try:
            start = time.time()
            page = session.get(f"https://freesound.org/search/?q=&page={page_num}#sound")
            soup = BeautifulSoup(page.content, "lxml")
            mydivs = soup.find_all("div", {"class": "sample_player_small"})
            for div in mydivs:
                div_soup = BeautifulSoup(str(div), "lxml")

                username = div_soup.find("a", {"class": "user"}).get_text()
                title = div_soup.find("a", {"class": "title"}).get_text()
                title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list("title")[0].replace(" ", "-")
                id = div_soup.find("div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
                download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
                description = div_soup.find("p", {"class": "description"}).get_text()
                if description[-3:] == "...":
                    try:
                        description_full_link = session.get(f"https://freesound.org/people/{username}/sounds/{id}")
                        #print(f"https://freesound.org/people/{username}/sounds/{id}")
                        description_full_soup = BeautifulSoup(description_full_link.content, "lxml")
                        description_full_div = description_full_soup.find("div", {"id": "sound_description"}).find("p").get_text()
                        description_text = "".join(description_full_div.splitlines())
                        #print(description_text)
                        description = description_text
                        del description_text
                        del description_full_div
                        del description_full_link
                    except Exception as e:
                        print("EXCEPTION:", e)
                try:
                    tag_internal = div_soup.findChildren("div", {"class": "sound_tags"})
                    for i in range(len(tag_internal)):
                        if str(tag_internal[i].get_text()) != "\n":
                            tags = str(str(tag_internal[i].get_text()).replace("\n", ",")).replace(",,", "").replace(",,,", "")
                    tags = tags[:-1]
                except:
                    tags = ""

                data = {
                    "username": username,
                    "id": str(id),
                    "title": title,
                    "description": description,
                    "download_url": download_url,
                    "tags": tags
                }
                array.append(data)
                del data
                counter += 0.001
        except Exception as e:
            print("EXCEPTION:" ,e)
        print(f"PARQUET: set {str(set_num)},  page ", page_num,", took",time.time()-start)
        #if counter Mod 1000 == 0:

        if counter ==1:
            thousandCounter += 1
            print(f"PARQUET: set {str(set_num)},  page ", page_num,", took",time.time()-start)
            df = pd.DataFrame(array, columns=["username","id", "title", "description", "download_url", "tags"])
            df.to_parquet(f"freesound_parquet_{str(set_num)}_{str(thousandCounter)}.parquet")
            del df
            array = []
    filename = f'freesound_set_{set_num}_{str(thousandCounter+1)}.parquet'
    df = pd.DataFrame(array, columns=["username","id", "title", "description", "download_url", "tags"])
    #df.to_parquet(filename)



def createNewScrapePARQUETThread(min_pages, max_pages, part_num):
    download_thread = threading.Thread(target=scrape_pages_parquet, args=(min_pages, max_pages, part_num))
    download_thread.start()

createNewScrapePARQUETThread(1, 15001, 1)
createNewScrapePARQUETThread(15002, 36602, 2)
#createNewScrapePARQUETThread(15003, 20003, 4)
#createNewScrapePARQUETThread(20004, 25004, 5)
#createNewScrapePARQUETThread(25005, 30005, 6)
#createNewScrapePARQUETThread(30006, 35006, 7)
#createNewScrapePARQUETThread(35007, 36007, 8)
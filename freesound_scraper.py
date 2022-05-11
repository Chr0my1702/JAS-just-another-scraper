from bs4 import BeautifulSoup
import json, time, threading
from requests_html import HTMLSession
import pandas as pd


def scrape_pages_json(min_pages, max_pages, set_num):
    session = HTMLSession()
    filename = f'freesound_set_{str(max_pages)}_{str(set_num)}.json'
    with open(filename, 'a') as f:
        f.write('[')
        for page_num in range(min_pages, max_pages):
            start = time.time()
            page = session.get(f"https://freesound.org/search/?q=&page={page_num}#sound")
            #soup = BeautifulSoup(page.content, "html.parser")
            soup = BeautifulSoup(page.content, "lxml")
            mydivs = soup.find_all("div", {"class": "sample_player_small"})
            for div in mydivs:
                #div_soup = BeautifulSoup(str(div), "html.parser")
                div_soup = BeautifulSoup(str(div), "lxml")

                username = div_soup.find("a", {"class": "user"}).get_text()
                title = div_soup.find("a", {"class": "title"}).get_text()
                title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list("title")[0].replace(" ", "-")
                description = div_soup.find("p", {"class": "description"}).get_text()
                id = div_soup.find("div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
                download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
                tag_internal = div_soup.findChildren("div", {"class": "sound_tags"})
                try:
                    for i in range(len(tag_internal)):
                        if str(tag_internal[i].get_text()) != "\n":
                            tags = str(str(tag_internal[i].get_text()).replace("\n", ",")).replace(",,", "").replace(",,,", "")
                    tags = tags[:-1]
                except:
                    tags = ""
                json.dump({"username": username, "id":id, "title": title, "description": description, "download_url": download_url, "tags": tags}, f)
                f.write(',\n')
            print("JSON: page ", page_num," took",time.time()-start)


        f.write(']')

def scrape_pages_parquet(min_pages, max_pages, set_num):
    session = HTMLSession()
    counter = 0
    array = []
    for page_num in range(min_pages, max_pages):
        start = time.time()
        page = session.get(f"https://freesound.org/search/?q=&page={page_num}#sound")
        soup = BeautifulSoup(page.content, "lxml")
        mydivs = soup.find_all("div", {"class": "sample_player_small"})
        for div in mydivs:
            div_soup = BeautifulSoup(str(div), "lxml")

            username = div_soup.find("a", {"class": "user"}).get_text()
            title = div_soup.find("a", {"class": "title"}).get_text()
            title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list("title")[0].replace(" ", "-")
            description = div_soup.find("p", {"class": "description"}).get_text()
            id = div_soup.find("div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
            download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
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
            counter += 1
        print(f"PARQUET: set {str(set_num)},  page ", page_num,", took",time.time()-start)
    filename = f'freesound_set_{str(counter*15)}_{set_num}.parquet'
    df = pd.DataFrame(array, columns=["username","id", "title", "description", "download_url", "tags"])
    df.to_parquet(filename)

def createNewScrapeJSONThread(min_pages, max_pages, part_num):
    download_thread = threading.Thread(target=scrape_pages_json, args=(min_pages, max_pages, part_num))
    download_thread.start()

def createNewScrapePARQUETThread(min_pages, max_pages, part_num):
    download_thread = threading.Thread(target=scrape_pages_parquet, args=(min_pages, max_pages, part_num))
    download_thread.start()

#createNewScrapeJSONThread(1, 20, 1)
createNewScrapePARQUETThread(1, 20, 1)


#get html text, process via beutiful soup.
#for each sound get title, length, license, description, tags, ect

#if description ends in ... then open url.
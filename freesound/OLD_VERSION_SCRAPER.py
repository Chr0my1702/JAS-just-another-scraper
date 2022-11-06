from bs4 import BeautifulSoup
import json
import time
import threading
from requests_html import HTMLSession
import pandas as pd


def scrape_pages_json(min_pages, max_pages, set_num):
    session = HTMLSession()
    filename = f'freesound_set_{str(max_pages)}_{str(set_num)}.json'
    with open(filename, 'a') as f:
        f.write('[')
        for page_num in range(min_pages, max_pages):
            start = time.time()
            page = session.get(
                f"https://freesound.org/search/?q=&page={page_num}#sound")
            #soup = BeautifulSoup(page.content, "html.parser")
            soup = BeautifulSoup(page.content, "lxml")
            mydivs = soup.find_all("div", {"class": "sample_player_small"})
            for div in mydivs:
                #div_soup = BeautifulSoup(str(div), "html.parser")
                div_soup = BeautifulSoup(str(div), "lxml")

                username = div_soup.find("a", {"class": "user"}).get_text()
                title = div_soup.find("a", {"class": "title"}).get_text()
                title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list(
                    "title")[0].replace(" ", "-")
                id = div_soup.find(
                    "div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
                description = div_soup.find(
                    "p", {"class": "description"}).get_text()
                if description[-3:] == "...":
                    try:
                        description_full_link = session.get(
                            f"https://freesound.org/people/{username}/sounds/{id}")
                        print(
                            f"https://freesound.org/people/{username}/sounds/{id}")
                        description_full_soup = BeautifulSoup(
                            description_full_link.content, "lxml")
                        description_full_div = description_full_soup.find(
                            "div", {"id": "sound_description"}).find("p").get_text()
                        description_text = "".join(
                            description_full_div.splitlines())
                        print(description_text)
                        description = description_text
                        del description_text
                        del description_full_div
                        del description_full_link
                    except Exception as e:
                        print("EXCEPTION:", e)
                download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
                tag_internal = div_soup.findChildren(
                    "div", {"class": "sound_tags"})
                try:
                    # tags = [if str(tag_internal[i].get_text()) != "\n":str(str(tag_internal[i].get_text()).replace("\n", ",")).replace(",,", "").replace(",,,", "") for i in range(len(tag_internal))]
                    for i in range(len(tag_internal)):
                        if str(tag_internal[i].get_text()) != "\n":
                            tags = str(str(tag_internal[i].get_text()).replace(
                                "\n", ",")).replace(",,", "").replace(",,,", "")
                    tags = tags[:-1]
                except:
                    tags = ""
                json.dump({"username": username, "id": id, "title": title,
                          "description": description, "download_url": download_url, "tags": tags}, f)
                f.write(',\n')
                del username, title, description, id, download_url, tags, div_soup, tag_internal
            del page
            del soup
            del mydivs
            del div

        f.seek(0, 2)
        f.seek(f.tell() - 4, 0)
        f.truncate()
        f.write(']')


def scrape_pages_parquet(min_pages, max_pages, set_num):
    session = HTMLSession()
    counter = 0
    array = []
    for page_num in range(min_pages, max_pages):
        try:
            start = time.time()
            page = session.get(
                f"https://freesound.org/search/?q=&page={page_num}#sound")
            soup = BeautifulSoup(page.content, "lxml")
            mydivs = soup.find_all("div", {"class": "sample_player_small"})
            for div in mydivs:
                div_soup = BeautifulSoup(str(div), "lxml")

                username = div_soup.find("a", {"class": "user"}).get_text()
                title = div_soup.find("a", {"class": "title"}).get_text()
                title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list(
                    "title")[0].replace(" ", "-")
                description = div_soup.find(
                    "p", {"class": "description"}).get_text()
                id = div_soup.find(
                    "div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
                download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
                try:
                    tag_internal = div_soup.findChildren(
                        "div", {"class": "sound_tags"})
                    for i in range(len(tag_internal)):
                        if str(tag_internal[i].get_text()) != "\n":
                            tags = str(str(tag_internal[i].get_text()).replace(
                                "\n", ",")).replace(",,", "").replace(",,,", "")
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
                # print(data)
                array.append(data)
                del data
                counter += 1
        except Exception as e:
            print("EXCEPTION:", e)
        print(f"PARQUET: set {str(set_num)},  page ",
              page_num, ", took", time.time()-start)
    filename = f'freesound_set_{str(counter*15)}_{set_num}.parquet'
    df = pd.DataFrame(array, columns=[
                      "username", "id", "title", "description", "download_url", "tags"])
    df.to_parquet(filename)


def createNewScrapeJSONThread(min_pages, max_pages, part_num):
    download_thread = threading.Thread(
        target=scrape_pages_json, args=(min_pages, max_pages, part_num))
    download_thread.start()


def createNewScrapePARQUETThread(min_pages, max_pages, part_num):
    download_thread = threading.Thread(
        target=scrape_pages_parquet, args=(min_pages, max_pages, part_num))
    download_thread.start()


createNewScrapeJSONThread(1, 36600, 1)

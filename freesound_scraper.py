
#request url and open with beutifulsoup

from bs4 import BeautifulSoup
import requests
import urllib
import json, time
max_pages = 20
#make json file and write to it
with open('freesound_samples.json', 'a') as f:
    f.write('[')
    for page_num in range(max_pages):
        start = time.time()
        page = requests.get(f"https://freesound.org/search/?q=&page={page_num}#sound")
        soup = BeautifulSoup(page.content, 'html.parser')
        mydivs = soup.find_all("div", {"class": "sample_player_small"})
        for div in mydivs:
            div_soup = BeautifulSoup(str(div), 'html.parser')

            username = div_soup.find("a", {"class": "user"}).get_text()
            title = div_soup.find("a", {"class": "title"}).get_text()
            title_internal = div_soup.find("a", {"class": "title"}).get_attribute_list("title")[0].replace(" ", "-")
            description = div_soup.find("p", {"class": "description"}).get_text()
            id = div_soup.find("div", {"class": "sample_player_small"}).get_attribute_list("id")[0]
            download_url = f"https://freesound.org/people/{username}/sounds/{id}/download/{id}__{username}__{title_internal}"
            tag_internal = div_soup.findChildren("div", {"class": "sound_tags"})
            for i in range(len(tag_internal)):
                if str(tag_internal[i].get_text()) != "\n":
                    tags = str(str(tag_internal[i].get_text()).replace("\n", ",")).replace(",,", "").replace(",,,", "")
            tags = tags[:-1]

            #print(",".join(tags))
            #json.dump({"username": username, "title": title, "description": description, "download_url": download_url, "tags": tags}, f)
            #f.write(',\n')
        print("page ", page_num," took",time.time()-start)


    f.write(']')



#get html text, process via beutiful soup.
#for each sound get title, length, license, description, tags, ect

#if description ends in ... then open url.
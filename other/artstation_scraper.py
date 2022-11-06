import json
import os
import requests
from PIL import Image
from io import BytesIO
from time import sleep
from tqdm import tqdm

# CREDIT GOES TO: BoneAmputee#8363 on Discord For assisting me with this code.

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows   10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0'}


def get_file(url):
    return requests.get(url, headers=headers).content


def download_image(url):
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return Image.open(BytesIO(resp.content))


def scrape_artists(chunk_size=30, cooldown=30):

    base_url = "https://www.artstation.com/api/v2/search/users.json"

    total_count = json.loads(get_file(f"{base_url}?query=&page=1&per_page=3"))[
        'total_count']
    print(f"Found {total_count} artists...")

    num_pages = total_count // chunk_size
    print(f"This is going to take {num_pages} API calls...")

    for i in tqdm(range(100)):
        url = f"{base_url}?query=&page={i+1}&per_page={chunk_size}&sorting=followers&pro_first=0"
        artists = json.loads(get_file(url))['data']
        for artist in artists:
            scrape_artist(artist['username'])
            sleep(cooldown)


def scrape_artist(artist=''):
    os.makedirs(f"artstation/{artist}/assets/", exist_ok=True)

    base_url = f"https://www.artstation.com/users/{artist}/projects.json?page="

    page_number = 1
    while True:

        print(f"Grabbing \"{artist}\" page {page_number}...")
        page = get_file(base_url+str(page_number))
        page_number += 1
        page_data = json.loads(page)['data']

        if page_data == []:
            break

        for j, project in enumerate(page_data):

            # with open('artstation_titles.txt', 'a', encoding='utf-8') as f:
            #     f.write(f"{project['title']}\n")

            project_url = f"https://www.artstation.com/projects/{project['hash_id']}.json"

            print(f"Grabbing \"{artist}\" project {page_number}.{j}...")
            project = get_file(project_url)
            project_data = json.loads(project)

            with open(f"artstation/{artist}/{project_data['hash_id']}.json", 'wb') as f:
                f.write(project)

            for i, asset in enumerate(project_data['assets']):

                try:
                    img = download_image(asset['image_url'])
                    extension = asset['image_url'].split('.')[-1].split('?')[0]
                    img.save(
                        f"artstation/{artist}/assets/{project_data['hash_id']}_{i}.{extension}")
                except Exception as e:
                    print(e)

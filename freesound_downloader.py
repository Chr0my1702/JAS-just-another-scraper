
import requests
import os

def return_full_url(username, id):
    return f"https://freesound.org/home/login/?next=/people/{username}/sounds/{id}/"


def login(username, password, url, csrf_token, csrf_middleware_token):
    cookies = {
        'csrftoken': str(csrf_token),
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Origin': 'https://freesound.org',
        'Referer': 'https://freesound.org/home/login/?next=/'
    }

    data = {
        'csrfmiddlewaretoken': str(csrf_middleware_token),
        'username': str(username),
        'password': str(password),
        'next': str(url.partition("next=")[2]+'download'),
    }
    response = requests.post(url, cookies=cookies, headers=headers, data=data)
    return response




download =  login("Username", "Password", return_full_url("sound_owner", 123), "csrf_token", "csf_middleware_token")
with open(str(os.path.basename(download.url).partition("filename=")[2]), "wb") as f:
    f.write(download.content) 
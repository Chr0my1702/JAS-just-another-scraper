import time
from selenium import webdriver
import chromedriver_autoinstaller
import pandas as pd


chromedriver_autoinstaller.install()
driver = webdriver.Chrome()
driver.get('https://openneuro.org/search')
time.sleep(5)
# initialize the dataframe
df = pd.DataFrame(columns=['title', 'access_string',
                  'size', 'age_range', 'type', 'tags'])
# set the access string as the index
df.set_index('access_string', inplace=True)

datanum = 0
pages = 0
while pages <= (1000):
    try:
        time.sleep(1)
        load_button = driver.find_element_by_xpath(
            '//*[@id="main"]/div[1]/div[1]/section/div/div/div[2]/div[2]/div/div[4]/div/button')
        time.sleep(1)
        for i in range(1, 2):
            try:
                load_button.click()
                time.sleep(1)
            except:
                pass
    except:
        break

data_list = driver.find_elements_by_xpath(
    "/html/body/div/div[1]/div[1]/section/div/div/div[2]/div[2]/div/div[3]/div")
for div in data_list:
    try:
        title_dataset = div.find_element_by_xpath("./div[1]/h3/a")
        title_dataset_text = title_dataset.text
    except:
        title_dataset_text = ""
    try:
        access_string = div.find_element_by_xpath("./div[4]/span[1]/span")
        access_string_text = access_string.text
    except:
        access_string_text = ""
    try:
        size_string = div.find_element_by_xpath("./div[4]/span[5]/span")
        size_string_text = size_string.text
    except:
        size_string_text = ""
    try:
        age_range = div.find_element_by_xpath("./div[4]/span[4]/span")
        age_range_text = age_range.text
    except:
        age_range_text = ""
    try:
        type_div = div.find_element_by_xpath("./div[3]/div[1]/div")
        type_div_text = type_div.text
    except:
        type_div_text = ""
    try:
        tags_div = div.find_elements_by_xpath("./div[3]/div[2]/div")
        tags = []
        for tag in tags_div:
            tags.append(tag.text)
    except:
        tags = ""

    # make a dictionary that stores all those variables
    data = {
        "title": title_dataset_text,
        "access": access_string_text,
        "size": size_string_text,
        "age_range": age_range_text,
        "type": type_div_text,
        "tags": ",njjnj".join(tags),
        "github_url": str("https://github.com/OpenNeuroDatasets/" + access_string_text),
        "openneuro_url": str("https://openneuro.org/datasets/" + access_string_text)
    }
    # add data to pd dataframe but make sure there are no duplicates by checking the access string

    if access_string_text not in df.index:
        df.loc[access_string_text] = data
        datanum += 1
        print(datanum)
    # if there are more then 711 datasets, break out of the loop
    if datanum == 712:
        # save the dataframe to a csv
        df.to_csv("openneuro_data.csv")
        break

# STILL IN A TESTING STATE

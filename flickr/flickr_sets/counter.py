
import pandas as pd
import os

import winsound
frequency = 2500  # Set Frequency To 2500 Hertz
duration = 1000  # Set Duration To 1000 ms == 1 second
winsound.Beep(frequency, duration)
# sum of the number of rows in each parquet file in the current directory


def sum_rows(path):
    sum = 0
    # set path to current directory
    path = os.chdir(path)
    # for every parquet file in the current directory

    for file in os.listdir(path):
        if file.endswith(".parquet"):
            df = pd.read_parquet(file)
            sum += df.shape[0]
    return sum


winsound.Beep(frequency, duration)

print(str(sum_rows(r"C:\Users\alexa\OneDrive\Desktop\just-another-scraper\flickr_sets")))
# make noise to indicate that the program has finished running
winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)

import wget
from random import sample
import os

with open("wet.paths") as fPath:
    paths = fPath.read().splitlines()

sampled_paths = sample(paths, 200)
root_url = "https://commoncrawl.s3.amazonaws.com/"

for p in sampled_paths:
    url = root_url + p
    print("Downloading {}".format(url))
    target_path = os.path.expanduser("~/Documents/WebSearchEngine/CrawledFiles/")
    wget.download(url, target_path)


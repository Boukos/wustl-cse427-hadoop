import os
import re
def processFile():
    dir = "/Users/wq/Desktop/hw_reviews_FL2016"
    resPath = dir+"/"+"all_reviews.txt"
    res = open(resPath, 'a')
    for folder in os.listdir(dir):
        if folder.find(".") != -1:
            continue
        print folder
        for review in os.listdir(dir+"/"+folder):
            id = review[2] if review[2].isdigit() else "0"
            path = dir+"/"+folder+"/"+review
            f = open(path)
            content = f.read()
            words = re.split('\W+', content)
            if len(words) > 50:
                label = folder if folder != "not_labled" else " "
                res.write(id + "\t" + label + "\t" + " ".join(words) + "\n")
            f.close()

processFile()
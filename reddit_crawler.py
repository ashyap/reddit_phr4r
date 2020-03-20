import pandas as pd 
import datetime
import csv
import os 
import requests 
import datetime as dt
import pytz
import time

class RedditCsvWriter:
    def __init__(self, filename):
        self.columns = ["id", "title", "selftext", "num_comments", 
                        "score", "created_utc"]
        self.crawled_ids = []
        if os.path.isfile(filename):
            print(f"{filename} exists, getting ids that have been crawled")
            self.file = open(filename, mode='a', encoding='utf-8')
            self.file_writer = csv.writer(self.file, delimiter=',')
            
            temp_df = pd.read_csv(filename)
            self.crawled_ids = temp_df["id"].values.tolist()
        else:
            print(f"{filename} does NOT exist, creating file")
            self.file = open(filename, mode='w', encoding='utf-8')
            self.file_writer = csv.writer(self.file, delimiter=',')
            self.file_writer.writerow(self.columns)
            
        
    def write_submissions(self, submissions):
        for submission in submissions["data"]:
            if submission["id"] not in self.crawled_ids: 
                self.file_writer.writerow([submission[col] if col in submission.keys() 
                                           else None
                                           for col in self.columns])
                self.crawled_ids.append(submission["id"])
        
    def __del__(self):
        self.file.close()
        
class SubRedditCrawler:
    def __init__(self, sub, start_date="2017-01-01", 
                 end_date="2020-01-01", sort_type="score",
                 sort="desc", filename="phr4r.csv",
                 fields=["id","title","selftext","score","num_comments","created_utc"]
                 ):
        self.sub = sub
        self.start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
        self.sort_type = sort_type
        self.sort = sort
        self.fields = fields
        self.reddit_writer = RedditCsvWriter(filename)
        self.url = "https://api.pushshift.io/reddit/submission/search/"
        
    def to_utc(self, date):
        return int(date.replace(tzinfo=dt.timezone.utc).timestamp())
    
    def to_readable_date(self, timestamp):
        return dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        
    def get_posts(self):
        date_range = (pd.date_range(self.start_date, 
                                    periods=(self.end_date - self.start_date).days)
                      .tolist())

        for i, s_date in enumerate(date_range):
            if i != len(date_range)-1:
                e_date = date_range[i+1]
                r = requests.get(url = self.url, params={
                    'after': self.to_utc(s_date),
                    'before': self.to_utc(e_date),
                    'sort_type': self.sort_type,
                    'sort': self.sort,
                    'subreddit': self.sub,
                    'fields': self.fields,
                    "size": 500
                })
                
                print(f"Doing {s_date.strftime('%Y-%m-%d')} to {e_date.strftime('%Y-%m-%d')}")
                if r.status_code == 200:
                    self.reddit_writer.write_submissions(r.json())
                    print("=====Done")
                else:
                    print("=====Skipped")
                time.sleep(1)

if __name__ == "__main__":
    reddit_crawler = SubRedditCrawler("phr4r", start_date="2017-01-01", end_date="2020-01-01")        
    reddit_crawler.get_posts()
import datetime
import os

import pandas as pd


class ETL:
    INPUT_PATH = "./StackOverflow"
    OUTPUT_PATH = "Output"

    def __init__(self, value_date: datetime.date):
        self.year = value_date.year
        self.month = value_date.month

        self.summary_table = dict()

    def extract(self):
        self.posts_df = pd.read_csv(
            f"{self.INPUT_PATH}/{self.year}/{self.month:02d}/posts.csv.gz",
            compression="gzip",
        )
        self.users_df = pd.read_csv(
            f"{self.INPUT_PATH}/{self.year}/{self.month:02d}/users.csv.gz",
            compression="gzip",
        )

    def transform(self):
        self.generate_summary_table()

    def load(self):
        pass

    def generate_summary_table(self):
        self.summary_table["Number of posts in that month"] = self.posts_df.shape[0]
        self.summary_table["Average number of comments per post"] = self.posts_df["comment_count"].mean()
        self.summary_table["Average number of comments per post"] = 0
        self.summary_table["Average number of comments per post"] = 0
        self.summary_table["Average number of comments per post"] = 0
        self.summary_table["Average number of comments per post"] = 0
        self.summary_table["Average number of comments per post"] = 0

    def run(self):
        self.extract()
        self.transform()
        self.load()
        return True

import datetime
import os
import itertools

import pandas as pd


class ETL:
    INPUT_PATH = "./StackOverflow"
    OUTPUT_PATH = "Output"
    DATETIME_FORMAT = "%Y-%m-%d"

    def __init__(self, value_date: datetime.date, start_date: datetime.date):
        """

        Args:
            value_date (datetime.date): Value date of analytics
            start_date (datetime.date): Start date of data (mainly used for getting all users' data)
        """

        self.year = value_date.year
        self.month = value_date.month

        self.available_data_date_list: list[datetime.date] = (
            pd.date_range(start_date, value_date, freq="M").to_pydatetime().tolist()
            if value_date != start_date
            else [value_date]
        )

        self.summary_table = dict()

    def run(self):
        self._extract()
        self._transform()
        self._load()
        return True

    def _extract(self):
        users_df_list = [
            pd.read_csv(
                f"{self.INPUT_PATH}/{date.year}/{date.month:02d}/users.csv.gz",
                compression="gzip",
            )
            for date in self.available_data_date_list
        ]

        self.users_df = pd.concat(users_df_list).reset_index(drop=True)
        self.posts_df = pd.read_csv(
            f"{self.INPUT_PATH}/{self.year}/{self.month:02d}/posts.csv.gz",
            compression="gzip",
        )

    def _transform(self):
        self.__pre_processing()
        self.__generate_summary_table()
        self.__generate_tag_table()

    def _load(self):
        print(self.summary_table)
        print(self.tags_df)

    def __pre_processing(self):
        """Use this function for processing necessary columns and data"""
        self.users_df = self.users_df.drop_duplicates(subset=["id"], keep="last")

        self.users_df["creation_datetime"] = (
            self.users_df["creation_date"]
            .str.split(" ")
            .apply(lambda arr: datetime.datetime.strptime(arr[0], self.DATETIME_FORMAT))
        )

        self.posts_df["creation_datetime"] = (
            self.posts_df["creation_date"]
            .str.split(" ")
            .apply(lambda arr: datetime.datetime.strptime(arr[0], self.DATETIME_FORMAT))
        )

        self.posts_df["creation_datetime_str"] = self.posts_df[
            "creation_datetime"
        ].dt.strftime("%Y-%m-%d")

        self.posts_df["tags_in_list"] = self.posts_df["tags"].str.split("|")

        self.exploded_posts_df = (
            self.posts_df.dropna(subset="tags")
            .explode("tags_in_list")
            .rename(columns={"tags_in_list": "tag"})
        )
        self.exploded_posts_df = self.exploded_posts_df.merge(
            self.users_df, how="left", left_on="owner_user_id", right_on="id"
        )
        self.exploded_posts_df["count"] = 1

        # Calculate the number of days in the month
        num_days = (
            datetime.datetime(self.year, self.month + 1, 1)
            - datetime.datetime(self.year, self.month, 1)
        ).days

        # Generate the list of dates
        self.dates = [
            (
                datetime.datetime(self.year, self.month, 1) + datetime.timedelta(days=i)
            ).strftime(self.DATETIME_FORMAT)
            for i in range(num_days)
        ]

    def __generate_summary_table(self):
        self.summary_table["Number of posts in that month"] = self.posts_df.shape[0]

        self.summary_table["Average number of comments per post"] = self.posts_df[
            "comment_count"
        ].mean()

        self.summary_table["Number of new users created in that month"] = len(
            self.users_df[
                (self.users_df["creation_datetime"].dt.year == self.year)
                & (self.users_df["creation_datetime"].dt.month == self.month)
            ]
        )

        self.summary_table["Total number of active tags in that month"] = len(
            self.exploded_posts_df["tag"].unique().tolist()
        )

        self.summary_table["Average number of posts per user"] = (
            self.posts_df.groupby(by="owner_user_id").size().mean()
        )

        self.summary_table["Average reputation per user"] = self.users_df[
            "reputation"
        ].mean()

        histogram = self.posts_df.groupby(by="creation_datetime_str").size().to_dict()
        missing_dates = list(set(histogram.keys()) - set(self.dates))
        for missing_date in missing_dates:
            histogram[missing_date] = 0
        self.summary_table["A histogram with the number of posts for each day"] = dict(
            sorted(histogram.items())
        )

    def __generate_tag_table(self):
        self.tags_df = (
            self.exploded_posts_df.groupby(by="tag")
            .agg({"count": "sum", "reputation": "mean", "score": "mean"})
            .reset_index()
        )
        self.tags_df = self.tags_df.rename(
            columns={
                "count": "posts",
                "reputation": "avg_reputation",
                "score": "avg_score",
            }
        )

        self.tags_df = self.tags_df.merge(
            self.__get_tag_hero_df(), how="left", left_on="tag", right_on="tag"
        )

        self.tags_df["month"] = self.month

        self.tags_df["year"] = self.year

    def __get_tag_hero_df(self):
        count_tag_id = self.exploded_posts_df.groupby(["tag", "id"]).size()
        count_tag_id_df = count_tag_id.to_frame(name="count").reset_index()
        count_tag_id_df = count_tag_id_df.groupby(["tag"]).max().reset_index()
        count_tag_id_df = count_tag_id_df.merge(
            self.users_df[["id", "display_name"]],
            how="left",
            left_on="id",
            right_on="id",
        )
        count_tag_id_df = count_tag_id_df.rename(columns={"display_name": "tag_hero"})
        return count_tag_id_df[["tag", "tag_hero"]]

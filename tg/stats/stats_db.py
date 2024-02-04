import datetime as dt

import pandas as pd
import supabase

MSGS_DF_COLS = [
    "username",
    "link",
    "reach",
    "likes",
    "replies",
    "forwards",
    "datetime",
    "text",
]


class StatsDatabase:
    """Loads the channel list and the full statistics dataframe from the Supabase database.
    Calculates the last statictics dataframe and timedelta since the last statictics update.
    Saves new statictics to the database."""

    def __init__(
        self, client: supabase.Client, channels_table, stats_table, msgs_table
    ):
        self.client = client
        self.channels_table = channels_table
        self.stats_table = stats_table
        self.msgs_table = msgs_table

    def load_data(self) -> None:
        """Loads the channel list and the full statistics dataframe from the Supabase database."""
        self.load_channel_list()
        self.load_stats_dataframe()
        self.load_msgs_dataframe()
        self.calc_last_stats_dataframe()
        self.calc_timedelta_since_last_stats_update()

    def load_channel_list(self):
        """Returns the list of channels from the database."""
        list_of_dicts = (
            self.client.table(self.channels_table).select("username").execute().data
        )
        self.channels = {item["username"] for item in list_of_dicts}

    def load_stats_dataframe(self):
        """Returns the full statistics dataframe from the database."""
        self.stats_df = pd.DataFrame(
            self.client.table(self.stats_table)
            .select("*")
            .order("created_at", desc=True)
            .execute()
            .data
        )
        if self.stats_df.empty:
            self.stats_df = pd.DataFrame(
                columns=["created_at", "username", "reach", "subscribers"]
            )
            return
        self.stats_df["created_at"] = to_msk(self.stats_df["created_at"])

    def load_msgs_dataframe(self):
        self.msgs_df = pd.DataFrame(
            self.client.table(self.msgs_table)
            .select("*")
            .order("datetime", desc=True)
            .execute()
            .data
        )
        if self.msgs_df.empty:
            self.msgs_df = pd.DataFrame(columns=MSGS_DF_COLS)
            return
        self.msgs_df["datetime"] = to_msk(self.msgs_df["datetime"])

    def calc_last_stats_dataframe(self):
        """Calculates the last statictics dataframe from the database."""
        self.max_datetime = (
            dt.datetime(1980, 1, 1)
            if self.stats_df.empty
            else self.stats_df.created_at.max()
        )
        self.last_stats_df = self.stats_df[
            self.stats_df.created_at == self.max_datetime
        ].copy()

    def calc_timedelta_since_last_stats_update(self):
        """Calculates the timedelta since the last statictics update."""
        if self.stats_df.empty:
            self.delta = dt.timedelta(days=365)
        else:
            self.delta = dt.datetime.now(dt.timezone.utc) - self.max_datetime

    def save_new_stats_to_db(self, stats_df: pd.DataFrame):
        """Saves the new statictics dataframe to the database."""
        data = stats_df[["username", "reach", "subscribers"]].to_dict("records")
        self.client.table(self.stats_table).insert(data).execute()

    def save_msgs(self, msgs_df: pd.DataFrame):
        """Updates recent messages and their stats in the messages table."""

        # convert datetime to str so it can be saved to postgres
        datetime_old = msgs_df["datetime"]
        msgs_df["datetime"] = msgs_df["datetime"].astype("str")

        data = msgs_df[MSGS_DF_COLS].to_dict("records")

        table = self.client.table(self.msgs_table)
        table.delete().neq("username", "anyone").execute()  # deletes all rows
        table.insert(data).execute()

        msgs_df["datetime"] = datetime_old


def to_msk(col):
    return pd.to_datetime(col, utc=True).dt.tz_convert("Europe/Moscow")

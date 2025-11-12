import asyncio
import datetime as dt
from collections import namedtuple
from itertools import chain
from typing import AsyncIterator
from icontract import require

import pandas as pd

from ..account import Scanner
from .stats_db import StatsDatabase

import logging

logger = logging.getLogger(__name__)

Msg = namedtuple(
    "Message", "username link reach likes replies forwards datetime text full_text"
)
Channel = namedtuple("Channel", "username subscribers")


class StatsCollector:
    """
    Collects and analyzes statistics from Telegram channels and messages.

    This class handles the collection of message statistics (views, likes, replies, forwards)
    and channel statistics (subscriber counts) from Telegram channels. It can process
    channels sequentially (with progress bar) or in parallel.

    Args:
        scanner (Scanner): Scanner instance for accessing Telegram data.
        min_date (datetime.datetime, optional): Minimum date for collecting messages.
        depth (int, optional): Number of days to look back from now. Cannot be used with min_date.

    Attributes:
        scanner (Scanner): The scanner instance used for data collection.
        min_date (datetime.datetime): The minimum date for message collection.
        msgs_df (pd.DataFrame): DataFrame containing message statistics.
        channels_df (pd.DataFrame): DataFrame containing channel statistics.
        stats (pd.DataFrame): Combined statistics DataFrame.

    Methods:
        collect_all_stats: Main method to collect all statistics for given channels.
        collect_msg_stats: Collect message-level statistics for a channel.
        collect_channel_stats: Collect channel-level statistics.
        calc_msg_popularity: Calculate message popularity metrics.
        collect_and_save: Collect statistics and save to database.
    """

    scanner: Scanner

    @require(lambda min_date: isinstance(min_date, dt.datetime) or min_date is None)
    def __init__(self, scanner, /, min_date=None, depth=None):
        self.scanner = scanner
        self.min_date = min_date
        if depth and min_date:
            raise ValueError("Can't set both depth and min_date")
        if depth:
            self.min_date = (
                dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=depth)
            ).replace(tzinfo=None)

    async def collect_all_stats(self, channels, pbar=None):
        """
        Collect statistics for all specified channels.

        This is the main method for collecting both message and channel statistics.
        Uses sequential scanning when progress bar is provided, parallel scanning otherwise.

        Args:
            channels (list): List of channel usernames to collect statistics for.
            pbar: Optional progress bar for sequential scanning.
        """
        async with self.scanner.session(pbar):
            if pbar:
                msg_stats, channel_stats = await self.sequential_scan(channels, pbar)

            else:
                msg_stats, channel_stats = await self.parallel_scan(channels)

        self.msgs_df = pd.DataFrame(msg_stats)
        self.channels_df = pd.DataFrame(channel_stats)

        # Handle NaN values in msgs_df before processing
        if not self.msgs_df.empty:
            # Fill NaN values with appropriate defaults for JSON serialization
            self.msgs_df = self.msgs_df.fillna(
                {
                    "reach": 0,
                    "likes": 0,
                    "replies": 0,
                    "forwards": 0,
                    "text": "",
                    "full_text": "",
                }
            )
            # Convert numeric columns to int to avoid "0.0" strings in JSON
            numeric_columns = ["reach", "likes", "replies", "forwards"]
            for col in numeric_columns:
                if col in self.msgs_df.columns:
                    self.msgs_df[col] = self.msgs_df[col].astype(int)

        self.calc_msg_popularity()
        self.collect_stats_to_single_df()

    async def sequential_scan(self, channels, pbar):
        """
        Scan channels sequentially with progress bar updates.

        Args:
            channels (list): List of channel usernames to scan.
            pbar: Progress bar object to update.

        Returns:
            tuple: (msg_stats, channel_stats) where msg_stats is a list of message statistics
                   and channel_stats is a list of channel statistics.
        """
        msg_stats = []
        channel_stats = []

        for channel in channels:
            pbar.set_postfix_str(channel)

            async for msg in self.collect_msg_stats(channel):
                msg_stats.append(msg)
            channel_stats.append(await self.collect_channel_stats(channel))

            pbar.update()

        return msg_stats, channel_stats

    async def parallel_scan(self, channels):
        """
        Scan channels in parallel for faster processing.

        Args:
            channels (list): List of channel usernames to scan.

        Returns:
            tuple: (msg_stats, channel_stats) where msg_stats is an iterable of message statistics
                   and channel_stats is a list of channel statistics.
        """
        async def collect_list(channel):
            return [msg async for msg in self.collect_msg_stats(channel)]

        msg_stats = chain.from_iterable(
            await asyncio.gather(*[collect_list(c) for c in channels])
        )
        channel_stats = await asyncio.gather(
            *[self.collect_channel_stats(c) for c in channels]
        )

        return msg_stats, channel_stats

    async def collect_msg_stats(self, channel) -> AsyncIterator[Msg]:
        """
        Collect message-level statistics for a specific channel.

        Gathers views, likes, forwards, and reply counts for each message
        in the channel since the minimum date.

        Args:
            channel (str): Channel username to collect statistics for.

        Yields:
            Msg: Named tuple containing message statistics.
        """
        logger.info(f"Starting to collect message stats for channel: {channel}")
        msgs_dict = {}
        message_count = 0

        async for msg in self.scanner.get_chat_history(channel, min_date=self.min_date):
            message_count += 1
            logger.debug(
                f"Processing message {message_count} (ID: {msg.id}) from channel {channel}"
            )

            # Telethon reactions structure
            likes = 0
            if hasattr(msg, "reactions") and msg.reactions:
                if hasattr(msg.reactions, "results"):
                    likes = sum(result.count for result in msg.reactions.results)
                elif hasattr(msg.reactions, "reactions"):
                    likes = sum(reaction.count for reaction in msg.reactions.reactions)

            # Telethon message text/caption
            full_text = msg.message or msg.raw_text or ""

            # Telethon message link format
            # Remove @ symbol from channel name for proper URL format
            channel_name = channel.lstrip("@") if channel.startswith("@") else channel
            link = f"https://t.me/{channel_name}/{msg.id}" if hasattr(msg, "id") else ""

            msgs_dict[msg.id] = Msg(
                username=channel,
                link=link,
                reach=getattr(msg, "views", 0) or 0,
                likes=likes,
                replies=0,
                forwards=getattr(msg, "forwards", 0) or 0,
                datetime=msg.date,
                text=shorten(full_text),
                full_text=full_text,
            )

        logger.info(
            f"Collected {message_count} messages from channel {channel}, now getting replies counts"
        )

        async def add_replies(msg_id, msg: Msg) -> Msg:
            logger.debug(
                f"Getting replies count for message {msg_id} in channel {channel} (link: {msg.link})"
            )
            try:
                replies = await self.scanner.get_discussion_replies_count(
                    channel, msg_id
                )
                logger.debug(
                    f"Message {msg_id} in channel {channel} has {replies} replies"
                )
                return msg._replace(replies=replies)
            except Exception as e:
                logger.error(
                    f"Failed to get replies count for message {msg_id} in channel {channel}. "
                    f"Message details: link={msg.link}, datetime={msg.datetime}, text_preview='{msg.text[:50]}...'. "
                    f"Error: {e}"
                )
                return msg._replace(replies=0)

        tasks = [
            asyncio.create_task(add_replies(msg_id, msg))
            for msg_id, msg in msgs_dict.items()
        ]

        logger.info(
            f"Created {len(tasks)} tasks to get replies counts for channel {channel}"
        )
        completed_count = 0

        for completed in asyncio.as_completed(tasks):
            completed_count += 1
            result = await completed
            logger.debug(
                f"Completed replies task {completed_count}/{len(tasks)} for channel {channel}"
            )
            yield result

        logger.info(
            f"Completed all {completed_count} reply collection tasks for channel {channel}"
        )

    async def collect_channel_stats(self, channel) -> Channel:
        """
        Collect channel-level statistics for a specific channel.

        Args:
            channel (str): Channel username to collect statistics for.

        Returns:
            Channel: Named tuple containing channel username and subscriber count.
        """
        # Telethon chat members count
        members_count = await self.scanner.get_chat_members_count(channel)

        return Channel(username=channel, subscribers=members_count)

    def calc_msg_popularity(self):
        """
        Calculate message popularity metric.

        Popularity is calculated as (likes + replies + forwards) / views.
        This gives a normalized measure of engagement per view.
        """
        self.msgs_df["popularity"] = (
            self.msgs_df.likes + self.msgs_df.replies + self.msgs_df.forwards
        ) / self.msgs_df.reach

    def collect_stats_to_single_df(self):
        """
        Combine message and channel statistics into a single DataFrame.

        Merges channel subscriber data with average message reach per channel
        to create a comprehensive statistics DataFrame.
        """
        # Check if msgs_df is valid and contains 'username'
        if (
            getattr(self, "msgs_df", None) is not None
            and not self.msgs_df.empty
            and "username" in self.msgs_df.columns
        ):
            # Compute mean reach per channel and fill NaNs with 0, cast to int
            reach_by_channel = (
                self.msgs_df.groupby("username", as_index=False, sort=False)["reach"]
                .mean()
                .fillna(0)
                .astype({"reach": int})
            )
        else:
            reach_by_channel = pd.DataFrame({"username": [], "reach": []})

        # Efficient merge to include all channels, fill missing reach with 0 and cast to int
        self.stats = pd.merge(
            self.channels_df, reach_by_channel, on="username", how="left"
        )
        self.stats["reach"] = self.stats["reach"].fillna(0).astype(int)
        assert not self.stats.isna().any().any(), "self.stats contains NaN values"

    async def collect_and_save(self, stats_db: StatsDatabase, pbar=None):
        """
        Collect statistics and save them to the database.

        Args:
            stats_db (StatsDatabase): Database instance to save statistics to.
            pbar: Optional progress bar for collection process.
        """
        await self.collect_all_stats(stats_db.channels, pbar)
        stats_db.save_new_stats_to_db(self.stats)
        stats_db.save_msgs(self.msgs_df)


def shorten(text: str, max_length=200):
    return (
        text.encode("utf-8").decode("utf-8")[:max_length] + "..."
        if isinstance(text, str) and len(text) > max_length
        else text
    )

import asyncio
import datetime as dt
from collections import namedtuple
from itertools import chain
from typing import AsyncIterator
from icontract import require

import pandas as pd

from ..account import Scanner
from .stats_db import StatsDatabase

Msg = namedtuple(
    "Message", "username link reach likes replies forwards datetime text full_text"
)
Channel = namedtuple("Channel", "username subscribers")


class StatsCollector:
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
        async with self.scanner.session(pbar):
            if pbar:
                msg_stats, channel_stats = await self.sequential_scan(channels, pbar)

            else:
                msg_stats, channel_stats = await self.parallel_scan(channels)

        self.msgs_df = pd.DataFrame(msg_stats)
        self.channels_df = pd.DataFrame(channel_stats)

        self.calc_msg_popularity()
        self.collect_stats_to_single_df()

    async def sequential_scan(self, channels, pbar):
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
        msgs_dict = {}

        async for msg in self.scanner.get_chat_history(channel, min_date=self.min_date):
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
            link = f"https://t.me/{channel}/{msg.id}" if hasattr(msg, "id") else ""

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

        async def add_replies(msg_id, msg: Msg) -> Msg:
            replies = await self.scanner.get_discussion_replies_count(channel, msg_id)
            return msg._replace(replies=replies)

        tasks = [
            asyncio.create_task(add_replies(msg_id, msg))
            for msg_id, msg in msgs_dict.items()
        ]

        for completed in asyncio.as_completed(tasks):
            yield await completed

    async def collect_channel_stats(self, channel) -> Channel:
        # Telethon chat members count
        members_count = await self.scanner.get_chat_members_count(channel)

        return Channel(username=channel, subscribers=members_count)

    def calc_msg_popularity(self):
        self.msgs_df["popularity"] = (
            self.msgs_df.likes + self.msgs_df.replies + self.msgs_df.forwards
        ) / self.msgs_df.reach

    def collect_stats_to_single_df(self):
        # Compute mean reach per channel (may be empty if there are no messages)
        if (
            hasattr(self, "msgs_df")
            and not self.msgs_df.empty
            and "username" in self.msgs_df.columns
        ):
            reach_by_channel = self.msgs_df.groupby("username", as_index=False)[
                "reach"
            ].mean()
            # Round up reach values
            reach_by_channel["reach"] = reach_by_channel["reach"].apply(
                lambda x: int(x) if pd.notna(x) else x
            )
        else:
            reach_by_channel = pd.DataFrame(columns=["username", "reach"])

        # Merge with channels to include channels with no messages (reach = NaN)
        self.stats = pd.merge(
            self.channels_df, reach_by_channel, on="username", how="left"
        )

    async def collect_and_save(self, stats_db: StatsDatabase, pbar=None):
        await self.collect_all_stats(stats_db.channels, pbar)
        stats_db.save_new_stats_to_db(self.stats)
        stats_db.save_msgs(self.msgs_df)


def shorten(text: str, max_length=200):
    return (
        text.encode("utf-8").decode("utf-8")[:max_length] + "..."
        if isinstance(text, str) and len(text) > max_length
        else text
    )

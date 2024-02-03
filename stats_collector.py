import asyncio
import datetime as dt
from collections import namedtuple
from itertools import chain

import pandas as pd

from .scanner import Scanner

Msg = namedtuple("Message", "username link reach likes replies forwards datetime text")
Channel = namedtuple("Channel", "username subscribers")


class StatsCollector:
    scanner: Scanner

    def __init__(self, scanner, min_date=None, depth=None):
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

            msg_stats.extend(await self.collect_msg_stats(channel))
            channel_stats.append(await self.collect_channel_stats(channel))

            pbar.update()

        return msg_stats, channel_stats

    async def parallel_scan(self, channels):
        msg_stats = chain.from_iterable(
            await asyncio.gather(*[self.collect_msg_stats(c) for c in channels])
        )
        channel_stats = await asyncio.gather(
            *[self.collect_channel_stats(c) for c in channels]
        )

        return msg_stats, channel_stats

    async def collect_msg_stats(self, channel) -> list[Msg]:
        msgs_dict = {}

        async for msg in self.scanner.get_chat_history(channel, min_date=self.min_date):
            likes = (
                sum(reaction.count for reaction in msg.reactions.reactions)
                if msg.reactions
                else 0
            )

            msgs_dict[msg.id] = Msg(
                username=channel,
                link=msg.link,
                reach=msg.views or 0,
                likes=likes,
                replies=0,
                forwards=msg.forwards or 0,
                datetime=msg.date,
                text=shorten(msg.text or msg.caption),
            )

        async def add_replies(msg_id, msg: Msg) -> Msg:
            replies = await self.scanner.get_discussion_replies_count(channel, msg_id)
            return msg._replace(replies=replies)

        return await asyncio.gather(
            *[add_replies(msg_id, msg) for msg_id, msg in msgs_dict.items()]
        )

    async def collect_channel_stats(self, channel) -> Channel:
        chat = await self.scanner.get_chat(channel)

        return Channel(username=channel, subscribers=chat.members_count)

    def calc_msg_popularity(self):
        self.msgs_df["popularity"] = (
            self.msgs_df.likes + self.msgs_df.replies + self.msgs_df.forwards
        ) / self.msgs_df.reach

    def collect_stats_to_single_df(self):
        self.stats = self.msgs_df.groupby("username").agg({"reach": "mean"}).astype(int)
        self.stats["subscribers"] = self.channels_df.set_index("username")[
            "subscribers"
        ]
        self.stats.reset_index(inplace=True)


def shorten(text: str, max_length=200):
    return (
        text.encode("utf-8").decode("utf-8")[:max_length] + "..."
        if isinstance(text, str) and len(text) > max_length
        else text
    )

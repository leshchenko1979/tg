import asyncio
import contextlib
import datetime as dt
import logging
from typing import AsyncIterable, Any

from icontract import ensure, require
from telethon import functions, types
from telethon.errors.rpcerrorlist import FloodWaitError, MsgIdInvalidError

from ..chat_cache import ChatCache, ChatCacheItem
from . import Account, AccountCollection
from ..utils import AbstractFileSystemProtocol, TQDMProtocol

MAX_ACC_WAITING_TIME = 1000  # max waiting time for an available account

logger = logging.getLogger(__name__)


class Scanner(AccountCollection):
    """Выполняет запросы к телеграму, используя коллекцию аккаунтов."""

    def __init__(
        self,
        /,
        fs: AbstractFileSystemProtocol,
        phones: list[str] = None,
        chat_cache=True,
    ):
        """
        Initializes a Scanner instance.

        The Scanner handles scanning Telegram data and caching chat info.

        Args:
            fs: The file system to use for reading/writing session data.
            phones: List of phone numbers of Telegram accounts
                to be used for scanning.
            chat_cache: Whether to use chat caching.

        """
        new_phones = phones or [
            item.split(".session")[0] for item in fs.glob("*.session")
        ]
        super().__init__(
            accounts={phone: Account(fs, phone) for phone in new_phones},
            fs=fs,
            invalid="ignore",
        )
        self.phones = new_phones

        if chat_cache:
            self.chat_cache = ChatCache(fs)
            self.chat_cache.load()
        else:
            self.chat_cache = None

        self.pbar = None

    @ensure(
        lambda self: self.available_accs.qsize() > 0,
        "No accounts available after start",
    )
    async def start_sessions(self):
        await super().start_sessions()

        self.available_accs = asyncio.Queue()

        for acc in self.accounts.values():
            if acc.started:
                self.available_accs.put_nowait(acc)

    async def close_sessions(self):
        await super().close_sessions()
        self.available_accs = asyncio.Queue()

    @contextlib.asynccontextmanager
    async def session(self, pbar: TQDMProtocol = None):
        try:
            self.pbar = pbar
            async with super().session():
                yield
        finally:
            if self.chat_cache:
                self.chat_cache.save()
            self.pbar = None

    async def get_chat(self, chat_id) -> Any:
        if not self.chat_cache:
            async with self.get_acc() as acc:
                return await acc.app.get_entity(chat_id)

        if chat_id not in self.chat_cache:
            async with self.get_acc() as acc:
                chat = await acc.app.get_entity(chat_id)
            self.chat_cache[chat_id] = ChatCacheItem(chat)

        return self.chat_cache[chat_id].chat

    async def get_chat_members_count(self, chat_id) -> int:
        async def fetch_count(app, entity):
            # Check for channel types
            if isinstance(
                entity, (types.InputPeerChannel, types.Channel, types.InputChannel)
            ):
                req = functions.channels.GetFullChannelRequest(channel=entity)
            # Check for normal chat types
            elif isinstance(entity, (types.InputPeerChat, types.Chat)):
                req = functions.messages.GetFullChatRequest(chat_id=entity.id)
            else:
                return 0

            full = await app(req)
            return getattr(full.full_chat, "participants_count", 0)

        async def get_count():
            async with self.get_acc() as acc:
                entity = await acc.app.get_entity(chat_id)
                return await fetch_count(acc.app, entity)

        if not self.chat_cache:
            return await get_count()

        chat_cache_item = self.chat_cache[chat_id]
        if not chat_cache_item.members_count:
            chat_cache_item.members_count = await get_count()
        return chat_cache_item.members_count

    async def get_discussion_replies_count(self, chat_id, msg_id) -> int:
        try:
            async with self.get_acc() as acc:
                entity = await acc.app.get_entity(chat_id)
                # Use iter_messages with reply_to to get discussion replies
                count = 0
                async for _ in acc.app.iter_messages(
                    entity, reply_to=msg_id, limit=1000
                ):
                    count += 1
                return count
        except MsgIdInvalidError:
            return 0

    @require(lambda min_date: isinstance(min_date, dt.datetime) or min_date is None)
    async def get_chat_history(
        self, chat_id, limit=None, min_date=None
    ) -> AsyncIterable[Any]:
        async with self.get_acc() as acc:
            entity = await acc.app.get_entity(chat_id)
            count = 0
            async for msg in acc.app.iter_messages(entity, limit=limit):
                # Telethon Message.date is timezone-aware UTC; normalize
                msg_date = (
                    msg.date.replace(tzinfo=None) if msg.date.tzinfo else msg.date
                )
                if min_date and msg_date < min_date:
                    break
                yield msg
                count += 1
                if limit and count >= limit:
                    break

    async def get_discussion_replies(
        self, chat_id, msg_id, limit=None
    ) -> AsyncIterable[Any]:
        try:
            async with self.get_acc() as acc:
                entity = await acc.app.get_entity(chat_id)
                # Use iter_messages with reply_to to get discussion replies
                async for msg in acc.app.iter_messages(
                    entity, reply_to=msg_id, limit=limit
                ):
                    yield msg
        except MsgIdInvalidError:
            return

    # removed generic pyrogram command wrappers; each method uses Telethon directly

    @contextlib.asynccontextmanager
    async def get_acc(self):  # sourcery skip: raise-from-previous-error
        min_wait = self.min_wait()
        if min_wait and min_wait > MAX_ACC_WAITING_TIME:
            available_at = dt.datetime.now() + dt.timedelta(seconds=min_wait)
            raise RuntimeError(
                f"All accounts unavailable. First available at {available_at}."
            )

        try:
            acc: Account = await asyncio.wait_for(
                self.available_accs.get(), timeout=MAX_ACC_WAITING_TIME
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                "All accounts unavailable. "
                f"Max waiting time of {MAX_ACC_WAITING_TIME} secs exceeded."
            )

        try:
            yield acc
            self.available_accs.put_nowait(acc)

        except FloodWaitError as e:
            timeout_seconds = getattr(e, "seconds", None)
            if timeout_seconds is None and hasattr(e, "retry_after"):
                timeout_seconds = e.retry_after
            if timeout_seconds is None:
                timeout_seconds = 60
            asyncio.create_task(self.flood_wait(acc, timeout_seconds))

        except Exception:
            self.available_accs.put_nowait(acc)
            raise

    def min_wait(self):
        return min(
            (
                acc.flood_wait_timeout
                - (dt.datetime.now() - acc.flood_wait_from).seconds
                for acc in self.accounts.values()
                if acc.flood_wait_from
            ),
            default=None,
        )

    async def flood_wait(self, acc: Account, timeout: int):
        acc.flood_wait_from = dt.datetime.now()
        acc.flood_wait_timeout = timeout

        if self.pbar:
            old_postfix = self.pbar.postfix or ""
            self.pbar.set_postfix_str(
                ", ".join([old_postfix, f"{acc}: flood_wait {timeout} secs"])
            )
        else:
            logger.info("%s: flood_wait %s secs", acc, timeout)

        await asyncio.sleep(timeout)

        if self.pbar:
            self.pbar.set_postfix_str(old_postfix)

        self.available_accs.put_nowait(acc)

        acc.flood_wait_from = None
        acc.flood_wait_timeout = 0

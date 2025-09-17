import asyncio
import contextlib
import datetime as dt
import logging
from typing import AsyncIterable, Any

from icontract import ensure, require
from telethon import functions, types
from telethon.errors.rpcerrorlist import (
    FloodWaitError,
    MsgIdInvalidError,
    MessageIdInvalidError,
    PeerIdInvalidError,
)

from ..chat_cache import ChatCache, ChatCacheItem
from . import Account, AccountCollection
from ..utils import AbstractFileSystemProtocol, TQDMProtocol

MAX_ACC_WAITING_TIME = 60 * 5  # max waiting time for an available account

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
        started_count = 0

        for acc in self.accounts.values():
            if acc.started:
                self.available_accs.put_nowait(acc)
                started_count += 1
                logger.info(f"Account {acc.phone} started and added to available queue")
            else:
                logger.warning(f"Account {acc.phone} failed to start")

        logger.info(f"Started {started_count}/{len(self.accounts)} accounts")

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
        except (MsgIdInvalidError, MessageIdInvalidError, PeerIdInvalidError):
            logger.debug(
                f"Invalid message ID {msg_id} for chat {chat_id}, returning 0 replies"
            )
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
        except (MsgIdInvalidError, MessageIdInvalidError, PeerIdInvalidError):
            logger.debug(
                f"Invalid message ID {msg_id} for chat {chat_id}, returning empty replies"
            )
            return

    # removed generic pyrogram command wrappers; each method uses Telethon directly

    @contextlib.asynccontextmanager
    async def get_acc(self):  # sourcery skip: raise-from-previous-error
        logger.debug(
            f"Requesting account, timeout: {MAX_ACC_WAITING_TIME}s. Available queue size: {self.available_accs.qsize()}"
        )

        try:
            acc: Account = await asyncio.wait_for(
                self.available_accs.get(), timeout=MAX_ACC_WAITING_TIME
            )
            logger.debug(
                f"Got account: {acc.phone}. Accounts in queue: {self.available_accs.qsize()}"
            )
        except asyncio.TimeoutError:
            # Only check flood_wait after timing out waiting for accounts
            logger.debug(
                f"Timeout waiting for account. Queue size: {self.available_accs.qsize()}"
            )
            min_wait = self.min_wait()
            if min_wait and min_wait > 0:
                available_at = dt.datetime.now() + dt.timedelta(seconds=min_wait)
                logger.error(
                    f"All accounts unavailable. First available at {available_at}. Min wait: {min_wait}"
                )
                raise RuntimeError(
                    f"All accounts unavailable. First available at {available_at}."
                )
            else:
                logger.error(
                    f"Timeout waiting for account after {MAX_ACC_WAITING_TIME}s. No accounts in flood_wait."
                )
                raise RuntimeError(
                    f"All accounts unavailable. Max waiting time of {MAX_ACC_WAITING_TIME} secs exceeded."
                )

        try:
            yield acc
            logger.debug(f"Returning account {acc.phone} to queue")
            self.available_accs.put_nowait(acc)

        except FloodWaitError as e:
            timeout_seconds = getattr(e, "seconds", None)
            if timeout_seconds is None and hasattr(e, "retry_after"):
                timeout_seconds = e.retry_after
            if timeout_seconds is None:
                timeout_seconds = 60
            logger.warning(f"FloodWaitError for {acc.phone}: {timeout_seconds}s")
            asyncio.create_task(self.flood_wait(acc, timeout_seconds))

        except Exception as e:
            logger.error(f"Exception for account {acc.phone}: {e}")
            self.available_accs.put_nowait(acc)
            raise

    def min_wait(self):
        flood_wait_accounts = []
        for acc in self.accounts.values():
            if acc.flood_wait_from:
                elapsed = (dt.datetime.now() - acc.flood_wait_from).total_seconds()
                remaining = acc.flood_wait_timeout - elapsed
                flood_wait_accounts.append(
                    (acc.phone, remaining, acc.flood_wait_timeout, elapsed)
                )
                logger.debug(
                    f"Account {acc.phone}: timeout={acc.flood_wait_timeout}s, elapsed={elapsed:.1f}s, remaining={remaining:.1f}s"
                )

        if flood_wait_accounts:
            min_remaining = min(remaining for _, remaining, _, _ in flood_wait_accounts)
            logger.info(
                f"Flood wait accounts: {len(flood_wait_accounts)}, min remaining: {min_remaining:.1f}s"
            )
            for phone, remaining, timeout, elapsed in flood_wait_accounts:
                logger.info(
                    f"  {phone}: {remaining:.1f}s remaining (timeout: {timeout}s, elapsed: {elapsed:.1f}s)"
                )
            return min_remaining
        else:
            logger.debug("No accounts in flood_wait state")
            return None

    async def flood_wait(self, acc: Account, timeout: int):
        acc.flood_wait_from = dt.datetime.now()
        acc.flood_wait_timeout = timeout

        logger.info(f"Account {acc.phone} entering flood_wait for {timeout} seconds")

        if self.pbar:
            old_postfix = self.pbar.postfix or ""
            self.pbar.set_postfix_str(
                ", ".join([old_postfix, f"{acc}: flood_wait {timeout} secs"])
            )
        else:
            logger.info("%s: flood_wait %s secs", acc, timeout)

        await asyncio.sleep(timeout)

        logger.info(
            f"Account {acc.phone} flood_wait completed, returning to available queue"
        )

        if self.pbar:
            self.pbar.set_postfix_str(old_postfix)

        self.available_accs.put_nowait(acc)

        acc.flood_wait_from = None
        acc.flood_wait_timeout = 0

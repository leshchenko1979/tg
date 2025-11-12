import contextlib
import datetime as dt
import logging
from typing import AsyncIterable, Any

from icontract import ensure, require
from telethon import functions, types
from telethon.errors.rpcerrorlist import (
    MsgIdInvalidError,
    MessageIdInvalidError,
    PeerIdInvalidError,
)

from ..chat_cache import ChatCache, ChatCacheItem
from .account import Account
from .collection import AccountCollection
from ..utils import AbstractFileSystemProtocol, TQDMProtocol


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

    @ensure(
        lambda self: self.available_accs.qsize() > 0,
        "No accounts available after start",
    )
    async def start_sessions(self):
        """Start sessions and ensure accounts are available."""
        await super().start_sessions()

    @contextlib.asynccontextmanager
    async def session(self, pbar: TQDMProtocol = None):
        try:
            async with super().session(pbar):
                yield
        finally:
            if self.chat_cache:
                self.chat_cache.save()

    async def get_chat(self, chat_id) -> Any:
        """
        Get chat entity, using cache if available.

        Retrieves chat information from Telegram API, with optional caching
        to reduce API calls for repeated requests.

        Args:
            chat_id: Chat identifier (username, ID, etc.).

        Returns:
            Any: Chat entity object from Telegram API.
        """
        if not self.chat_cache:
            async with self.get_acc() as acc:
                return await acc.app.get_entity(chat_id)

        if chat_id not in self.chat_cache:
            async with self.get_acc() as acc:
                chat = await acc.app.get_entity(chat_id)
            self.chat_cache[chat_id] = ChatCacheItem(chat)

        return self.chat_cache[chat_id].chat

    async def get_chat_members_count(self, chat_id) -> int:
        """
        Get the member count for a chat, using cache if available.

        Retrieves the number of participants/members in a chat.
        Uses caching to avoid repeated API calls.

        Args:
            chat_id: Chat identifier (username, ID, etc.).

        Returns:
            int: Number of members in the chat.
        """
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
        """
        Get the number of discussion replies for a message.

        Counts replies to a specific message in a discussion thread.
        Handles various error conditions gracefully.

        Args:
            chat_id: Chat identifier containing the message.
            msg_id (int): Message ID to count replies for.

        Returns:
            int: Number of replies to the message.
        """
        logger.debug(
            f"Getting discussion replies count for chat_id={chat_id}, msg_id={msg_id}"
        )

        # Basic validation
        if not msg_id or msg_id <= 0:
            logger.warning(
                f"Invalid message ID {msg_id} for chat {chat_id} - must be positive integer"
            )
            return 0

        if not chat_id:
            logger.warning(f"Invalid chat_id {chat_id} - cannot be empty")
            return 0

        try:
            async with self.get_acc() as acc:
                logger.debug(
                    f"Using account {acc.phone} for get_discussion_replies_count"
                )
                entity = await acc.app.get_entity(chat_id)
                logger.debug(
                    f"Entity resolved for chat_id={chat_id}: {type(entity).__name__}"
                )

                # Use iter_messages with reply_to to get discussion replies
                count = 0
                async for _ in acc.app.iter_messages(
                    entity, reply_to=msg_id, limit=1000
                ):
                    count += 1

                logger.debug(
                    f"Successfully counted {count} replies for msg_id={msg_id} in chat_id={chat_id}"
                )
                return count

        except MsgIdInvalidError as e:
            logger.warning(
                f"MsgIdInvalidError: Invalid message ID {msg_id} for chat {chat_id}. "
                f"Error details: {e}. This usually means the message doesn't exist or was deleted."
            )
            return 0
        except MessageIdInvalidError as e:
            logger.warning(
                f"MessageIdInvalidError: Invalid message ID {msg_id} for chat {chat_id}. "
                f"Error details: {e}. This usually means the message ID is malformed or out of range."
            )
            return 0
        except PeerIdInvalidError as e:
            logger.warning(
                f"PeerIdInvalidError: Invalid peer (chat) ID {chat_id} for message {msg_id}. "
                f"Error details: {e}. This usually means the chat doesn't exist or is inaccessible."
            )
            return 0
        except Exception as e:
            logger.error(
                f"Unexpected error getting discussion replies count for chat_id={chat_id}, msg_id={msg_id}. "
                f"Error type: {type(e).__name__}, Error details: {e}"
            )
            return 0

    @require(lambda min_date: isinstance(min_date, dt.datetime) or min_date is None)
    async def get_chat_history(
        self, chat_id, limit=None, min_date=None
    ) -> AsyncIterable[Any]:
        """
        Get chat message history.

        Retrieves messages from a chat in chronological order (newest first).
        Optionally filter by date and limit the number of messages.

        Args:
            chat_id: Chat identifier to get history from.
            limit (int, optional): Maximum number of messages to retrieve.
            min_date (datetime.datetime, optional): Minimum date for messages.

        Yields:
            Any: Message objects from the chat.
        """
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
        """
        Get discussion replies for a message.

        Retrieves replies to a specific message in a discussion thread.
        Handles various error conditions gracefully.

        Args:
            chat_id: Chat identifier containing the message.
            msg_id (int): Message ID to get replies for.
            limit (int, optional): Maximum number of replies to retrieve.

        Yields:
            Any: Reply message objects.
        """
        logger.debug(
            f"Getting discussion replies for chat_id={chat_id}, msg_id={msg_id}, limit={limit}"
        )

        # Basic validation
        if not msg_id or msg_id <= 0:
            logger.warning(
                f"Invalid message ID {msg_id} for chat {chat_id} - must be positive integer"
            )
            return

        if not chat_id:
            logger.warning(f"Invalid chat_id {chat_id} - cannot be empty")
            return

        try:
            async with self.get_acc() as acc:
                logger.debug(f"Using account {acc.phone} for get_discussion_replies")
                entity = await acc.app.get_entity(chat_id)
                logger.debug(
                    f"Entity resolved for chat_id={chat_id}: {type(entity).__name__}"
                )

                # Use iter_messages with reply_to to get discussion replies
                reply_count = 0
                async for msg in acc.app.iter_messages(
                    entity, reply_to=msg_id, limit=limit
                ):
                    reply_count += 1
                    yield msg

                logger.debug(
                    f"Successfully yielded {reply_count} replies for msg_id={msg_id} in chat_id={chat_id}"
                )

        except MsgIdInvalidError as e:
            logger.warning(
                f"MsgIdInvalidError: Invalid message ID {msg_id} for chat {chat_id}. "
                f"Error details: {e}. This usually means the message doesn't exist or was deleted."
            )
            return
        except MessageIdInvalidError as e:
            logger.warning(
                f"MessageIdInvalidError: Invalid message ID {msg_id} for chat {chat_id}. "
                f"Error details: {e}. This usually means the message ID is malformed or out of range."
            )
            return
        except PeerIdInvalidError as e:
            logger.warning(
                f"PeerIdInvalidError: Invalid peer (chat) ID {chat_id} for message {msg_id}. "
                f"Error details: {e}. This usually means the chat doesn't exist or is inaccessible."
            )
            return
        except Exception as e:
            logger.error(
                f"Unexpected error getting discussion replies for chat_id={chat_id}, msg_id={msg_id}. "
                f"Error type: {type(e).__name__}, Error details: {e}"
            )
            return

    # removed generic pyrogram command wrappers; each method uses Telethon directly

import cloudpickle
import pyrogram

from ..utils import ensure_at_single


class ChatCacheItem:
    """ "Элемент кэша чатов."""

    chat: pyrogram.types.Chat
    members_count: int

    def __init__(self, chat):
        self.chat = chat
        self.members_count = None


class ChatCache:
    cache: dict[str, ChatCacheItem]

    def __init__(self, fs):
        self.cache = {}
        self.fs = fs

    def __getitem__(self, key):
        return self.cache[ensure_at_single(key)]

    def __setitem__(self, key, value):
        self.cache[ensure_at_single(key)] = value

    def __contains__(self, key):
        return ensure_at_single(key) in self.cache

    def load(self):
        if self.fs.exists(".chat_cache"):
            with self.fs.open(".chat_cache", "rb") as f:
                self.chat_cache = cloudpickle.load(f)

        # нормализуем все названия чатов при загрузке
        self.cache = {ensure_at_single(key): value for key, value in self.cache.items()}

    def save(self):
        with self.fs.open(".chat_cache", "wb") as f:
            cloudpickle.dump(self.chat_cache, f)

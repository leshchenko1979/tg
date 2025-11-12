import cloudpickle

from ..utils import ensure_at_single


class ChatCacheItem:
    """ "Элемент кэша чатов."""

    chat: object
    members_count: int

    def __init__(self, chat):
        self.chat = chat
        self.members_count = None


class ChatCache:
    """
    Cache for storing Telegram chat information to reduce API calls.

    This class caches chat entities and their member counts to avoid repeated
    API requests when the same chat information is needed multiple times.
    The cache is persisted to the file system for reuse across sessions.

    Args:
        fs: File system object for loading/saving cache data.

    Attributes:
        cache (dict[str, ChatCacheItem]): Dictionary mapping chat IDs to cached chat items.
        fs: File system object for persistence.

    Methods:
        __getitem__: Get cached chat item by chat ID.
        __setitem__: Set chat item in cache.
        __contains__: Check if chat ID is in cache.
        load: Load cache from file system.
        save: Save cache to file system.
    """

    cache: dict[str, ChatCacheItem]

    def __init__(self, fs):
        """
        Initialize the chat cache.

        Args:
            fs: File system object for loading/saving cache data.
        """
        self.cache = {}
        self.fs = fs

    def __getitem__(self, key):
        """
        Get cached chat item by chat ID.

        Args:
            key (str): Chat identifier (normalized automatically).

        Returns:
            ChatCacheItem: The cached chat item.

        Raises:
            KeyError: If the chat ID is not in cache.
        """
        return self.cache[ensure_at_single(key)]

    def __setitem__(self, key, value):
        """
        Set chat item in cache.

        Args:
            key (str): Chat identifier (normalized automatically).
            value (ChatCacheItem): The chat item to cache.
        """
        self.cache[ensure_at_single(key)] = value

    def __contains__(self, key):
        """
        Check if chat ID is in cache.

        Args:
            key (str): Chat identifier (normalized automatically).

        Returns:
            bool: True if chat is cached, False otherwise.
        """
        return ensure_at_single(key) in self.cache

    def load(self):
        """
        Load cache from file system.

        Loads previously saved cache data and normalizes all chat names.
        """
        if self.fs.exists(".chat_cache"):
            with self.fs.open(".chat_cache", "rb") as f:
                self.cache = cloudpickle.loads(f)

        # нормализуем все названия чатов при загрузке
        self.cache = {ensure_at_single(key): value for key, value in self.cache.items()}

    def save(self):
        """
        Save cache to file system.

        Persists the current cache state for reuse across sessions.
        """
        with self.fs.open(".chat_cache", "wb") as f:
            f.write(cloudpickle.dumps(self.cache))

import re
import io
from typing import Protocol


def ensure_ats(strs: set[str]) -> set[str]:
    """
    Ensure all strings in a set have the @ prefix.

    Args:
        strs (set[str]): Set of strings to normalize.

    Returns:
        set[str]: Set of strings with @ prefix ensured.
    """
    return {ensure_at_single(s) for s in strs}


def ensure_at_single(s: str) -> str:
    """
    Ensure a single string has the @ prefix and is lowercase.

    Args:
        s (str): String to normalize.

    Returns:
        str: Normalized string with @ prefix and lowercase.
    """
    return (
        s
        if not isinstance(s, str)
        else (s.lower() if s.startswith("@") else f"@{s.lower()}")
    )


def get_nicknames(text: str) -> set[str]:
    """
    Extract Telegram usernames/nicknames from text.

    Finds @mentions and t.me links in text and normalizes them.

    Args:
        text (str): Text to extract usernames from.

    Returns:
        set[str]: Set of normalized usernames with @ prefix.
    """
    if not text:
        return set()

    at_signs = re.findall(r"@[A-Za-z\d_]{5,32}", text)
    links = re.findall(r"https://t\.me/([A-Za-z\d_]{5,32})", text)

    # TODO: игнорируются ссылки доменного типа и пригласительные ссылки, нужно добавить

    return ensure_ats(at_signs) | ensure_ats(links)


def parse_telegram_message_url(url: str) -> (str, int):
    """
    Parse a Telegram message URL and extract chat ID and message ID.

    Supports various Telegram URL formats including direct links and discussion threads.

    Args:
        url (str): Telegram message URL (t.me/... or https://t.me/...).

    Returns:
        tuple: (chat_id, message_id) where chat_id is the username/channel identifier
               and message_id is the integer message ID.

    Raises:
        AssertionError: If URL format is invalid or message ID is not positive.
    """
    # Vaild format:
    # t.me/<username>/<thread_id>/<id>
    # t.me/<username>/<id>
    # t.me/c/<channel>/<id>
    # t.me/c/<channel>/<thread_id>/<id>
    # or the above, but starting with https://

    if url.startswith("https://"):
        url = url[8:]

    parts = url.split("/")

    assert parts[0] == "t.me", "Should start with t.me/ or https://t.me/"

    if len(parts) > 3:
        chat_id = parts[2] if parts[1] == "c" else parts[1]
    else:
        chat_id = parts[1]

    message_id = int(parts[-1])

    assert chat_id
    assert message_id > 0

    return chat_id, message_id

class AbstractFileSystemProtocol(Protocol):
    def glob(self, path: str) -> list[str]:
        pass

    def exists(self, path: str) -> bool:
        pass

    def open(self, path: str, mode: str = "r") -> io.FileIO:
        pass

    def touch(self, path: str) -> None:
        pass

    def rm(self, path: str) -> None:
        pass

    def mkdir(self, path: str) -> None:
        pass

class TQDMProtocol(Protocol):
    def set_postfix_str(self, postfix: str) -> None:
        pass

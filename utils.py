import re


def ensure_ats(strs: set[str]) -> set[str]:
    return {ensure_at_single(s) for s in strs}


def ensure_at_single(s: str) -> str:
    return (
        s
        if not isinstance(s, str)
        else (s.lower() if s.startswith("@") else f"@{s.lower()}")
    )


def get_nicknames(text: str) -> set[str]:
    if not text:
        return set()

    at_signs = re.findall(r"@[A-Za-z\d_]{5,32}", text)
    links = re.findall(r"https://t\.me/([A-Za-z\d_]{5,32})", text)

    # TODO: игнорируются ссылки доменного типа и пригласительные ссылки, нужно добавить

    return ensure_ats(at_signs) | ensure_ats(links)


def parse_telegram_message_url(url: str) -> (str, int):
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

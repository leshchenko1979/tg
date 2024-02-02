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

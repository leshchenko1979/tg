import pytest

from sender import parse_telegram_message_url


# Happy path tests with various realistic test values
@pytest.mark.parametrize(
    "url, expected_chat_id, expected_message_id",
    [
        ("t.me/username/123", "username", 123),
        ("t.me/c/channel/456", "channel", 456),
        ("t.me/username/thread_id/789", "username", 789),
        ("https://t.me/username/101112", "username", 101112),
        ("https://t.me/c/channel/thread_id/131415", "channel", 131415),
    ],
    ids=[
        "happy-username",
        "happy-channel",
        "happy-thread-id",
        "happy-https-username",
        "happy-https-channel-thread-id",
    ],
)
def test_parse_telegram_url_happy_path(url, expected_chat_id, expected_message_id):
    # Act
    chat_id, message_id = parse_telegram_message_url(url)

    # Assert
    assert chat_id == expected_chat_id
    assert message_id == expected_message_id


# Edge cases
@pytest.mark.parametrize(
    "url, expected_chat_id, expected_message_id",
    [
        ("t.me/username/0/1", "username", 1),
        ("https://t.me/c/channel/0/2", "channel", 2),
    ],
    ids=["edge-zero-thread-id", "edge-https-zero-tread-id"],
)
def test_parse_telegram_url_edge_cases(url, expected_chat_id, expected_message_id):
    # Act
    chat_id, message_id = parse_telegram_message_url(url)

    # Assert
    assert chat_id == expected_chat_id
    assert message_id == expected_message_id


# Error cases
@pytest.mark.parametrize(
    "url, exception",
    [
        ("", AssertionError),
        ("t.me/username/-123", AssertionError),
        ("t.me/username/notanumber", ValueError),
        ("notat.me/username/123", AssertionError),
        ("t.me/username/", ValueError),
        ("t.me//123", AssertionError),
        ("https://t.me/username", ValueError),
    ],
    ids=[
        "error-empty-string",
        "error-negative-message-id",
        "error-non-numeric-message-id",
        "error-invalid-domain",
        "error-no-chat-id",
        "error-no-message-id",
        "error-https-no-message-id",
    ],
)
def test_parse_telegram_url_error_cases(url, exception):
    # Act & Assert
    with pytest.raises(exception):
        parse_telegram_message_url(url)

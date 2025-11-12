import pytest
from unittest.mock import AsyncMock, MagicMock
from tg.account import AccountCollection
from tg.account import AccountStartFailed

# Test Account mock class
class MockAccount:
    def __init__(self, phone="test_phone", started=False, raises: Exception = None):
        self.phone = phone
        self.started = started
        self.raises = raises

    async def start(self, revalidate=False):
        if self.raises:
            raise self.raises

        self.started = True

    async def stop(self):
        self.started = False

@pytest.fixture
def mock_fs():
    fs = MagicMock()
    fs.exists.return_value = False
    fs.touch = AsyncMock()
    fs.rm = AsyncMock()
    return fs

@pytest.fixture
def account_dict():
    return {
        "account1": MockAccount(phone="account1", raises=AccountStartFailed("account1")),
        "account2": MockAccount(phone="account2"),
    }

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid, expected_exception, test_id",
    [
        ("ignore", None, "happy_path_ignore"),
        ("raise", AccountStartFailed, "error_path_raise"),
        ("revalidate", AccountStartFailed, "happy_path_revalidate"),
    ],
)
async def test_start_sessions(invalid, expected_exception, test_id, account_dict, mock_fs):
    # Arrange
    collection = AccountCollection(accounts=account_dict, fs=mock_fs, invalid=invalid)

    # Act
    if expected_exception:
        with pytest.raises(expected_exception):
            await collection.start_sessions()
    else:
        await collection.start_sessions()

    # Assert
    for account in account_dict.values():
        assert account.started == (account.raises is None)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fs_exists, expected_exception, test_id",
    [
        (False, None, "happy_path_no_lock"),
        (True, RuntimeError, "error_path_lock_exists"),
    ],
)
async def test_session_context_manager(fs_exists, expected_exception, test_id, account_dict, mock_fs):
    mock_fs.exists.return_value = fs_exists
    collection = AccountCollection(accounts=account_dict, fs=mock_fs, invalid="ignore")

    # Act / Assert
    if expected_exception:
        with pytest.raises(expected_exception):
            async with collection.session():
                pass
    else:
        async with collection.session():
            mock_fs.touch.assert_called_once_with(".session_lock")
        mock_fs.rm.assert_called_once_with(".session_lock")
        for account in account_dict.values():
            assert not account.started

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "started, test_id",
    [
        (True, "happy_path_started"),
        (False, "happy_path_not_started"),
    ],
)
async def test_close_sessions(started, test_id, account_dict, mock_fs):
    for account in account_dict.values():
        account.started = started

    collection = AccountCollection(accounts=account_dict, fs=mock_fs, invalid="ignore")

    # Act
    await collection.close_sessions()

    # Assert
    for account in account_dict.values():
        assert not account.started

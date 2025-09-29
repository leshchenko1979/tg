import contextlib
import datetime as dt
import os
import logging

from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon import TelegramClient

from ..utils import AbstractFileSystemProtocol

logger = logging.getLogger(__name__)


class AccountStartFailed(Exception):
    """
    Exception raised by AccountCollection when an account fails to start.

    Examples:
        >>> raise AccountStartFailed("Failed to start account")
    """

    def __init__(self, phone: str):
        self.phone = phone


class Account:
    """
    Account class for managing Telegram user accounts.

    Args:
        fs: File system object.
        phone (str): Phone number associated with the account.
        filename (str): Name of the session file.

    Attributes:
        app (telethon.client.telegramclient.TelegramClient): Telethon client object.
        fs (fsspec.spec.AbstractFileSystem): File system object.
        phone (str): Phone number associated with the account.
        filename (str): Name of the session file.
        started (bool): Indicates if the account has been started.
        flood_wait_timeout (int): Timeout for flood wait.
        flood_wait_from (datetime.datetime): Start time of flood wait.

    Methods:
        __init__: Initialize the Account object.
        __repr__: Return a string representation of the Account object.
        start: Start the account session.
        setup_new_session: Set up a new session for the account.
        stop: Stop the account session.
        save_session_string: Save the session string to a file.
        session: Context manager for managing the account session.
    """

    app: TelegramClient
    fs: AbstractFileSystemProtocol
    phone: str
    filename: str

    started: bool
    flood_wait_timeout: int
    flood_wait_from: dt.datetime

    def __init__(self, /, fs: AbstractFileSystemProtocol, phone=None, filename=None):
        self.filename = filename or f"{phone}.session"
        self.fs = fs
        self.phone = phone
        self.started = False
        self.flood_wait_timeout = 0
        self.flood_wait_from = None
        self.app = None

    def __repr__(self) -> str:
        return f"<Account {self.phone}>"

    @contextlib.asynccontextmanager
    async def session(self, revalidate):
        try:
            await self.start(revalidate)
            yield

        finally:
            await self.stop()

    async def start(
        self,
        revalidate: bool,
        code_retrieval_func=lambda: input("Enter code:"),
        password_retrieval_func=lambda: input("Enter password:"),
    ):
        """
        Start the account session.

        Args:
            revalidate (bool): Flag indicating whether to revalidate the session
            if it fails to connect on the first try or no session file is found.
            code_retrieval_func (callable): Function to retrieve the verification code.
            password_retrieval_func (callable): Function to retrieve the account password.

        Raises:
            RuntimeError: If no session file exists for the account and revalidate == False.
            AuthKeyUnregistered: If the authentication key is unregistered and revalidate == False.
            UserDeactivated: If the user account is deactivated and revalidate == False.

        Examples:
            >>> await account.start(revalidate=True)
        """
        logger.debug(
            "Account.start called for %s; revalidate=%s", self.phone, revalidate
        )
        if self.fs.exists(self.filename):
            logger.debug("Session file '%s' exists; attempting to load", self.filename)
            with self.fs.open(self.filename, "r") as f:
                raw_session_str = f.read().strip()
            logger.debug("Loaded session string with length=%d", len(raw_session_str))

            try:
                session = StringSession(raw_session_str)
            except Exception:
                logger.warning(
                    "Failed to load StringSession from stored value; revalidate=%s",
                    revalidate,
                )
                if revalidate:
                    await self.setup_new_session(
                        code_retrieval_func, password_retrieval_func
                    )
                    self.started = True
                    self.flood_wait_timeout = 0
                    self.flood_wait_from = None
                    logger.info(
                        "Created new Telethon session via revalidation for %s",
                        self.phone,
                    )
                    return
                else:
                    raise

            self.app = TelegramClient(
                session,
                int(os.environ["API_ID"]),
                os.environ["API_HASH"],
            )

            await self.app.connect()
            logger.debug(
                "Client connected for %s: is_connected=%s",
                self.phone,
                self.app.is_connected(),
            )

            if not await self.app.is_user_authorized():
                logger.info("Client is not authorized for %s", self.phone)
                if revalidate:
                    await self.setup_new_session(
                        code_retrieval_func, password_retrieval_func
                    )
                else:
                    raise RuntimeError("Saved session is not authorized")

        elif revalidate:
            logger.debug(
                "No session file; starting setup_new_session for %s", self.phone
            )
            await self.setup_new_session(code_retrieval_func, password_retrieval_func)
        else:
            raise RuntimeError(f"No session file for {self.phone}")

        self.started = True
        self.flood_wait_timeout = 0
        self.flood_wait_from = None
        logger.info("Account started for %s", self.phone)

    async def setup_new_session(self, code_retrieval_func, password_retrieval_func):
        """
        Set up a new session for the account.

        Args:
            code_retrieval_func (callable): Function to retrieve the verification code.
            password_retrieval_func (callable): Function to retrieve the account password.

        Examples:
            >>> await setup_new_session(code_retrieval_func, password_retrieval_func)
        """
        logger.info("Setting up new session for %s", self.phone)
        self.app = TelegramClient(
            StringSession(), int(os.environ["API_ID"]), os.environ["API_HASH"]
        )

        await self.app.connect()
        logger.debug("Connected during setup_new_session for %s", self.phone)

        await self.app.send_code_request(self.phone)
        logger.debug("Code request sent to %s", self.phone)

        try:
            code = code_retrieval_func()
            logger.debug("Attempting sign_in for %s", self.phone)
            await self.app.sign_in(self.phone, code)
        except SessionPasswordNeededError:
            logger.info("2FA required for %s; prompting for password", self.phone)
            password = password_retrieval_func()
            await self.app.sign_in(password=password)

        self.started = True
        logger.info("New session established for %s", self.phone)
        # Persist the new Telethon session immediately to avoid stale subsequent reads
        await self.save_session_string()

    async def stop(self):
        if not self.started:
            return

        await self.save_session_string()

        if self.app.is_connected():
            logger.debug("Disconnecting client for %s", self.phone)
            await self.app.disconnect()

        self.started = False
        logger.info("Account stopped for %s", self.phone)

    async def save_session_string(self):
        # Save StringSession representation
        session_str = self.app.session.save()

        with self.fs.open(self.filename, "w") as f:
            f.write(session_str)
        logger.debug(
            "Session string saved for %s (length=%d)", self.phone, len(session_str)
        )

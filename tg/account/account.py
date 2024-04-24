import asyncio
import contextlib
import datetime as dt
import os

import icontract
import pyrogram
from pyrogram.errors import AuthKeyUnregistered, UserDeactivated, SessionPasswordNeeded

from ..utils import AbstractFileSystemProtocol


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
        app (pyrogram.Client): Pyrogram client object.
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

    app: pyrogram.Client
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
        if self.fs.exists(self.filename):
            with self.fs.open(self.filename, "r") as f:
                session_str = f.read()

            self.app = pyrogram.Client(
                self.phone, session_string=session_str, in_memory=True, no_updates=True
            )

            try:
                await self.app.start()

            except (AuthKeyUnregistered, UserDeactivated):
                if revalidate:
                    await self.setup_new_session(
                        code_retrieval_func, password_retrieval_func
                    )
                else:
                    raise

        elif revalidate:
            await self.setup_new_session(code_retrieval_func, password_retrieval_func)
        else:
            raise RuntimeError(f"No session file for {self.phone}")

        self.started = True
        self.flood_wait_timeout = 0
        self.flood_wait_from = None

    async def setup_new_session(self, code_retrieval_func, password_retrieval_func):
        """
        Set up a new session for the account.

        Args:
            code_retrieval_func (callable): Function to retrieve the verification code.
            password_retrieval_func (callable): Function to retrieve the account password.

        Examples:
            >>> await setup_new_session(code_retrieval_func, password_retrieval_func)
        """
        print(self.phone)
        self.app = pyrogram.Client(
            self.phone,
            os.environ["API_ID"],
            os.environ["API_HASH"],
            in_memory=True,
            no_updates=True,
            phone_number=self.phone,
        )

        await self.app.connect()

        code_object = await self.app.send_code(self.phone)

        try:
            await self.app.sign_in(
                self.phone, code_object.phone_code_hash, code_retrieval_func()
            )
        except SessionPasswordNeeded:
            await self.app.check_password(password_retrieval_func())

        self.started = True

    async def stop(self):
        if not self.started:
            return

        await self.save_session_string()

        await self.app.stop()

        self.started = False

    async def save_session_string(self):
        session_str = await self.app.export_session_string()

        with self.fs.open(self.filename, "w") as f:
            f.write(session_str)


class AccountCollection:
    """
    Collection of accounts with session management.

    Args:
        accounts (dict): Dictionary of accounts.
        fs: File system object.
        invalid (str): Behaviour during session start-up.
        Can be "ignore", "raise", or "revalidate":
            - "ignore": Ignore any exceptions and continue.
            - "raise": Raise AccountStartFailed if an exception occurs.
            - "revalidate": Revalidate the session if an exception occurs.

    Examples:
        >>> collection = AccountCollection(accounts, fs, invalid)
        >>> async with collection.session():
        ...     # Perform actions within the session
    """

    accounts: dict[str, Account]

    @icontract.require(lambda invalid: invalid in ["ignore", "raise", "revalidate"])
    def __init__(
        self, accounts: dict[str, Account], fs: AbstractFileSystemProtocol, invalid: str
    ):
        self.accounts = accounts
        self.fs = fs
        self.invalid = invalid

    def __getitem__(self, item):
        return self.accounts[item]

    @contextlib.asynccontextmanager
    async def session(self):
        """
        Context manager for managing the account session.
        Prevents other applications from using the same sessions.
        Automatically closes the sessions when the context is exited.
        Allows for progress bars to be displayed during the session.

        Raises:
            RuntimeError: If sessions are already in use.
        """
        SESSION_LOCK = ".session_lock"
        if self.fs.exists(SESSION_LOCK):
            raise RuntimeError("Sessions are already in use")

        try:
            await self.start_sessions()
            self.fs.touch(SESSION_LOCK)
            yield

        finally:
            self.fs.rm(SESSION_LOCK)
            await self.close_sessions()

    async def start_sessions(self):
        """
        Start the sessions for all accounts.

        Raises:
            AccountStartFailed: If an exception occurs during account start
            and self.invalid != "ignore".

        Examples:
            >>> await start_sessions()
        """
        tasks = {
            phone: asyncio.create_task(
                acc.start(revalidate=self.invalid == "revalidate")
            )
            for phone, acc in self.accounts.items()
        }

        return_when = (
            asyncio.FIRST_EXCEPTION
            if self.invalid != "ignore"
            else asyncio.ALL_COMPLETED
        )

        await asyncio.wait(tasks.values(), return_when=return_when)

        # Check if any exceptions occured during the above wait
        for phone, task in tasks.items():
            exc = None
            with contextlib.suppress(asyncio.InvalidStateError):
                exc = task.exception()

            if exc:
                if self.invalid != "ignore":
                    raise AccountStartFailed(phone) from exc
                else:
                    print(f"Exception for {phone}: {exc}")

    async def close_sessions(self):
        await asyncio.gather(
            *[acc.stop() for acc in self.accounts.values() if acc.started]
        )

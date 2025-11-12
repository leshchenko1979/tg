import asyncio
import contextlib
import datetime as dt
import logging
from typing import Optional

import icontract
from telethon.errors.rpcerrorlist import FloodWaitError

from ..utils import AbstractFileSystemProtocol, TQDMProtocol
from .account import Account, AccountStartFailed

logger = logging.getLogger(__name__)

MAX_ACC_WAITING_TIME = 60 * 5  # max waiting time for an available account


class AccountCollection:
    """
    Collection of accounts with session management and account pooling.

    Args:
        accounts (dict): Dictionary of accounts.
        fs: File system object.
        invalid (str): Behaviour during session start-up.
        Can be "ignore", "raise", or "revalidate":
            - "ignore": Ignore any exceptions and continue.
            - "raise": Raise AccountStartFailed if an exception occurs.
            - "revalidate": Revalidate the session if an exception occurs.
        api_id (int, optional): Telegram API ID. If not provided, will be loaded from API_ID environment variable.
        api_hash (str, optional): Telegram API hash. If not provided, will be loaded from API_HASH environment variable.

    Examples:
        >>> collection = AccountCollection(accounts, fs, invalid)
        >>> async with collection.session():
        ...     # Perform actions within the session
    """

    accounts: dict[str, Account]
    available_accs: asyncio.Queue
    pbar: Optional[TQDMProtocol]
    api_id: int
    api_hash: str

    @icontract.require(lambda invalid: invalid in ["ignore", "raise", "revalidate"])
    def __init__(
        self, accounts: dict[str, Account], fs: AbstractFileSystemProtocol, invalid: str, api_id=None, api_hash=None
    ):
        self.accounts = accounts
        self.fs = fs
        self.invalid = invalid
        self.api_id = api_id
        self.api_hash = api_hash
        self.available_accs = asyncio.Queue()
        self.pbar = None

    def __getitem__(self, item):
        return self.accounts[item]

    @contextlib.asynccontextmanager
    async def session(self, pbar: Optional[TQDMProtocol] = None):
        """
        Context manager for managing the account session.
        Prevents other applications from using the same sessions.
        Automatically closes the sessions when the context is exited.
        Allows for progress bars to be displayed during the session.

        Args:
            pbar: Optional progress bar to display during the session.

        Raises:
            RuntimeError: If sessions are already in use.
        """
        SESSION_LOCK = ".session_lock"
        if self.fs.exists(SESSION_LOCK):
            raise RuntimeError("Sessions are already in use")

        try:
            self.pbar = pbar
            await self.start_sessions()
            self.fs.touch(SESSION_LOCK)
            yield

        finally:
            self.fs.rm(SESSION_LOCK)
            await self.close_sessions()
            self.pbar = None

    @icontract.ensure(
        lambda self: any(acc.started for acc in self.accounts.values()),
        "No valid accounts were started",
    )
    async def start_sessions(self):
        """
        Start the sessions for all accounts and setup the available queue.

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
                    logger.warning("Exception for %s: %s", phone, exc)

        # Setup the available queue with started accounts
        self.setup_available_queue()

    async def close_sessions(self):
        await asyncio.gather(
            *[acc.stop() for acc in self.accounts.values() if acc.started]
        )
        self.available_accs = asyncio.Queue()

    def setup_available_queue(self):
        """Setup the available accounts queue with started accounts."""
        started_count = 0
        for acc in self.accounts.values():
            if acc.started:
                self.available_accs.put_nowait(acc)
                started_count += 1
                logger.info(f"Account {acc.phone} added to available queue")
            else:
                logger.warning(f"Account {acc.phone} failed to start")

        logger.info(
            f"Setup {started_count}/{len(self.accounts)} accounts in available queue"
        )

    @contextlib.asynccontextmanager
    async def get_acc(self):  # sourcery skip: raise-from-previous-error
        """
        Get an available account from the queue.

        Yields:
            Account: An available account for use.

        Raises:
            RuntimeError: If no accounts are available within the timeout period.
        """
        logger.debug(
            f"Requesting account, timeout: {MAX_ACC_WAITING_TIME}s. Available queue size: {self.available_accs.qsize()}"
        )

        try:
            acc: Account = await asyncio.wait_for(
                self.available_accs.get(), timeout=MAX_ACC_WAITING_TIME
            )
            logger.debug(
                f"Got account: {acc.phone}. Accounts in queue: {self.available_accs.qsize()}"
            )
        except asyncio.TimeoutError:
            # Only check flood_wait after timing out waiting for accounts
            logger.debug(
                f"Timeout waiting for account. Queue size: {self.available_accs.qsize()}"
            )
            min_wait = self.min_wait()
            if min_wait and min_wait > 0:
                available_at = dt.datetime.now() + dt.timedelta(seconds=min_wait)
                logger.error(
                    f"All accounts unavailable. First available at {available_at}. Min wait: {min_wait}"
                )
                raise RuntimeError(
                    f"All accounts unavailable. First available at {available_at}."
                )
            else:
                logger.error(
                    f"Timeout waiting for account after {MAX_ACC_WAITING_TIME}s. No accounts in flood_wait."
                )
                raise RuntimeError(
                    f"All accounts unavailable. Max waiting time of {MAX_ACC_WAITING_TIME} secs exceeded."
                )

        try:
            yield acc
            logger.debug(f"Returning account {acc.phone} to queue")
            self.available_accs.put_nowait(acc)

        except FloodWaitError as e:
            timeout_seconds = getattr(e, "seconds", None)
            if timeout_seconds is None and hasattr(e, "retry_after"):
                timeout_seconds = e.retry_after
            if timeout_seconds is None:
                timeout_seconds = 60
            logger.warning(f"FloodWaitError for {acc.phone}: {timeout_seconds}s")
            asyncio.create_task(self.flood_wait(acc, timeout_seconds))

        except Exception as e:
            logger.error(f"Exception for account {acc.phone}: {e}")
            self.available_accs.put_nowait(acc)
            raise

    def min_wait(self) -> Optional[float]:
        """
        Calculate the minimum wait time for accounts in flood_wait state.

        Returns:
            Optional[float]: The minimum remaining wait time in seconds, or None if no accounts are in flood_wait.
        """
        flood_wait_accounts = []
        for acc in self.accounts.values():
            if acc.flood_wait_from:
                elapsed = (dt.datetime.now() - acc.flood_wait_from).total_seconds()
                remaining = acc.flood_wait_timeout - elapsed
                flood_wait_accounts.append(
                    (acc.phone, remaining, acc.flood_wait_timeout, elapsed)
                )
                logger.debug(
                    f"Account {acc.phone}: timeout={acc.flood_wait_timeout}s, elapsed={elapsed:.1f}s, remaining={remaining:.1f}s"
                )

        if flood_wait_accounts:
            min_remaining = min(remaining for _, remaining, _, _ in flood_wait_accounts)
            logger.info(
                f"Flood wait accounts: {len(flood_wait_accounts)}, min remaining: {min_remaining:.1f}s"
            )
            for phone, remaining, timeout, elapsed in flood_wait_accounts:
                logger.info(
                    f"  {phone}: {remaining:.1f}s remaining (timeout: {timeout}s, elapsed: {elapsed:.1f}s)"
                )
            return min_remaining
        else:
            logger.debug("No accounts in flood_wait state")
            return None

    async def flood_wait(self, acc: Account, timeout: int):
        """
        Handle flood wait for an account.

        Args:
            acc: The account that needs to wait.
            timeout: The timeout duration in seconds.
        """
        acc.flood_wait_from = dt.datetime.now()
        acc.flood_wait_timeout = timeout

        logger.info(f"Account {acc.phone} entering flood_wait for {timeout} seconds")

        if self.pbar:
            old_postfix = self.pbar.postfix or ""
            self.pbar.set_postfix_str(
                ", ".join([old_postfix, f"{acc}: flood_wait {timeout} secs"])
            )
        else:
            logger.info("%s: flood_wait %s secs", acc, timeout)

        await asyncio.sleep(timeout)

        logger.info(
            f"Account {acc.phone} flood_wait completed, returning to available queue"
        )

        if self.pbar:
            self.pbar.set_postfix_str(old_postfix)

        self.available_accs.put_nowait(acc)

        acc.flood_wait_from = None
        acc.flood_wait_timeout = 0

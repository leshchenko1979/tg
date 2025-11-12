import contextlib
import os
import tempfile

import supabase


class SupabaseTableFileSystem:
    """
    File system interface for Supabase database tables.

    This class provides a file system-like interface for storing and retrieving
    key-value pairs from a Supabase database table. It implements common file
    system operations like reading, writing, checking existence, and listing.

    Args:
        supabase (supabase.Client): Supabase client instance.
        table_name (str): Name of the database table to use for storage.

    Attributes:
        table: Supabase table instance for the specified table_name.

    Methods:
        __getitem__: Get value by key.
        __setitem__: Set value for key.
        __delitem__: Delete key-value pair.
        __contains__: Check if key exists.
        keys: Get all keys.
        ls: Alias for keys().
        exists: Check if key exists.
        rm: Delete key-value pair.
        glob: Find keys matching pattern.
        touch: Create empty entry for key.
        open: Context manager for file-like operations.
    """

    def __init__(self, supabase: supabase.Client, table_name):
        self.table = supabase.table(table_name)

    def _get_row(self, path):
        """
        Get a row from the database by key.

        Args:
            path (str): The key to look up.

        Returns:
            dict or None: The row data if found, None otherwise.
        """
        res = self.table.select("key", "value").eq("key", path).limit(1).execute()
        data = getattr(res, "data", None) or []
        return data[0] if data else None

    def __getitem__(self, path):
        """
        Get value by key.

        Args:
            path (str): The key to look up.

        Returns:
            str: The value associated with the key.

        Raises:
            KeyError: If the key does not exist.
        """
        row = self._get_row(path)
        if not row:
            raise KeyError(path)
        return row["value"]

    def __setitem__(self, path, value):
        """
        Set value for a key. Creates or updates the key-value pair.

        Args:
            path (str): The key to set.
            value: The value to associate with the key.
        """
        self.table.upsert({"key": path, "value": value}).execute()

    def __delitem__(self, path):
        """
        Delete a key-value pair.

        Args:
            path (str): The key to delete.
        """
        self.table.delete().eq("key", path).execute()

    def keys(self):
        """
        Get all keys in the database.

        Returns:
            list[str]: List of all keys.
        """
        res = self.table.select("key").execute()
        data = getattr(res, "data", None) or []
        return [item["key"] for item in data]

    def __contains__(self, path):
        """
        Check if a key exists.

        Args:
            path (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self._get_row(path) is not None

    def ls(self, *args):
        """
        List all keys (alias for keys()).

        Returns:
            list[str]: List of all keys.
        """
        return self.keys()

    def exists(self, path):
        """
        Check if a key exists.

        Args:
            path (str): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self._get_row(path) is not None

    def rm(self, path):
        """
        Remove a key-value pair (alias for __delitem__).

        Args:
            path (str): The key to remove.
        """
        del self[path]

    def glob(self, path):
        """
        Find keys matching a pattern using SQL LIKE.

        Args:
            path (str): Pattern to match (supports SQL LIKE wildcards).

        Returns:
            list[str]: List of matching keys.
        """
        res = self.table.select("key").filter("key", "like", path).execute()
        data = getattr(res, "data", None) or []
        return [item["key"] for item in data]

    def touch(self, path):
        """
        Create an empty entry for a key.

        Args:
            path (str): The key to create.
        """
        self[path] = ""

    @contextlib.contextmanager
    def open(self, path, mode="r"):
        """
        Open a file-like interface for a key.

        Provides a file-like object that reads from and writes to the database.
        Changes are only persisted to the database when the context manager exits
        and the mode allows writing.

        Args:
            path (str): The key to open.
            mode (str): File mode ("r", "w", "a", "+", etc.).

        Yields:
            io.FileIO: File-like object for reading/writing.
        """
        # Respect mode semantics on writeback
        write_back = any(m in mode for m in ("w", "+", "a"))

        with tempfile.TemporaryDirectory() as td:
            tmp_path = os.path.join(td, "_tmp")

            # Prepare temp file content based on mode
            if "w" in mode and "+" not in mode and "a" not in mode:
                initial = ""
            else:
                row = self._get_row(path)
                initial = row["value"] if row else ""

            # Create and fill temp file
            with open(tmp_path, "w+", encoding="utf-8") as f:
                if initial:
                    f.write(initial)
                    f.seek(0)

                yield f

                # Ensure data is flushed to disk before we read back
                f.flush()
                os.fsync(f.fileno())

            # Only write back to Supabase if caller intended to write
            if write_back:
                with open(tmp_path, "r", encoding="utf-8") as f:
                    self[path] = f.read()

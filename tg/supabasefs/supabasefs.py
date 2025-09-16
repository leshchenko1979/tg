import contextlib
import os
import tempfile

import supabase


class SupabaseTableFileSystem:
    def __init__(self, supabase: supabase.Client, table_name):
        self.table = supabase.table(table_name)

    def _get_row(self, path):
        res = self.table.select("key", "value").eq("key", path).limit(1).execute()
        data = getattr(res, "data", None) or []
        return data[0] if data else None

    def __getitem__(self, path):
        row = self._get_row(path)
        if not row:
            raise KeyError(path)
        return row["value"]

    def __setitem__(self, path, value):
        self.table.upsert({"key": path, "value": value}).execute()

    def __delitem__(self, path):
        self.table.delete().eq("key", path).execute()

    def keys(self):
        res = self.table.select("key").execute()
        data = getattr(res, "data", None) or []
        return [item["key"] for item in data]

    def __contains__(self, path):
        return self._get_row(path) is not None

    def ls(self, *args):
        return self.keys()

    def exists(self, path):
        return self._get_row(path) is not None

    def rm(self, path):
        del self[path]

    def glob(self, path):
        res = self.table.select("key").filter("key", "like", path).execute()
        data = getattr(res, "data", None) or []
        return [item["key"] for item in data]

    def touch(self, path):
        self[path] = ""

    @contextlib.contextmanager
    def open(self, path, mode="r"):
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

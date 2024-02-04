import contextlib
import os
import tempfile

import supabase


class SupabaseTableFileSystem:
    def __init__(self, supabase: supabase.Client, table_name):
        self.table = supabase.table(table_name)

    def __getitem__(self, path):
        return self.table.select("value").eq("key", path).execute().data[0]["value"]

    def __setitem__(self, path, value):
        self.table.upsert({"key": path, "value": value}).execute()

    def __delitem__(self, path):
        self.table.delete().eq("key", path).execute()

    def keys(self):
        return [
            next(iter(item.values()))
            for item in self.table.select("key").execute().data
        ]

    def __contains__(self, path):
        return path in self.keys()

    def ls(self, *args):
        return self.keys()

    def exists(self, path):
        return path in self

    def rm(self, path):
        del self[path]

    def glob(self, path):
        return [
            next(iter(item.values()))
            for item in self.table.select("key")
            .filter("key", "like", path)
            .execute()
            .data
        ]

    def touch(self, path):
        self[path] = ""

    @contextlib.contextmanager
    def open(self, path, mode=None):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, path), "x+") as f:
                if self.exists(path):
                    f.write(self[path])
                    f.seek(0)

                yield f

            with open(os.path.join(td, path), "r") as f:
                self[path] = f.read()

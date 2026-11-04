import os
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from yoyo import get_backend, read_migrations


def create_pool():
    pool = ConnectionPool(
        os.environ["DATABASE_URL"],
        min_size=1,
        max_size=10,
        kwargs={"row_factory": dict_row, "autocommit": True},
        open=False,
    )
    pool.open(wait=True)
    return pool


def migrate(pool, path):
    url = pool.conninfo.replace("postgresql://", "postgresql+psycopg://", 1)
    with get_backend(url) as backend, backend.lock():
        migrations = read_migrations(str(Path(path)))
        backend.apply_migrations(backend.to_apply(migrations))

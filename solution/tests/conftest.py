import uuid

import pytest

from pipeline.schema import load_dispositions


@pytest.fixture
def conn():
    """A fresh, isolated Postgres schema per test (dropped afterwards).

    Skips if the docker Postgres isn't running.
    """
    psycopg = pytest.importorskip("psycopg")
    from pipeline import db

    schema_name = f"test_{uuid.uuid4().hex[:8]}"
    try:
        c = db.connect(search_path=schema_name)
    except psycopg.OperationalError:
        pytest.skip("Postgres not running (docker compose up -d)")
    db.init_db(c)
    db.sync_dispositions(c, load_dispositions())
    yield c
    c.rollback()
    c.execute(f'DROP SCHEMA "{schema_name}" CASCADE')
    c.commit()
    c.close()

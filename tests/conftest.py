import time
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from api.main import app

SOURCE_URL = "postgresql://postgres:postgres@localhost:5433/source_db"
TARGET_URL = "postgresql://postgres:postgres@localhost:5434/target_db"


@pytest.fixture(scope="session")
def client():
    return TestClient(app)


def wait_for_db(url, retries=15, delay=2):
    engine = create_engine(url)
    for _ in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            time.sleep(delay)
    raise RuntimeError(f"DB at {url} is not ready")


@pytest.fixture(scope="session")
def source_engine():
    return wait_for_db(SOURCE_URL)


@pytest.fixture(scope="session")
def target_engine():
    return wait_for_db(TARGET_URL)


@pytest.fixture
def prepare_source(source_engine):
    with source_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))
        conn.execute(text("""
            INSERT INTO users (email, name, age, city) VALUES
            ('a@test.com', 'Alice', 25, 'London'),
            ('b@test.com', 'Bob', 30, 'Paris'),
            ('d@test.com', 'David', 40, 'Berlin'),
            ('e@test.com', 'Eve', 35, 'Rome')
        """))
    return True


@pytest.fixture
def prepare_target(target_engine):
    with target_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))

        conn.execute(text("""
            INSERT INTO users (id, email, name, age, city) VALUES
            (1, 'a@test.com', 'Alice PROD', 25, 'London PROD'),
            (2, 'b@test.com', 'Bob PROD', NULL, NULL),
            (3, 'c@test.com', 'Charlie', 30, 'Madrid')
        """))

        # 🔥 КРИТИЧНО: синхронизируем sequence с максимальным id
        conn.execute(text("""
            SELECT setval(
                pg_get_serial_sequence('users', 'id'),
                (SELECT MAX(id) FROM users)
            )
        """))

    return True


import pytest
from sqlalchemy import create_engine, text
from syncer.db_syncer import DBSyncer
import time

SOURCE_URL = "postgresql://postgres:postgres@localhost:5433/source_db"
TARGET_URL = "postgresql://postgres:postgres@localhost:5434/target_db"


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


@pytest.fixture(scope="module")
def source_engine():
    return wait_for_db(SOURCE_URL)


@pytest.fixture(scope="module")
def target_engine():
    return wait_for_db(TARGET_URL)


# Подготовка данных перед тестом для source БД
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


# Подготовка данных перед тестом для target БД
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
    return True


@pytest.mark.parametrize("strategy", ["skip", "overwrite", "merge"])
def test_sync_postgres(source_engine, target_engine, prepare_source, prepare_target, strategy):
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)

    syncer.sync_schema(interactive=False)
    syncer.sync_data(pk_strategy=strategy)

    # Считываем результат
    with target_engine.connect() as conn:
        rows = conn.execute(text("SELECT id, name, city, age FROM users ORDER BY id")).mappings().all()
        result_dict = {row['id']: row for row in rows}

    if strategy == "skip":
        # Существующие значения остаются, NULL не меняются
        assert result_dict[1]['name'] == "Alice PROD"
        assert result_dict[1]['city'] == "London PROD"
        assert result_dict[1]['age'] == 25
        assert result_dict[2]['name'] == "Bob PROD"
        assert result_dict[2]['city'] is None
        assert result_dict[2]['age'] is None

    elif strategy == "overwrite":
        # Все поля берутся из source
        assert result_dict[1]['name'] == "Alice"
        assert result_dict[1]['city'] == "London"
        assert result_dict[1]['age'] == 25
        assert result_dict[2]['name'] == "Bob"
        assert result_dict[2]['city'] == "Paris"
        assert result_dict[2]['age'] == 30

    elif strategy == "merge":
        # id=1 → все поля заполнены → остаются как есть
        assert result_dict[1]['name'] == "Alice PROD"
        assert result_dict[1]['city'] == "London PROD"
        assert result_dict[1]['age'] == 25

        # id=2 → age и city были NULL → берутся из source
        assert result_dict[2]['name'] == "Bob PROD"
        assert result_dict[2]['city'] == "Paris"
        assert result_dict[2]['age'] == 30

        # id=3 → строка без source → не меняется
        assert result_dict[3]['name'] == "Charlie"
        assert result_dict[3]['city'] == "Madrid"
        assert result_dict[3]['age'] == 30

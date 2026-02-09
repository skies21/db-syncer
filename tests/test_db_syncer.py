import pytest
from sqlalchemy import text
from syncer.db_syncer import DBSyncer

SOURCE_URL = "postgresql://postgres:postgres@localhost:5433/source_db"
TARGET_URL = "postgresql://postgres:postgres@localhost:5434/target_db"


@pytest.mark.parametrize("strategy", ["skip", "overwrite", "merge"])
def test_sync_postgres(source_engine, target_engine, prepare_source, prepare_target, strategy):
    """
    Тестирует синхронизацию данных между source и target DB на разных стратегиях
    """
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)

    syncer.sync_schema(interactive=False)
    syncer.sync_data(pk_strategy=strategy)

    # Считываем результат
    with target_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, city, age FROM users ORDER BY id")
        ).mappings().all()
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

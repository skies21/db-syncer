import pytest
from sqlalchemy import text
from tests.conftest import SOURCE_URL, TARGET_URL


@pytest.mark.parametrize("strategy", ["skip", "overwrite", "merge"])
def test_api_sync(client, source_engine, target_engine, prepare_source, prepare_target, strategy):
    """
    Тестирует API POST /api/sync с разными стратегиями
    """
    payload = {
        "source_url": SOURCE_URL,
        "target_url": TARGET_URL,
        "pk_strategy": strategy
    }

    response = client.post("/api/sync", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "ok"
    assert data["schema_synced"] is True
    assert data["data_synced"] is True
    assert data["pk_strategy"] == strategy

    with target_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, city, age FROM users ORDER BY id")
        ).mappings().all()
        result_dict = {row['id']: {**row, 'age': int(row['age']) if row['age'] is not None else None} for row in rows}

    if strategy == "skip":
        assert result_dict[1]['name'] == "Alice PROD"
        assert result_dict[1]['city'] == "London PROD"
        assert result_dict[1]['age'] == 99
        assert result_dict[2]['name'] == "Bob PROD"
        assert result_dict[2]['city'] is None
        assert result_dict[2]['age'] is None

    elif strategy == "overwrite":
        assert result_dict[1]['name'] == "Alice"
        assert result_dict[1]['city'] == "London"
        assert result_dict[1]['age'] == 25
        assert result_dict[2]['name'] == "Bob"
        assert result_dict[2]['city'] == "Paris"
        assert result_dict[2]['age'] == 30

    elif strategy == "merge":
        assert result_dict[1]['name'] == "Alice PROD"
        assert result_dict[1]['city'] == "London PROD"
        assert result_dict[1]['age'] == 99
        assert result_dict[2]['name'] == "Bob PROD"
        assert result_dict[2]['city'] == "Paris"
        assert result_dict[2]['age'] == 30
        assert result_dict[3]['name'] == "Charlie"
        assert result_dict[3]['city'] == "Madrid"
        assert result_dict[3]['age'] == 30


def test_api_sync_invalid_strategy(client, prepare_source, prepare_target):
    """
    Тестируем API при передаче некорректной стратегии pk_strategy
    """
    payload = {
        "source_url": SOURCE_URL,
        "target_url": TARGET_URL,
        "pk_strategy": "invalid_strategy"
    }

    response = client.post("/api/sync", json=payload)
    assert response.status_code == 422

    errors = response.json()["detail"]
    assert any(
        error["loc"] == ["body", "pk_strategy"] and "unexpected value" in error["msg"]
        for error in errors
    )


def test_api_sync_unreachable_db(client):
    """
    Тестирует поведение API при недоступной БД
    """
    payload = {
        "source_url": "postgresql://postgres:postgres@localhost:9999/nonexistent_db",
        "target_url": "postgresql://postgres:postgres@localhost:9999/nonexistent_db",
        "pk_strategy": "skip"
    }

    response = client.post("/api/sync", json=payload)
    assert response.status_code == 500
    assert "Sync failed" in response.json()["detail"]

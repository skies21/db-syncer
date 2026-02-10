from syncer.db_syncer import DBSyncer
from tests.conftest import SOURCE_URL, TARGET_URL
import pytest
from sqlalchemy import text


@pytest.mark.parametrize(
    "strategy",
    ["skip", "overwrite", "merge"],
)
def test_sync_data_bulk_strategies(
    source_engine,
    target_engine,
    prepare_source,
    prepare_target,
    strategy,
):
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)

    plan = syncer.analyze_schema()
    syncer.apply_safe_schema_changes(plan)

    syncer.sync_data_bulk(strategy=strategy)

    with target_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id, email, name, age, city
                FROM users
                ORDER BY id
            """)
        ).mappings().all()

    result = {r["id"]: r for r in rows}

    if strategy == "skip":
        assert result[1]["name"] == "Alice PROD"
        assert result[1]["city"] == "London PROD"
        expected_ages = {1: "99", 2: None, 3: "30", 4: "18"}
        for id_, expected in expected_ages.items():
            val = result[id_]["age"]
            if val is not None:
                val = str(val)
            assert val == expected

        assert result[2]["name"] == "Bob PROD"
        assert result[2]["city"] is None
        assert result[2]["age"] is None

        assert result[3]["name"] == "Charlie"
        assert result[4]["name"] == "David PROD"


    elif strategy == "overwrite":
        assert result[1]["name"] == "Alice"
        assert result[1]["city"] == "London"

        assert result[2]["name"] == "Bob"
        assert result[2]["city"] == "Paris"

        assert result[4]["name"] == "Eve"

    elif strategy == "merge":
        # id=1 → всё заполнено → не меняется
        assert result[1]["name"] == "Alice PROD"
        assert result[1]["city"] == "London PROD"

        # id=2 → NULL → берём из source
        assert result[2]["name"] == "Bob PROD"
        assert result[2]["city"] == "Paris"
        assert result[2]["age"] == "30"

        # id=3 → только target → остаётся
        assert result[3]["name"] == "Charlie"

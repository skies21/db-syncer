from syncer.db_syncer import DBSyncer
from tests.conftest import SOURCE_URL, TARGET_URL


def test_conflict_report_detects_conflicts(
        source_engine,
        target_engine,
        prepare_source,
        prepare_target,
):
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)

    plan = syncer.analyze_schema()
    syncer.apply_safe_schema_changes(plan)

    conflicts = syncer.report_conflicts()

    assert "users" in conflicts
    conflict_ids = {c["pk"] for c in conflicts["users"]}

    assert conflict_ids == {1, 2, 3, 4}

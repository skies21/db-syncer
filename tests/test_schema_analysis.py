from syncer.db_syncer import DBSyncer
from tests.conftest import SOURCE_URL, TARGET_URL


def test_schema_analysis_detects_safe_and_manual_changes(source_engine, target_engine, prepare_source, prepare_target):
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)
    plan = syncer.analyze_schema()
    warnings = [w.message for w in plan.warnings]
    assert any("type mismatch" in w.lower() for w in warnings)
    assert any("legacy_code" in w for w in warnings)
    assert any("audit_log" in w for w in warnings)
    assert plan.create_tables == []

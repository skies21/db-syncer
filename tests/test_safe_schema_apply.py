from syncer.db_syncer import DBSyncer
from tests.conftest import SOURCE_URL, TARGET_URL


def test_apply_safe_schema_changes_only(
    source_engine,
    target_engine,
    prepare_source,
    prepare_target,
):
    syncer = DBSyncer(SOURCE_URL, TARGET_URL)
    plan = syncer.analyze_schema()

    syncer.apply_safe_schema_changes(plan)

    # обновляем metadata
    syncer.target_meta.clear()
    syncer.target_meta.reflect(bind=target_engine)

    # users: добавлены безопасные колонки
    users = syncer.target_meta.tables["users"]
    assert "last_login" in users.c
    assert "is_active" in users.c

    # legacy_code НЕ удалён
    assert "legacy_code" in users.c

    # orders создана
    assert "orders" in syncer.target_meta.tables

    # audit_log осталась (не трогаем)
    assert "audit_log" in syncer.target_meta.tables

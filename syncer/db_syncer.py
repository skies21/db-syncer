import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List

from sqlalchemy import create_engine, inspect, MetaData, Table, text, select, ForeignKeyConstraint, Index, String, Text, \
    and_
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class SchemaWarning:
    level: str
    message: str


@dataclass
class MigrationPlan:
    create_tables: List[str] = field(default_factory=list)
    add_columns: Dict[str, Dict[str, str]] = field(default_factory=dict)
    add_indexes: List[str] = field(default_factory=list)
    add_foreign_keys: List[str] = field(default_factory=list)
    add_unique_constraints: List[str] = field(default_factory=list)
    add_check_constraints: List[str] = field(default_factory=list)
    add_sequences: List[str] = field(default_factory=list)
    warnings: List[SchemaWarning] = field(default_factory=list)


class DBSyncer:
    def __init__(self, source_url: str, target_url: str):
        self.source_engine: Engine = create_engine(source_url, future=True)
        self.target_engine: Engine = create_engine(target_url, future=True)

        self.source_meta = MetaData()
        self.target_meta = MetaData()

        self.source_meta.reflect(bind=self.source_engine)
        self.target_meta.reflect(bind=self.target_engine)

        self.source_inspector = inspect(self.source_engine)
        self.target_inspector = inspect(self.target_engine)

    def analyze_schema(self) -> MigrationPlan:
        self.source_meta.reflect(bind=self.source_engine, extend_existing=True)
        self.target_meta.reflect(bind=self.target_engine, extend_existing=True)

        plan = MigrationPlan()

        source_tables = set(self.source_meta.tables)
        target_tables = set(self.target_meta.tables)

        # Новые таблицы
        for table in sorted(source_tables - target_tables):
            plan.create_tables.append(table)
            plan.warnings.append(SchemaWarning("WARNING", f"New table: {table}"))

        # Общие таблицы
        for table in sorted(source_tables & target_tables):
            source_table = self.source_meta.tables[table]
            target_table = self.target_meta.tables[table]

            source_cols = {c.name: c for c in source_table.columns}
            target_cols = {c.name: c for c in target_table.columns}

            # Колонки только в source
            for col in source_cols.keys() - target_cols.keys():
                plan.add_columns.setdefault(table, {})[col] = str(source_cols[col].type)
                plan.warnings.append(SchemaWarning("WARNING", f"Column {col} missing in target {table}"))

            # Лишние колонки в target
            for col in target_cols.keys() - source_cols.keys():
                plan.warnings.append(SchemaWarning("MANUAL", f"Extra column {col} in target table {table}"))

            # Проверка типов колонок
            for col in source_cols.keys() & target_cols.keys():
                s_type = str(source_cols[col].type)
                t_type = str(target_cols[col].type)
                if s_type != t_type:
                    plan.warnings.append(SchemaWarning("WARNING",
                                                       f"Type mismatch for {col} in table {table}: source={s_type}, target={t_type}"))

            # FK, Indexes, UNIQUE, CHECK, Sequences
            self._analyze_foreign_keys(table, plan)
            self._analyze_indexes(table, plan)
            self._analyze_unique_and_check(table, plan)
            self._analyze_sequences(table, plan)

        # Таблицы только в target
        for table in sorted(target_tables - source_tables):
            plan.warnings.append(SchemaWarning("MANUAL", f"Extra table in target DB: {table}"))

        return plan

    def _analyze_foreign_keys(self, table: str, plan: MigrationPlan) -> None:
        source_fks = self.source_inspector.get_foreign_keys(table)
        target_fks = self.target_inspector.get_foreign_keys(table)

        def fk_signature(fk):
            return tuple(fk["constrained_columns"]), fk["referred_table"], tuple(fk["referred_columns"])

        target_sigs = {fk_signature(fk) for fk in target_fks}

        for fk in source_fks:
            sig = fk_signature(fk)
            if sig not in target_sigs:
                plan.add_foreign_keys.append(
                    f"{table}: FK {fk['constrained_columns']} -> {fk['referred_table']}({fk['referred_columns']})"
                )

    def _analyze_indexes(self, table: str, plan: MigrationPlan) -> None:
        source_indexes = self.source_inspector.get_indexes(table)
        target_indexes = self.target_inspector.get_indexes(table)

        def idx_signature(idx):
            return (idx["unique"], tuple(idx["column_names"]))

        target_sigs = {idx_signature(idx) for idx in target_indexes}

        for idx in source_indexes:
            if idx_signature(idx) not in target_sigs:
                plan.add_indexes.append(f"{table}: INDEX ({idx['column_names']})")

    def _analyze_unique_and_check(self, table: str, plan: MigrationPlan) -> None:
        # UNIQUE и CHECK constraints
        uniques = self.source_inspector.get_unique_constraints(table)
        checks = self.source_inspector.get_check_constraints(table)

        for u in uniques:
            plan.add_unique_constraints.append(f"{table}: UNIQUE {u['column_names']}")

        for c in checks:
            plan.add_check_constraints.append(f"{table}: CHECK {c['sqltext']}")

    def _analyze_sequences(self, table: str, plan: MigrationPlan) -> None:
        # Проверка последовательностей
        for col in self.source_meta.tables[table].columns:
            if getattr(col, "sequence", None):
                seq_def = f"{table}: SEQUENCE {col.sequence.name}:{col.name}"
                plan.add_sequences.append(seq_def)

        if not hasattr(self, "plan_sequences"):
            self.plan_sequences = []
        self.plan_sequences.extend(plan.add_sequences)

    def apply_safe_schema_changes(self, plan: MigrationPlan) -> None:
        with self.target_engine.begin() as conn:

            # Создание таблиц
            for table_name in plan.create_tables:
                if self.target_inspector.has_table(table_name):
                    logger.info("Table %s already exists, skipping", table_name)
                    continue
                source_table = self.source_meta.tables[table_name]
                source_table.to_metadata(self.target_meta).create(conn)
                logger.info("Created table %s", table_name)

            # Добавляем колонки
            for table, cols in plan.add_columns.items():
                target_table = Table(table, self.target_meta, autoload_with=self.target_engine)
                for col_name, col_type in cols.items():
                    source_col = self.source_meta.tables[table].columns[col_name]
                    default_sql = ""
                    if source_col.default is not None:
                        if callable(source_col.default.arg):
                            default_sql = f" DEFAULT {source_col.default.arg()}"
                        else:
                            default_sql = f" DEFAULT {source_col.default.arg!r}"
                    nullable = "" if source_col.nullable else " NOT NULL"
                    sql = f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}{nullable}{default_sql}'
                    try:
                        conn.execute(text(sql))
                        logger.info("Added column %s.%s with default=%s", table, col_name, source_col.default.arg)
                    except Exception as e:
                        logger.warning("Failed to add column %s.%s: %s", table, col_name, e)

            # FK
            for fk_def in plan.add_foreign_keys:
                table_name, rest = fk_def.split(": FK ")
                constrained_cols, ref_part = rest.split(" -> ")
                constrained_cols = eval(constrained_cols)
                ref_table, ref_cols = ref_part.split("(")
                ref_cols = eval(ref_cols[:-1])
                target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)
                fk = ForeignKeyConstraint(constrained_cols, [f"{ref_table}.{c}" for c in ref_cols])
                try:
                    fk.create(target_table, connection=conn)
                    logger.info("Created FK %s(%s) -> %s(%s)", table_name, constrained_cols, ref_table, ref_cols)
                except Exception as e:
                    logger.warning("Failed FK %s: %s", fk_def, e)

            # Индексы
            for idx_def in plan.add_indexes:
                table_name, rest = idx_def.split(": INDEX ")
                columns = eval(rest)
                target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)
                index_name = f"idx_{table_name}_{'_'.join(columns)}"
                idx = Index(index_name, *[target_table.c[c] for c in columns])
                try:
                    idx.create(conn)
                    logger.info("Created index %s on %s(%s)", index_name, table_name, columns)
                except Exception as e:
                    logger.warning("Failed index %s: %s", idx_def, e)

            # Unique
            for u_def in plan.add_unique_constraints:
                table_name, rest = u_def.split(": UNIQUE ")
                columns = eval(rest)
                target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)
                try:
                    uc_name = f"uniq_{table_name}_{'_'.join(columns)}"
                    conn.execute(
                        text(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{uc_name}" UNIQUE ({", ".join(columns)})'))
                    logger.info("Created UNIQUE constraint %s on %s", uc_name, table_name)
                except Exception as e:
                    logger.warning("Failed UNIQUE %s: %s", u_def, e)

            for c_def in plan.add_check_constraints:
                table_name, rest = c_def.split(": CHECK ")
                target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)
                try:
                    check_name = f"chk_{table_name}_{abs(hash(rest))}"  # уникальное имя
                    conn.execute(text(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{check_name}" CHECK ({rest})'))
                    logger.info("Created CHECK %s on %s", check_name, table_name)
                except Exception as e:
                    logger.warning("Failed CHECK %s: %s", c_def, e)

            # Sequences
            for seq_def in plan.add_sequences:
                table_name, seq_name, col_name = seq_def.split(": SEQUENCE ")
                col_name = col_name.strip()
                try:
                    conn.execute(text(f'CREATE SEQUENCE IF NOT EXISTS {seq_name}'))

                    result_source = self.source_engine.execute(
                        text(f"SELECT last_value FROM {seq_name}")
                    ).scalar()

                    result_target_max = conn.execute(
                        text(f"SELECT COALESCE(MAX({col_name}), 0) FROM {table_name}")
                    ).scalar()

                    new_val = max(result_source, result_target_max + 1)
                    conn.execute(text(f'SELECT setval(\'{seq_name}\', {new_val}, true)'))

                    logger.info("Sequence %s set to current value %s", seq_name, new_val)
                except Exception as e:
                    logger.warning("Failed sequence %s: %s", seq_name, e)

        # Обновление метаданных target
        self.target_meta.reflect(bind=self.target_engine, extend_existing=True)

        # Сбор sequences для sync_data_bulk
        if not hasattr(self, "plan_sequences"):
            self.plan_sequences = []
        self.plan_sequences.extend(plan.add_sequences)

    def sync_data_bulk(self, strategy: str = "skip", batch_size: int = 1000,
                       create_missing_columns: bool = True) -> None:
        """
        Синхронизация данных source -> target с учётом FK
        """
        logger.info(
            "Starting bulk data sync: strategy=%s batch_size=%d create_missing_columns=%s",
            strategy,
            batch_size,
            create_missing_columns,
        )

        table_order, cyclic_tables = self._sort_tables_by_fk_safe()

        for table_name in table_order:
            source_table = self.source_meta.tables[table_name]

            if table_name not in self.target_meta.tables:
                continue

            target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)

            pk_cols = list(source_table.primary_key.columns)
            if not pk_cols:
                continue
            pk_names = [c.name for c in pk_cols]

            source_cols = set(source_table.columns.keys())
            target_cols = set(target_table.columns.keys())
            missing_cols = (source_cols - target_cols) if create_missing_columns else set()

            with self.source_engine.connect() as src_conn, self.target_engine.begin() as tgt_conn:

                # Добавление недостающих колонок
                for col_name in missing_cols:
                    col_obj = source_table.c[col_name]
                    col_type = col_obj.type.compile(dialect=self.target_engine.dialect)
                    nullable = "" if col_obj.nullable else " NOT NULL"
                    default_sql = ""
                    if col_obj.server_default is not None:
                        default_sql = f" DEFAULT {col_obj.server_default.arg.text}"
                    elif col_obj.default is not None:
                        default_sql = f" DEFAULT {col_obj.default.arg() if callable(col_obj.default.arg) else col_obj.default.arg!r}"
                    sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {col_type}{nullable}{default_sql}'
                    try:
                        tgt_conn.execute(text(sql))
                        logger.info("Added column %s.%s with default=%s", table_name, col_name,
                                    col_obj.default or col_obj.server_default)
                    except Exception as e:
                        logger.warning("Failed to add column %s.%s: %s", table_name, col_name, e)

                # Обновляем target_table после добавления колонок
                target_table = Table(table_name, self.target_meta, autoload_with=self.target_engine)
                all_cols = source_cols & set(target_table.columns.keys())

                # Если таблица в цикле FK — временно отключаем триггеры
                disable_fk = table_name in cyclic_tables
                if disable_fk:
                    tgt_conn.execute(text(f'ALTER TABLE "{table_name}" DISABLE TRIGGER ALL'))

                offset = 0
                while True:
                    rows = src_conn.execute(
                        select(source_table)
                        .order_by(*pk_cols)
                        .offset(offset)
                        .limit(batch_size)
                    ).mappings().all()

                    if not rows:
                        break

                    for row in rows:
                        pk_value = tuple(row[pk] for pk in pk_names) if len(pk_names) > 1 else row[pk_names[0]]
                        where_clause = [target_table.c[pk] == row[pk] for pk in pk_names]

                        existing = tgt_conn.execute(
                            select(target_table).where(and_(*where_clause))
                        ).mappings().fetchone()

                        row_data = {}
                        for col in all_cols:
                            val = row[col] if col in row else None
                            if col in target_table.c:
                                col_type = target_table.c[col].type
                                if isinstance(col_type, (String, Text)):
                                    val = str(val) if val is not None else None
                            row_data[col] = val

                        if existing:
                            if strategy == "skip":
                                continue
                            elif strategy == "overwrite":
                                update_data = {k: v for k, v in row_data.items() if k not in pk_names}
                                tgt_conn.execute(
                                    target_table.update().where(and_(*where_clause)).values(**update_data)
                                )
                            elif strategy == "merge":
                                update_data = {}
                                for k, v in row_data.items():
                                    if k in pk_names:
                                        continue
                                    existing_val = existing.get(k)
                                    if existing_val is None or str(existing_val) == "":
                                        update_data[k] = v
                                if update_data:
                                    tgt_conn.execute(
                                        target_table.update().where(and_(*where_clause)).values(**update_data)
                                    )
                        else:
                            tgt_conn.execute(target_table.insert().values(**row_data))

                    offset += batch_size

                if disable_fk:
                    tgt_conn.execute(text(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL'))

        for seq_def in getattr(self, "plan_sequences", []):
            # seq_def = "table: SEQUENCE sequence_name:column_name"
            table_name, seq_name_col = seq_def.split(": SEQUENCE ")
            seq_name, col_name = seq_name_col.split(":")
            col_name = col_name.strip()
            try:
                tgt_conn.execute(text(f'CREATE SEQUENCE IF NOT EXISTS {seq_name}'))

                result_source = self.source_engine.execute(
                    text(f"SELECT last_value FROM {seq_name}")
                ).scalar()

                result_target_max = tgt_conn.execute(
                    text(f"SELECT COALESCE(MAX({col_name}), 0) FROM {table_name}")
                ).scalar()

                new_val = max(result_source, result_target_max + 1)
                tgt_conn.execute(text(f'SELECT setval(\'{seq_name}\', {new_val}, true)'))

                logger.info("Sequence %s set to current value %s", seq_name, new_val)
            except Exception as e:
                logger.warning("Failed sequence %s: %s", seq_name, e)

    def _sort_tables_by_fk_safe(self) -> tuple[list[str], list[str]]:
        """
        Возвращает два списка:
        1. sorted_tables: безопасный порядок вставки (без циклов)
        2. cyclic_tables: таблицы, участвующие в циклах
        """

        tables = list(self.source_meta.tables.keys())
        graph = defaultdict(set)
        for t in tables:
            fks = self.source_inspector.get_foreign_keys(t)
            for fk in fks:
                ref = fk["referred_table"]
                if ref in tables:
                    graph[t].add(ref)

        sorted_tables = []
        visited = set()
        temp_mark = set()
        cycles = set()

        def visit(node):
            if node in visited:
                return
            if node in temp_mark:
                cycles.add(node)
                return
            temp_mark.add(node)
            for dep in graph[node]:
                visit(dep)
            temp_mark.remove(node)
            visited.add(node)
            sorted_tables.append(node)

        for t in tables:
            visit(t)

        cyclic_tables = list(cycles)
        return sorted_tables, cyclic_tables

    def report_conflicts(self) -> dict:
        """
        Отчёт о конфликтах (read only) с поддержкой составных PK и циклических FK
        """
        conflicts: dict[str, list[dict]] = {}

        table_order, cyclic_tables = self._sort_tables_by_fk_safe()

        for table_name in table_order + cyclic_tables:
            source_table = self.source_meta.tables[table_name]
            if table_name not in self.target_meta.tables:
                continue

            target_table = Table(
                table_name,
                self.target_meta,
                autoload_with=self.target_engine,
            )

            pk_cols = list(source_table.primary_key.columns)
            if not pk_cols:
                continue
            pk_names = [str(c.name) for c in pk_cols]

            all_cols = set(source_table.columns.keys()) | set(target_table.columns.keys())

            with self.source_engine.connect() as src, self.target_engine.connect() as tgt:
                src_rows = {
                    tuple(row._mapping[pk] for pk in pk_names) if len(pk_names) > 1 else row._mapping[
                        pk_names[0]]: dict(row._mapping)
                    for row in src.execute(select(source_table))
                }
                tgt_rows = {
                    tuple(row._mapping[pk] for pk in pk_names) if len(pk_names) > 1 else row._mapping[
                        pk_names[0]]: dict(row._mapping)
                    for row in tgt.execute(select(target_table))
                }

                all_pks = set(src_rows.keys()) | set(tgt_rows.keys())
                table_conflicts = []

                for pk_value in all_pks:
                    src_row = src_rows.get(pk_value, {})
                    tgt_row = tgt_rows.get(pk_value, {})

                    diffs = {}
                    for col in all_cols:
                        src_val = str(src_row[col]) if col in src_row else None
                        tgt_val = str(tgt_row[col]) if col in tgt_row else None
                        if src_val != tgt_val:
                            diffs[col] = (src_val, tgt_val)

                    if diffs:
                        table_conflicts.append({"pk": pk_value, "diffs": diffs})

                if table_conflicts:
                    conflicts[table_name] = table_conflicts

        return conflicts

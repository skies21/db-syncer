import logging
from typing import Dict, List

from sqlalchemy import create_engine, inspect, MetaData, Table, text, select, insert, update
from sqlalchemy.engine import Engine
from sqlalchemy.sql.schema import Column

logger = logging.getLogger(__name__)


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

    def diff_schema(self) -> Dict:
        """
        Сравнивает схемы БД и возвращает список изменений
        """
        changes = {
            "tables_to_add": [],
            "tables_to_drop": [],
            "columns_to_add": {},
            "columns_to_modify": {},
            "columns_to_drop": {},
        }

        source_tables = set(self.source_meta.tables)
        target_tables = set(self.target_meta.tables)

        changes["tables_to_add"] = sorted(source_tables - target_tables)
        changes["tables_to_drop"] = sorted(target_tables - source_tables)

        for table_name in source_tables & target_tables:
            source_table = self.source_meta.tables[table_name]

            source_cols = {c.name: c.type for c in source_table.columns}
            target_cols = {
                c["name"]: c["type"]
                for c in self.target_inspector.get_columns(table_name)
            }

            add_cols = {
                name: col_type
                for name, col_type in source_cols.items()
                if name not in target_cols
            }

            modify_cols = {
                name: col_type
                for name, col_type in source_cols.items()
                if name in target_cols
                and str(col_type) != str(target_cols[name])
            }

            drop_cols = [
                name for name in target_cols if name not in source_cols
            ]

            if add_cols:
                changes["columns_to_add"][table_name] = add_cols
            if modify_cols:
                changes["columns_to_modify"][table_name] = modify_cols
            if drop_cols:
                changes["columns_to_drop"][table_name] = drop_cols

        return changes

    def sync_schema(self, interactive: bool = True) -> None:
        changes = self.diff_schema()

        for table_name in changes["tables_to_add"]:
            source_table = self.source_meta.tables[table_name]

            new_table = Table(
                source_table.name,
                self.target_meta,
                *[
                    Column(
                        c.name,
                        c.type,
                        primary_key=c.primary_key,
                        nullable=c.nullable,
                    )
                    for c in source_table.columns
                ],
            )

            new_table.create(self.target_engine)
            logging.info(f"Создана таблица {table_name}")

        # Меняем существующие таблицы
        with self.target_engine.begin() as conn:

            # Добавление колонок
            for table, cols in changes["columns_to_add"].items():
                for col_name, col_type in cols.items():
                    conn.execute(
                        text(
                            f'ALTER TABLE "{table}" '
                            f'ADD COLUMN "{col_name}" {col_type}'
                        )
                    )

            # Изменение типов колонок
            for table, cols in changes["columns_to_modify"].items():
                for col_name, col_type in cols.items():
                    conn.execute(
                        text(
                            f'ALTER TABLE "{table}" '
                            f'ALTER COLUMN "{col_name}" TYPE {col_type}'
                        )
                    )

            # Удаление колонок
            for table, cols in changes["columns_to_drop"].items():
                for col_name in cols:
                    if interactive:
                        confirm = input(
                            f"Удалить колонку {table}.{col_name}? [y/N]: "
                        )
                        if confirm.lower() != "y":
                            continue

                    conn.execute(
                        text(
                            f'ALTER TABLE "{table}" '
                            f'DROP COLUMN "{col_name}"'
                        )
                    )

    def sync_data(self, pk_strategy: str = "skip") -> None:
        for table_name, source_table in self.source_meta.tables.items():
            if table_name not in self.target_meta.tables:
                continue

            target_table = Table(
                table_name,
                self.target_meta,
                autoload_with=self.target_engine,
            )

            pk_column = list(source_table.primary_key.columns)[0]

            # Получаем список колонок, которые есть в target
            target_columns = set(target_table.c.keys())

            with self.source_engine.connect() as src_conn, \
                    self.target_engine.begin() as tgt_conn:

                for row in src_conn.execute(select(source_table)):
                    row_data = dict(row._mapping)
                    pk_value = row_data.get(pk_column.name)

                    if pk_value is None:
                        continue

                    # Оставляем только колонки, которые есть в target
                    row_data_filtered = {
                        k: v for k, v in row_data.items()
                        if k in target_columns
                    }

                    pk_condition = (
                        target_table.c[pk_column.name] == pk_value
                    )

                    existing = tgt_conn.execute(
                        select(target_table).where(pk_condition)
                    ).mappings().fetchone()

                    if existing:
                        if pk_strategy == "skip":
                            continue

                        elif pk_strategy == "overwrite":
                            # обновляем только существующие колонки
                            update_data = {
                                k: v for k, v in row_data_filtered.items()
                                if k != pk_column.name
                            }
                            if update_data:
                                tgt_conn.execute(
                                    update(target_table)
                                    .where(pk_condition)
                                    .values(**update_data)
                                )

                        elif pk_strategy == "merge":
                            update_data = {
                                k: v
                                for k, v in row_data_filtered.items()
                                if existing[k] is None
                            }
                            if update_data:
                                tgt_conn.execute(
                                    update(target_table)
                                    .where(pk_condition)
                                    .values(**update_data)
                                )
                    else:
                        tgt_conn.execute(
                            insert(target_table)
                            .values(**row_data_filtered)
                        )

    def get_conflicts(self) -> Dict[str, List[Dict]]:
        conflicts: Dict[str, List[Dict]] = {}

        for table_name, source_table in self.source_meta.tables.items():
            if table_name not in self.target_meta.tables:
                continue

            target_table = Table(
                table_name,
                self.target_meta,
                autoload_with=self.target_engine,
            )

            pk_column = list(source_table.primary_key.columns)[0]

            with self.source_engine.connect() as src_conn, \
                    self.target_engine.connect() as tgt_conn:

                table_conflicts = []

                for row in src_conn.execute(select(source_table)):
                    row_data = dict(row._mapping)

                    existing = tgt_conn.execute(
                        select(target_table).where(
                            target_table.c[pk_column.name]
                            == row_data[pk_column.name]
                        )
                    ).fetchone()

                    if existing:
                        existing_data = dict(existing._mapping)
                        if row_data != existing_data:
                            table_conflicts.append(
                                {
                                    "pk": row_data[pk_column.name],
                                    "source_data": row_data,
                                    "target_data": existing_data,
                                }
                            )

                if table_conflicts:
                    conflicts[table_name] = table_conflicts

        return conflicts

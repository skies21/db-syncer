import logging
from pathlib import Path
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Table
from starlette.status import HTTP_400_BAD_REQUEST

from syncer.logging_config import setup_logging
from syncer.db_syncer import DBSyncer

app = FastAPI()

setup_logging()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

templates = Jinja2Templates(
    directory=str(BASE_DIR / "templates")
)

# Хранилища состояний
pending_syncers = {}
pending_diffs = {}
pending_conflicts: dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/diff", response_class=HTMLResponse)
async def get_diff(request: Request, source_url: str = Form(...), target_url: str = Form(...),
                   pk_strategy: str = Form("skip")):
    syncer = DBSyncer(source_url, target_url)

    diff = syncer.diff_schema()

    pending_syncers[target_url] = syncer

    return templates.TemplateResponse(
        "diff.html",
        {
            "request": request,
            "diff": diff,
            "source_url": source_url,
            "target_url": target_url,
            "pk_strategy": pk_strategy,
        },
    )


@app.post("/confirm_batch", response_class=HTMLResponse)
async def confirm_batch(request: Request, tables: list[str] = Form([]),
                        columns: list[str] = Form([]), source_url: str = Form(...),
                        target_url: str = Form(...), pk_strategy: str = Form("skip")):
    logger.info("Batch confirm requested for target DB: %s", target_url)
    logger.info("Tables to drop: %s, Columns to drop: %s", tables, columns)

    syncer = pending_syncers.get(target_url)
    if not syncer:
        logger.warning("No active syncer found for target DB: %s", target_url)
        return HTMLResponse("No active syncer.", status_code=404)

    try:
        with syncer.target_engine.begin() as conn:
            for table in tables:
                logger.info("Dropping table '%s' from target DB: %s", table, target_url)
                conn.execute(f"DROP TABLE {table} CASCADE")
                logger.debug("Table '%s' dropped successfully", table)

            for col in columns:
                table_name, col_name = col.split(".")
                logger.info(
                    "Dropping column '%s' from table '%s' in target DB: %s",
                    col_name,
                    table_name,
                    target_url,
                )
                conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")
                logger.debug(
                    "Column '%s' dropped successfully from table '%s'", col_name, table_name
                )

        diff = syncer.diff_schema()
        pending_diffs[target_url] = diff
        logger.info(
            "Batch deletion applied successfully. Updated schema diff stored for target DB: %s",
            target_url,
        )

        return templates.TemplateResponse(
            "diff.html",
            {
                "request": request,
                "diff": diff,
                "source_url": source_url,
                "target_url": target_url,
                "pk_strategy": pk_strategy
            },
        )

    except Exception as exc:
        logger.exception(
            "Error during batch confirm for target DB: %s", target_url
        )
        return HTMLResponse(
            "<div class='alert alert-danger'>Error during batch deletion. Check logs.</div>",
            status_code=500,
        )


@app.post("/sync", response_class=HTMLResponse)
async def run_sync(request: Request, source_url: str = Form(...), target_url: str = Form(...),
                   pk_strategy: str = Form("skip")):
    logger.info("Sync requested for target DB: %s", target_url)

    syncer = pending_syncers.get(target_url)
    if not syncer:
        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Нет активной сессии синхронизации для этой БД.",
            },
            status_code=404,
        )

    try:
        syncer.sync_schema(interactive=False)

        syncer.target_meta.clear()
        syncer.target_meta.reflect(bind=syncer.target_engine)

        syncer.sync_data(pk_strategy=pk_strategy)

        return templates.TemplateResponse(
            "sync_result.html",
            {
                "request": request,
                "source_url": source_url,
                "target_url": target_url,
                "pk_strategy": pk_strategy,
            },
        )

    except Exception as exc:
        logger.exception("Sync failed for %s", target_url)

        return templates.TemplateResponse(
            "partials/alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Ошибка во время синхронизации. Проверьте логи.",
            },
            status_code=500,
        )


@app.get("/conflicts", response_class=HTMLResponse)
async def view_conflicts(request: Request, source_url: str = Query(...), target_url: str = Query(...),
                         pk_strategy: str = Query("skip")):
    logger.info("Conflicts view requested for target_url=%s", target_url)

    syncer = pending_syncers.get(target_url)
    if not syncer:
        logger.warning(
            "Conflicts requested but no active sync session found for target_url=%s",
            target_url,
        )
        return HTMLResponse("No active sync session", status_code=404)

    try:
        conflicts = syncer.get_conflicts()
        pending_conflicts[target_url] = conflicts

        logger.info(
            "Conflicts loaded for target_url=%s, total_conflicts=%d",
            target_url,
            len(conflicts),
        )

    except Exception:
        logger.exception(
            "Failed to load conflicts for target_url=%s",
            target_url,
        )
        return HTMLResponse("Failed to load conflicts", status_code=500)

    return templates.TemplateResponse(
        "conflicts.html",
        {
            "request": request,
            "conflicts": conflicts,
            "source_url": source_url,
            "target_url": target_url,
            "pk_strategy": pk_strategy,
        },
    )


@app.post("/resolve_conflicts", response_class=HTMLResponse)
async def resolve_conflicts(request: Request, source_url: str = Form(...), target_url: str = Form(...)):
    logger.info("Resolving conflicts for target DB: %s", target_url)

    syncer = pending_syncers.get(target_url)
    conflicts = pending_conflicts.get(target_url)

    if not syncer:
        logger.warning("No active syncer found for target DB: %s", target_url)
        return HTMLResponse(
            "<div class='alert alert-danger'>Sync session expired. Please restart.</div>",
            status_code=HTTP_400_BAD_REQUEST,
        )

    if not conflicts:
        logger.info("No conflicts found for target DB: %s", target_url)
        diff = syncer.diff_schema()
        return templates.TemplateResponse(
            "diff.html",
            {
                "request": request,
                "diff": diff,
                "source_url": source_url,
                "target_url": target_url,
                "message": "No data conflicts found",
            },
        )

    logger.info(
        "Found %d tables with conflicts for target DB: %s",
        len(conflicts),
        target_url,
    )

    form = await request.form()
    global_strategy = form.get("pk_strategy", "skip")

    with syncer.target_engine.begin() as conn:
        for table, records in conflicts.items():
            target_table = Table(
                table,
                syncer.target_meta,
                autoload_with=syncer.target_engine,
            )
            target_columns = set(target_table.c.keys())
            pk_col = list(syncer.source_meta.tables[table].primary_key.columns)[0].name

            for rec in records:
                pk_value = rec.get("pk")
                source_data = rec.get("source_data")
                target_data = rec.get("target_data")

                if not source_data:
                    continue

                source_data_filtered = {k: v for k, v in source_data.items() if k in target_columns}

                strategy = form.get(f"strategy_{table}_{pk_value}", global_strategy)

                if strategy == "skip":
                    continue
                elif strategy == "overwrite":
                    stmt = target_table.update().where(target_table.c[pk_col] == pk_value).values(
                        **source_data_filtered)
                    conn.execute(stmt)
                elif strategy == "merge":
                    update_data = {k: v for k, v in source_data_filtered.items() if not target_data.get(k)}
                    if update_data:
                        stmt = target_table.update().where(target_table.c[pk_col] == pk_value).values(**update_data)
                        conn.execute(stmt)

    diff = syncer.diff_schema()
    logger.info("Conflict resolution finished for target DB: %s", target_url)

    return templates.TemplateResponse(
        "diff.html",
        {
            "request": request,
            "diff": diff,
            "source_url": source_url,
            "target_url": target_url,
        },
    )

import logging
from fastapi import Request, Form, Query, APIRouter
from fastapi.responses import HTMLResponse
from sqlalchemy import inspect
from starlette.templating import Jinja2Templates
from pathlib import Path

from syncer.db_syncer import DBSyncer

router = APIRouter()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

templates = Jinja2Templates(
    directory=str(BASE_DIR / "templates")
)

# Хранилища состояний
pending_syncers = {}
pending_diffs = {}
pending_plans = {}
pending_conflicts: dict[str, dict] = {}


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.post("/diff", response_class=HTMLResponse)
async def get_diff(request: Request, source_url: str = Form(...), target_url: str = Form(...),
                   pk_strategy: str = Form("skip")):
    syncer = DBSyncer(source_url, target_url)

    plan = syncer.analyze_schema()

    pending_syncers[target_url] = syncer
    pending_plans[target_url] = plan

    return templates.TemplateResponse(
        "diff.html",
        {
            "request": request,
            "plan": plan,
            "source_url": source_url,
            "target_url": target_url,
            "pk_strategy": pk_strategy,
        },
    )


@router.post("/confirm_batch", response_class=HTMLResponse)
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
            "Batch deletion routerlied successfully. Updated schema diff stored for target DB: %s",
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


@router.post("/sync_data", response_class=HTMLResponse)
async def run_sync(request: Request, source_url: str = Form(...), target_url: str = Form(...),
                   pk_strategy: str = Form("skip")):
    logger.info("Sync requested for target DB: %s", target_url)

    syncer = pending_syncers.get(target_url)
    if not syncer:
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Нет активной сессии синхронизации для этой БД.",
            },
            status_code=404,
        )

    try:
        # Обновляем метаданные target перед синхронизацией
        syncer.target_meta.reflect(bind=syncer.target_engine, extend_existing=True)
        # Добавляем все недостающие колонки
        syncer.sync_data_bulk(strategy=pk_strategy, create_missing_columns=True)

    except Exception as e:
        logger.exception("Data sync failed")
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": f"Ошибка при синхронизации данных: {e}",
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        "sync_result.html",
        {
            "request": request,
            "source_url": source_url,
            "target_url": target_url,
            "pk_strategy": pk_strategy,
        },
    )


@router.get("/conflicts", response_class=HTMLResponse)
async def view_conflicts(request: Request, source_url: str = Query(...), target_url: str = Query(...), ):
    logger.info("Conflict report requested for %s", target_url)

    syncer = pending_syncers.get(target_url)
    if not syncer:
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Нет активной сессии синхронизации.",
            },
            status_code=404,
        )

    try:
        conflicts = syncer.report_conflicts()

    except Exception:
        logger.exception("Failed to generate conflict report")
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Ошибка при анализе конфликтов.",
            },
            status_code=500,
        )

    return templates.TemplateResponse(
        "conflicts.html",
        {
            "request": request,
            "conflicts": conflicts,
            "source_url": source_url,
            "target_url": target_url,
        },
    )


@router.post("/confirm_schema", response_class=HTMLResponse)
async def confirm_schema(request: Request, source_url: str = Form(...), target_url: str = Form(...), ):
    logger.info("[confirm_schema] start target=%s", target_url)

    syncer = pending_syncers.get(target_url)
    plan = pending_plans.get(target_url)

    if not syncer or not plan:
        logger.warning("[confirm_schema] no active plan for %s", target_url)
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Нет активного плана миграции.",
            },
            status_code=404,
        )

    try:
        logger.info(
            "[confirm_schema] applying schema changes: tables=%s, columns=%s",
            plan.create_tables,
            plan.add_columns,
        )

        syncer.apply_safe_schema_changes(plan)
        logger.info("[confirm_schema] schema changes applied")

        logger.info("[confirm_schema] refreshing metadata")
        syncer.target_meta.clear()
        syncer.target_inspector = inspect(syncer.target_engine)
        syncer.target_meta.reflect(bind=syncer.target_engine)
        logger.info("[confirm_schema] metadata refreshed")

        logger.info("[confirm_schema] re-analyzing schema")
        new_plan = syncer.analyze_schema()
        pending_plans[target_url] = new_plan
        logger.info("[confirm_schema] re-analysis complete")

        return templates.TemplateResponse(
            "diff.html",
            {
                "request": request,
                "plan": new_plan,
                "source_url": source_url,
                "target_url": target_url,
                "message": "Безопасные изменения схемы применены",
            },
        )

    except Exception as exc:
        logger.exception(
            "[confirm_schema] FAILED target=%s plan=%s",
            target_url,
            plan,
        )
        print(exc)
        return templates.TemplateResponse(
            "alert.html",
            {
                "request": request,
                "type": "danger",
                "message": "Ошибка при применении схемы.",
            },
            status_code=500,
        )

from fastapi import APIRouter, HTTPException
from api.schemas.sync import SyncRequest
from syncer.db_syncer import DBSyncer
import logging

router = APIRouter(prefix="/api", tags=["api"])
logger = logging.getLogger(__name__)


@router.post("/sync")
def sync_databases(payload: SyncRequest):
    logger.info(
        "API sync requested: %s -> %s (strategy=%s)",
        payload.source_url,
        payload.target_url,
        payload.pk_strategy,
    )

    try:
        syncer = DBSyncer(
            source_url=str(payload.source_url),
            target_url=str(payload.target_url),
        )

        syncer.sync_schema(interactive=False)
        syncer.target_meta.clear()
        syncer.target_meta.reflect(bind=syncer.target_engine)
        syncer.sync_data(pk_strategy=payload.pk_strategy)

        return {
            "status": "ok",
            "schema_synced": True,
            "data_synced": True,
            "pk_strategy": payload.pk_strategy,
        }

    except Exception as exc:
        logger.exception("API sync failed")
        raise HTTPException(
            status_code=500,
            detail="Sync failed. Check logs.",
        )

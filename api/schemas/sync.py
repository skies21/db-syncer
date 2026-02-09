from pydantic import BaseModel, AnyUrl
from typing import Literal

class SyncRequest(BaseModel):
    source_url: AnyUrl
    target_url: AnyUrl
    pk_strategy: Literal["skip", "overwrite", "merge"] = "skip"

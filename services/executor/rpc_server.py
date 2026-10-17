from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import text

from config import PRIVATE_DOMAIN_ID
from executor import engine, handle_read, handle_write, log, handle_noop


app = FastAPI()


class ExecuteReadReq(BaseModel):
    request_id: str
    domain_id: str = PRIVATE_DOMAIN_ID
    type: str = "read"
    target_spec: str
    priority: int = 0
    reuse_enabled: bool = True


class ExecuteWriteReq(BaseModel):
    request_id: str
    domain_id: str = PRIVATE_DOMAIN_ID
    type: str = "write"
    upload_id: str
    target_spec: str
    priority: int = 0
    reuse_enabled: bool = True


@app.get("/health")
def health():
    return {"ok": True}

def get_request_state(request_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT state FROM requests WHERE request_id=:id"),
            {"id": request_id},
        ).mappings().first()
    return row["state"] if row else "UNKNOWN"


@app.post("/execute/read")
def execute_read(req: ExecuteReadReq):
    payload = req.dict()
    payload["type"] = "read"
    log(f"rpc read request_id={req.request_id}")
    artifact = handle_read(payload)
    if not artifact:
        return {
            "request_id": req.request_id,
            "domain_id": req.domain_id,
            "state": get_request_state(req.request_id),
            "priority": req.priority,
            "reuse_enabled": req.reuse_enabled,
        }
    return {
        "request_id": req.request_id,
        "domain_id": req.domain_id,
        "state": "RELEASED",
        "artifact": artifact,
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }


@app.post("/execute/write")
def execute_write(req: ExecuteWriteReq):
    payload = req.dict()
    payload["type"] = "write"
    log(f"rpc write request_id={req.request_id}")
    if not handle_write(payload):
        return {
            "request_id": req.request_id,
            "domain_id": req.domain_id,
            "state": get_request_state(req.request_id),
            "priority": req.priority,
            "reuse_enabled": req.reuse_enabled,
        }
    return {
        "request_id": req.request_id,
        "domain_id": req.domain_id,
        "state": "COMMITTED",
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }

from fastapi import FastAPI, HTTPException, Request, Response

from config import PRIVATE_BASE


app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/storage/exports/{target}")
def get_export(target: str):
    path = PRIVATE_BASE / "exports" / f"{target}.txt"
    if not path.exists():
        raise HTTPException(status_code=404, detail="export not found")
    return Response(path.read_bytes(), media_type="application/octet-stream")


@app.put("/storage/exports/{target}/{upload_id}")
async def put_export(target: str, upload_id: str, request: Request):
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="empty payload")

    dst_dir = PRIVATE_BASE / "exports"
    dst_dir.mkdir(parents=True, exist_ok=True)
    path = dst_dir / f"{target}_{upload_id}.file"
    path.write_bytes(data)
    return {"ok": True, "path": str(path)}

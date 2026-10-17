import os
import requests

api = os.getenv("CONTROL_API", "http://127.0.0.1:8000")

r = requests.post(
    f"{api}/uploads/init",
    json={
        "requester_id": "external-user-1",
        "content_type": "application/octet-stream",
        "size_bytes": 12,
        "filename": "labelB"
    },
    timeout=30,
)
data = r.json()
print("init:", data)

put_resp = requests.put(
    data["upload_url"],
    data=b"new_labels_v1",
    headers={"Content-Type": "application/octet-stream"},
    timeout=30,
)
print("upload status:", put_resp.status_code)

r2 = requests.post(
    f"{api}/requests/write",
    json={
        "requester_id": "external-user-1",
        "upload_id": data["upload_id"],
        "target_spec": "studyC_labels",
        "priority": 0,
        "reuse_enabled": True,
    },
    timeout=30,
)
print("write:", r2.json())

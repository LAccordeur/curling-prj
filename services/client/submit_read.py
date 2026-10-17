import os
import requests

api = os.getenv("CONTROL_API", "http://127.0.0.1:8000")
resp = requests.post(
    f"{api}/requests/read",
    json={
        "requester_id": "external-user-1",
        "target_spec": "studyA",
        "priority": 0,
        "reuse_enabled": True,
    },
    timeout=30,
)
print(resp.json())

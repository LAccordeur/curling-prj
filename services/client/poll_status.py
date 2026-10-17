import os
import sys
import requests

api = os.getenv("CONTROL_API", "http://127.0.0.1:8000")
request_id = sys.argv[1]
resp = requests.get(f"{api}/requests/{request_id}", timeout=30)
print(resp.json())

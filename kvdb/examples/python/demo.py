"""Use a running kvdb server from Python.

    pip install redis
    go run ./cmd/kvserver            # in another terminal
    python examples/python/demo.py

If the server was started with a password, set KVDB_PASSWORD before running this.
"""
import os

import redis

db = redis.Redis(
    host=os.environ.get("KVDB_HOST", "127.0.0.1"),
    port=int(os.environ.get("KVDB_PORT", "6380")),
    password=os.environ.get("KVDB_PASSWORD"),
    decode_responses=True,  # return str instead of bytes
)

db.set("project", "kvdb")
print("project =", db.get("project"))

# Counters are atomic, even with many clients at once.
print("visits  =", db.incr("visits"))

# Store structured data as JSON text.
import json

db.set("user:1", json.dumps({"name": "Vijay", "city": "Chennai"}))
print("user:1  =", json.loads(db.get("user:1")))

print("keys    =", sorted(db.keys("*")))
info = db.info()
print(f"{info['keys']} keys, file is {info['file_size_bytes']} bytes, {info['dead_bytes']} wasted")

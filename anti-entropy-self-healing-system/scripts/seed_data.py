#!/usr/bin/env python3
"""Seed storage nodes with initial test data via the coordinator."""
import json
import sys
import time
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5050"


def seed():
    print(f"Seeding data via {BASE_URL}...")
    for i in range(20):
        key = f"log-{i:03d}"
        value = f"log-entry-{i}-{int(time.time())}"
        body = json.dumps({"value": value}).encode()
        req = urllib.request.Request(f"{BASE_URL}/api/data/{key}", data=body, method="PUT")
        req.add_header("Content-Type", "application/json")
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            if resp.status == 200:
                print(f"  Written: {key}")
            else:
                print(f"  FAILED: {key} ({resp.status})")
        except Exception as e:
            print(f"  FAILED: {key} ({e})")
    print("Seeding complete.")


if __name__ == "__main__":
    seed()

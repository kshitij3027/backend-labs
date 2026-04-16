"""End-to-end verification script for the inverted index search engine."""

import os
import sys
import time

import httpx


BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
PASSED = 0
FAILED = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASSED, FAILED
    if condition:
        PASSED += 1
        print(f"  PASS: {name}")
    else:
        FAILED += 1
        print(f"  FAIL: {name} -- {detail}")


def main():
    global PASSED, FAILED
    client = httpx.Client(base_url=BACKEND_URL, timeout=30.0)

    # Wait for backend to be healthy
    print("Waiting for backend...")
    for i in range(30):
        try:
            r = client.get("/health")
            if r.status_code == 200:
                break
        except httpx.ConnectError:
            pass
        time.sleep(1)
    else:
        print("FAIL: Backend did not become healthy in 30s")
        sys.exit(1)

    print("\n=== 1. Health Check ===")
    r = client.get("/health")
    check("health status 200", r.status_code == 200)
    data = r.json()
    check("status is healthy", data["status"] == "healthy")
    check("documents >= 10", data["documents"] >= 10, f"got {data['documents']}")

    print("\n=== 2. Stats ===")
    r = client.get("/api/stats")
    check("stats status 200", r.status_code == 200)
    data = r.json()
    check("total_documents >= 10", data["total_documents"] >= 10)
    check("total_terms > 0", data["total_terms"] > 0)
    check("avg_terms_per_doc > 0", data["avg_terms_per_doc"] > 0)

    print("\n=== 3. Index a Document ===")
    r = client.post(
        "/api/index",
        json={
            "message": "E2E test: authentication error for user test@example.com from 10.0.0.1",
            "timestamp": time.time(),
            "service": "e2e-test",
            "level": "ERROR",
        },
    )
    check("index status 200", r.status_code == 200)
    data = r.json()
    check("doc_id returned", "doc_id" in data)

    print("\n=== 4. Search for Indexed Document ===")
    r = client.get("/api/search", params={"q": "e2e authentication"})
    check("search status 200", r.status_code == 200)
    data = r.json()
    check("results found", data["total_results"] > 0, f"got {data['total_results']}")
    check("search_time_ms present", "search_time_ms" in data)
    if data["results"]:
        check(
            "highlighting present",
            "<mark>" in data["results"][0].get("highlighted_message", ""),
        )

    print("\n=== 5. Bulk Index ===")
    r = client.post(
        "/api/index/bulk",
        json={
            "documents": [
                {
                    "message": f"Bulk E2E log entry {i}",
                    "timestamp": time.time(),
                    "service": "bulk-test",
                    "level": "INFO",
                }
                for i in range(5)
            ]
        },
    )
    check("bulk index status 200", r.status_code == 200)
    data = r.json()
    check("bulk count is 5", data["count"] == 5)

    print("\n=== 6. Suggestions ===")
    r = client.get("/api/suggestions", params={"prefix": "err"})
    check("suggestions status 200", r.status_code == 200)
    data = r.json()
    check("suggestions returned", len(data["suggestions"]) > 0)
    check("error in suggestions", "error" in data["suggestions"])

    print("\n=== 7. Search with Limit ===")
    r = client.get("/api/search", params={"q": "error", "limit": 1})
    check("limited search status 200", r.status_code == 200)
    data = r.json()
    check("results limited to 1", len(data["results"]) <= 1)

    print("\n=== 8. Frontend ===")
    try:
        frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")
        fc = httpx.Client(base_url=frontend_url, timeout=10.0)
        r = fc.get("/")
        check("frontend serves HTML", r.status_code == 200)
        check("frontend has React root", '<div id="root">' in r.text or 'id="root"' in r.text)

        # Verify proxy works
        r = fc.get("/health")
        check("frontend proxies /health", r.status_code == 200 and "healthy" in r.text)
        fc.close()
    except Exception as e:
        check("frontend accessible", False, str(e))

    # Summary
    print(f"\n{'=' * 40}")
    print(f"Results: {PASSED} passed, {FAILED} failed")
    print(f"{'=' * 40}")

    sys.exit(1 if FAILED > 0 else 0)


if __name__ == "__main__":
    main()

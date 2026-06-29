# Log Recommendation Engine

A recommendation system that matches a **new incident** against a **historical incident database** using **semantic + contextual similarity**, then surfaces a **ranked list of solution suggestions**. Every suggestion carries a similarity score and the resolution that fixed the matched historical incident. The engine **improves over time via feedback** — engineers mark which suggestions were helpful, and those signals re-rank future results. Exposed through a **FastAPI REST API** and a **separate interactive web dashboard**, running as two long-lived processes.

---

## What It Does

The engine turns an incoming incident description into ranked, actionable fixes:

1. **Ingest history** — resolved incidents (`title`, `description`, `service`, `severity`, `tags`, `resolution`) are stored as the historical knowledge base.
2. **Embed** — each incident's text is encoded into a dense semantic vector with a sentence-transformer model, capturing meaning rather than exact keyword overlap.
3. **Match** — a new incident is embedded and compared against the history:
   - **Semantic similarity** — cosine similarity between embeddings finds incidents that *mean* the same thing even when worded differently.
   - **Contextual similarity** — structured signals (matching `service`, `severity`, overlapping `tags`, recency) boost or dampen candidates so the ranking respects operational context, not just prose.
4. **Rank** — semantic and contextual scores are blended into a single relevance score; the top-`k` historical incidents and their resolutions are returned as **ranked solution suggestions**.
5. **Feedback loop** — engineers submit feedback (`helpful` / `not helpful`) on each suggestion. Feedback is aggregated per `(query pattern → suggestion)` pair and folded back into the ranking so proven fixes rise and unhelpful matches sink over time.
6. **Serve** — the REST API answers recommendation queries, accepts feedback, and manages the incident corpus; the dashboard lets a human paste an incident, view ranked suggestions with scores, and vote on them.

---

## Architecture

Two long-lived processes, kept separate so the UI and the API scale and deploy independently:

| Process | Role |
|---|---|
| **Recommendation API** | FastAPI/Uvicorn REST service. Embeds incidents, computes semantic + contextual similarity, serves ranked suggestions, ingests historical incidents, and records feedback. |
| **Web Dashboard** | A separate interactive web UI process. Lets an engineer submit a new incident, browse ranked suggestions and their scores, and mark suggestions helpful/unhelpful — feeding the improvement loop. |

```
                                   feedback (helpful / not helpful)
                                   ┌───────────────────────────────┐
                                   ▼                               │
new incident ─► embed ─► semantic similarity ─┐                    │
 (title, desc,          (cosine over          ├─► blended rank ─► ranked suggestions ─► dashboard
  service, tags)         embeddings)          │   (top-k)          (score + resolution)
                                              │
        historical corpus ─► contextual signals┘
        (resolved incidents)  (service, severity,
                               tag overlap, recency)
```

---

## Tech Stack

- **Language:** Python
- **API:** FastAPI + Uvicorn, Pydantic
- **Semantic similarity:** sentence-transformers embeddings, NumPy / cosine similarity (optional FAISS for larger corpora)
- **Contextual similarity:** structured feature scoring over service / severity / tags / recency
- **Persistence:** incident corpus, embeddings, and feedback store
- **Dashboard:** separate interactive web UI process

> Exact dependency versions are pinned in [`requirements.txt`](requirements.txt).

---

## How to Run

> ⚠️ **Not yet implemented.** This project currently contains only the README, `requirements.txt`, and `.gitignore`. Run instructions will be added once the API and dashboard are built.

Planned shape:

```bash
# install
pip install -r requirements.txt

# run the recommendation API (one process)
uvicorn app.main:app --host 0.0.0.0 --port 8000

# run the web dashboard (a separate process)
# ...
```

---

## API (Planned)

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/incidents` | Add a resolved incident to the historical corpus |
| `POST` | `/recommend` | Submit a new incident, get ranked solution suggestions |
| `POST` | `/feedback` | Record helpful / not-helpful feedback on a suggestion |
| `GET`  | `/incidents` | List / search the historical corpus |
| `GET`  | `/health` | Liveness / readiness |

---

## What I Learned

_To be filled in as the project is built — notes on embedding-based retrieval, blending semantic vs. contextual signals into a single ranking, and closing a feedback loop that improves recommendations over time._

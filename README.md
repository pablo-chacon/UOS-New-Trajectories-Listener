
---

# UOS-New-Trajectories-Listener

**UOS-New-Trajectories-Listener** is a lightweight event-driven service for [UrbanOS-PoC](https://github.com/pablo-chacon/UrbanOS-POC).
It listens for **new trajectories** in the Postgres database, computes anonymized **KPI aggregates**, and pushes them to a central **InfluxDB** for cross-cluster analysis.

This makes it possible to build **global convergence signals** while keeping each UrbanOS cluster **sovereign** and free of cross-cluster leakage.

---

## ✨ Features

* Subscribes to Postgres `LISTEN/NOTIFY` on `trajectories` inserts.
* Queries existing **KPI views** (`view_latest_client_trajectories`, `view_eta_accuracy_seconds`, `view_boarding_window_hit_rate`, etc).
* Exports **anonymous metrics only** — never includes `client_id` or `session_id`.
* Buckets timestamps to prevent correlation, ensures **idempotent exports** via a guard table.
* Supports **safety push interval** to keep dashboards alive even without new data.
* Runs as a standalone container with simple `.env` configuration.

---

## 🏗 Architecture

```mermaid
flowchart LR
    subgraph Cluster["UrbanOS Cluster"]
      DB[(Postgres DB)]
      Views["Curated KPI Views"]
      Listener["UOS-New-Trajectories-Listener<br/>(this repo)"]
    end

    DB --> Views --> Listener
    Listener -->|anonymous aggregates| Influx[(Central InfluxDB)]
```

---

## 🚀 Quick Start

1. **Clone this repo**

```bash
git clone https://github.com/your-org/UOS-New-Trajectories-Listener.git
cd UOS-New-Trajectories-Listener
```

2. **Add `.env` file** (example)

```bash
# Influx connection
INFLUX_URL=https://influx.example.com/api/v2/write?org=uos&bucket=uos_metrics&precision=ns
INFLUX_TOKEN=replace_me

# Anonymous by default
ANON_MODE=true
BUCKET_SECONDS=60
SAFETY_PUSH_EVERY_SEC=600

# Optional explicit DSNs
# POSTGRES_DSN_RO=postgresql://uos_reader:***@postgres:5432/urbanos
# POSTGRES_DSN_RW=postgresql://uos_writer:***@postgres:5432/urbanos
```

3. **Build and run**

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

You should see logs like:

```
[ready_to_commit] Listening on channel uos_trajectory_ready …
[ready_to_commit] Export → Influx OK
```

---

## ⚙️ Configuration

| Variable                | Default | Description                                    |
| ----------------------- | ------- | ---------------------------------------------- |
| `INFLUX_URL`            | —       | InfluxDB write endpoint                        |
| `INFLUX_TOKEN`          | —       | API token for InfluxDB                         |
| `ANON_MODE`             | `true`  | If true, strips all tags and only sends fields |
| `BUCKET_SECONDS`        | `60`    | Bucket interval for timestamps                 |
| `SAFETY_PUSH_EVERY_SEC` | `600`   | Interval for fallback exports                  |
| `POSTGRES_DSN_RO`       | —       | Optional read-only DSN                         |
| `POSTGRES_DSN_RW`       | —       | Optional read-write DSN                        |

---

## 🔑 Key Design Principles

* **Privacy first**: no client IDs or session IDs ever leave the cluster.
* **Sovereignty**: each UrbanOS cluster can run independently, the central Influx only sees anonymous aggregates.
* **Idempotency**: guard table ensures each trajectory triggers at most one export.
* **Lightweight**: built in Python, minimal dependencies, containerized for easy deployment.

---

## 📜 License

This project is licensed under the same license as [UrbanOS-PoC](https://github.com/pablo-chacon/UrbanOS-POC).

---


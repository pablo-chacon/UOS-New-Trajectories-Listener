import os, time, json, select, hashlib
from collections import defaultdict
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from db.db_connection import connect_rw, connect_ro

load_dotenv()

# Influx config
INFLUX_URL   = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
ANON_MODE    = os.getenv("ANON_MODE", "true").lower() == "true"
BUCKET_SECONDS = int(os.getenv("BUCKET_SECONDS", "60"))
SAFETY_PUSH_EVERY_SEC = int(os.getenv("SAFETY_PUSH_EVERY_SEC", "600"))

HEADERS = {"Authorization": f"Token {INFLUX_TOKEN}"}

if not INFLUX_URL or not INFLUX_TOKEN:
    raise SystemExit("Missing INFLUX_URL or INFLUX_TOKEN env vars")


# ---------- Trigger + guard table ----------
TRIGGER_FN = """
CREATE OR REPLACE FUNCTION uos_notify_trajectory_insert() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'uos_trajectory_ready',
    json_build_object(
      'trajectory_id', NEW.id,
      'client_id',     NEW.client_id,
      'session_id',    NEW.session_id,
      'created_at',    NEW.created_at
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

TRIGGER_SQL = """
DROP TRIGGER IF EXISTS trg_trajectory_insert_notify ON trajectories;
CREATE TRIGGER trg_trajectory_insert_notify
AFTER INSERT ON trajectories
FOR EACH ROW
EXECUTE FUNCTION uos_notify_trajectory_insert();
"""

GUARD_TABLE = """
CREATE TABLE IF NOT EXISTS metrics_export_guard (
  trajectory_id BIGINT PRIMARY KEY,
  exported_at   TIMESTAMP DEFAULT NOW()
);
"""


def ensure_trigger_and_guard():
    with connect_rw() as conn, conn.cursor() as cur:
        cur.execute(TRIGGER_FN)
        cur.execute(TRIGGER_SQL)
        cur.execute(GUARD_TABLE)


def try_mark_exported(conn, trajectory_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO metrics_export_guard (trajectory_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (trajectory_id,),
        )
        return cur.rowcount == 1


# ---------- KPI fetch ----------
def fetch_kpis(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ROUND(AVG(hit_rate_pct)::numeric,2) FROM view_boarding_window_hit_rate;")
        hit = float(cur.fetchone()[0] or 0)

        cur.execute("""
          WITH base AS (
            SELECT eta_error_seconds FROM view_eta_accuracy_seconds
            WHERE departure_time >= NOW() - INTERVAL '24 hours'
          )
          SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY eta_error_seconds),
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY eta_error_seconds)
          FROM base;
        """)
        row = cur.fetchone()
        p50, p95 = int(row[0] or 0), int(row[1] or 0)

        cur.execute("SELECT COUNT(*) FROM view_active_clients_geodata;")
        active = int(cur.fetchone()[0] or 0)

        cur.execute("""
          SELECT COUNT(*) FROM view_daily_routing_summary
          WHERE created_at::date = NOW()::date;
        """)
        routes_today = int(cur.fetchone()[0] or 0)

    return hit, p50, p95, active, routes_today


def ts_bucket_now_ns():
    now = datetime.now(timezone.utc)
    bucket_epoch = (int(now.timestamp()) // BUCKET_SECONDS) * BUCKET_SECONDS
    return int(bucket_epoch * 1e9)


def make_lines(hit, p50, p95, active, routes_today):
    ts_ns = ts_bucket_now_ns()
    if ANON_MODE:
        return "\n".join([
            f"uos_kpi boarding_hit_rate_pct={hit} {ts_ns}",
            f"uos_kpi eta_error_seconds_p50={p50}i,eta_error_seconds_p95={p95}i {ts_ns}",
            f"uos_kpi active_clients={active}i,routes_today={routes_today}i {ts_ns}",
        ])
    else:
        cluster = os.getenv("CLUSTER_ID", "unknown")
        env_tag = os.getenv("ENV_TAG", "unknown")
        region = os.getenv("REGION_TAG", "unknown")
        tags = f"cluster_id={cluster},env={env_tag},region={region}"
        return "\n".join([
            f"uos_kpi,{tags} boarding_hit_rate_pct={hit} {ts_ns}",
            f"uos_kpi,{tags} eta_error_seconds_p50={p50}i,eta_error_seconds_p95={p95}i {ts_ns}",
            f"uos_kpi,{tags} active_clients={active}i,routes_today={routes_today}i {ts_ns}",
        ])


def write_influx(lp: str):
    r = requests.post(INFLUX_URL, headers=HEADERS, data=lp, timeout=5)
    r.raise_for_status()


# ---------- Main loop ----------
def main():
    ensure_trigger_and_guard()
    pending = defaultdict(float)
    last_push_ts = 0

    with connect_ro() as conn:
        cur = conn.cursor()
        cur.execute("LISTEN uos_trajectory_ready;")
        print("[ready_to_commit] Listening on channel uos_trajectory_ready …")

        while True:
            if select.select([conn], [], [], 1.0) != ([], [], []):
                conn.poll()
                while conn.notifies:
                    n = conn.notifies.pop(0)
                    payload = json.loads(n.payload)
                    traj_id = int(payload.get("trajectory_id", 0))
                    with connect_rw() as rw:
                        if try_mark_exported(rw, traj_id):
                            pending["export"] = time.time()

            now = time.time()
            if now - last_push_ts >= SAFETY_PUSH_EVERY_SEC:
                try:
                    hit, p50, p95, active, routes_today = fetch_kpis(conn)
                    write_influx(make_lines(hit, p50, p95, active, routes_today))
                    last_push_ts = now
                    print("[ready_to_commit] Safety push → Influx OK")
                except Exception as e:
                    print(f"[ready_to_commit] Safety push failed: {e}")

            if "export" in pending and pending["export"] <= now:
                try:
                    hit, p50, p95, active, routes_today = fetch_kpis(conn)
                    write_influx(make_lines(hit, p50, p95, active, routes_today))
                    pending.pop("export", None)
                    print("[ready_to_commit] Export → Influx OK")
                except Exception as e:
                    print(f"[ready_to_commit] Export failed: {e}")


if __name__ == "__main__":
    main()

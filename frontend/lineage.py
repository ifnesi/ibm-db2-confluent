"""
Data lineage service — aggregates topology and metrics from Kafka, Connectors, and Flink.
"""
import os
import time
import requests

from collections import deque
from typing import Dict, List, Any, Optional
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "broker:29092")
KAFKA_CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083")
FLINK_URL = os.environ.get("FLINK_URL", "http://flink-jobmanager:9081")

TARGET_TOPICS = {"DB2INST1.IOT_DEVICES", "IOT_DEVICES_AVG"}
THROUGHPUT_WINDOW_SECS = 30


def fmt_bytes(bps: float) -> str:
    if bps >= 1024:
        return f"{bps/1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


class LineageService:
    def __init__(self):
        self.admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        # Rolling window: topic -> deque of (timestamp, total_offset) tuples
        self.offset_history: Dict[str, deque] = {}

    # ------------------------------------------------------------------
    # Kafka helpers
    # ------------------------------------------------------------------

    def _consumer(self, group_id: str = "__lineage-probe__") -> Consumer:
        return Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group_id,
            "enable.auto.commit": False,
        })

    def get_end_offsets(self, topic: str) -> Dict[int, int]:
        """Return {partition: high_watermark} for topic."""
        try:
            c = self._consumer()
            meta = c.list_topics(topic, timeout=5)
            if topic not in meta.topics:
                c.close()
                return {}
            result = {}
            for pid in meta.topics[topic].partitions:
                low, high = c.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
                result[pid] = high
            c.close()
            return result
        except Exception as e:
            print(f"[lineage] end_offsets {topic}: {e}")
            return {}

    def get_lag(self, group_id: str, topic: str, end_offsets: Dict[int, int]) -> int:
        """Total consumer lag for group on topic."""
        if not end_offsets:
            return 0
        try:
            c = self._consumer(group_id)
            tps = [TopicPartition(topic, pid) for pid in end_offsets]
            committed = c.committed(tps, timeout=5)
            c.close()
            total = 0
            for tp in committed:
                end = end_offsets.get(tp.partition, 0)
                offset = max(0, tp.offset) if tp.offset and tp.offset >= 0 else 0
                total += max(0, end - offset)
            return total
        except Exception as e:
            print(f"[lineage] lag {group_id}/{topic}: {e}")
            return 0

    def compute_throughputs(self) -> Dict[str, float]:
        """
        Bytes/sec per topic as a 30-second rolling average.
        Keeps a deque of (timestamp, total_offset) samples; computes rate from oldest
        sample still within the window to the current sample.
        """
        now = time.time()
        result = {}
        for topic in TARGET_TOPICS:
            current = self.get_end_offsets(topic)
            total_offset = sum(current.values()) if current else 0

            if topic not in self.offset_history:
                self.offset_history[topic] = deque()

            history = self.offset_history[topic]
            history.append((now, total_offset))

            # Drop samples older than the rolling window
            cutoff = now - THROUGHPUT_WINDOW_SECS
            while history and history[0][0] < cutoff:
                history.popleft()

            if len(history) < 2:
                result[topic] = 0.0
                continue

            oldest_ts, oldest_offset = history[0]
            dt = max(0.5, now - oldest_ts)
            delta = max(0, total_offset - oldest_offset)
            # Avro IoT messages average ~512 bytes
            result[topic] = round((delta * 512) / dt, 2)

        return result

    # ------------------------------------------------------------------
    # External service queries
    # ------------------------------------------------------------------

    def get_connectors(self) -> Dict[str, Any]:
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors?expand=info", timeout=5)
            resp.raise_for_status()
            result = {}
            for name, info in resp.json().items():
                config = info.get("info", {}).get("config", {})
                topics_raw = config.get("topics", "")
                topics = [t.strip() for t in topics_raw.split(",") if t.strip()]
                cls = config.get("connector.class", "")
                result[name] = {
                    "type": "source" if "Source" in cls else "sink",
                    "topics": topics,
                    "class": cls,
                    "config": config,
                }
            return result
        except Exception as e:
            print(f"[lineage] connectors: {e}")
            return {}

    def get_flink_jobs(self) -> List[Dict[str, Any]]:
        try:
            resp = requests.get(f"{FLINK_URL}/jobs", timeout=5)
            resp.raise_for_status()
            jobs = []
            for job in resp.json().get("jobs", []):
                jid = job.get("id")
                detail = requests.get(f"{FLINK_URL}/jobs/{jid}", timeout=5).json()
                jobs.append({
                    "id": jid,
                    "name": detail.get("name", "Flink Job"),
                    "status": detail.get("state", "UNKNOWN"),
                })
            return jobs
        except Exception as e:
            print(f"[lineage] flink: {e}")
            return []

    # ------------------------------------------------------------------
    # Graph builder
    # ------------------------------------------------------------------

    def build_lineage(self) -> Dict[str, Any]:
        nodes: List[Dict] = []
        edges: List[Dict] = []
        node_ids: Dict[str, str] = {}

        connectors = self.get_connectors()
        flink_jobs = self.get_flink_jobs()
        throughputs = self.compute_throughputs()
        end_offsets = {t: self.get_end_offsets(t) for t in TARGET_TOPICS}

        sinks = {k: v for k, v in connectors.items() if v["type"] == "sink"}

        # --- SOURCE ---
        if "db2-source-connector" in connectors:
            src = connectors["db2-source-connector"]
            nodes.append({"data": {
                "id": "db2-source", "label": "DB2 Source\n(JDBC)", "type": "source",
                "detail": {"connector": "db2-source-connector", "class": src["class"]}
            }})
            node_ids["db2-source"] = "db2-source"

        # --- TOPICS ---
        for topic in TARGET_TOPICS:
            nid = f"topic-{topic}"
            nodes.append({"data": {
                "id": nid, "label": topic, "type": "topic",
                "detail": {"topic": topic, "throughput_bytes_per_sec": throughputs.get(topic, 0)}
            }})
            node_ids[topic] = nid

        # --- FLINK (always shown; status from API if running) ---
        if flink_jobs:
            job = flink_jobs[0]
            flink_label = f"Flink\n(RUNNING)"
            flink_detail = {"status": job["status"], "job_id": job["id"], "name": job["name"]}
        else:
            flink_label = "Flink\n(stopped)"
            flink_detail = {"status": "NOT_RUNNING"}
        nodes.append({"data": {
            "id": "flink-job", "label": flink_label, "type": "processor",
            "detail": flink_detail
        }})
        node_ids["flink"] = "flink-job"

        # --- SINKS ---
        for sink_name, sink_info in sinks.items():
            group_id = f"connect-{sink_name}"
            total_lag = sum(
                self.get_lag(group_id, t, end_offsets.get(t, {}))
                for t in sink_info["topics"]
            )
            label = sink_name.replace("-iot-devices-sink", "").replace("-", " ").title()
            nodes.append({"data": {
                "id": f"sink-{sink_name}", "label": f"{label}\nSink", "type": "sink",
                "detail": {
                    "connector": sink_name,
                    "group_id": group_id,
                    "topics": sink_info["topics"],
                    "total_lag": total_lag,
                }
            }})
            node_ids[sink_name] = f"sink-{sink_name}"

        # --- WEBAPP CONSUMER (static node — always present) ---
        webapp_group = "frontend-consumer-group"
        webapp_lag_devices = self.get_lag(webapp_group, "DB2INST1.IOT_DEVICES", end_offsets.get("DB2INST1.IOT_DEVICES", {}))
        webapp_lag_avg = self.get_lag(webapp_group, "IOT_DEVICES_AVG", end_offsets.get("IOT_DEVICES_AVG", {}))
        nodes.append({"data": {
            "id": "webapp-consumer", "label": "WebApp\nDashboard", "type": "sink",
            "detail": {
                "group_id": webapp_group,
                "topics": ["DB2INST1.IOT_DEVICES", "IOT_DEVICES_AVG"],
                "lag_devices": webapp_lag_devices,
                "lag_avg": webapp_lag_avg,
            }
        }})
        node_ids["webapp"] = "webapp-consumer"

        # --- EDGES ---
        dev_tp = throughputs.get("DB2INST1.IOT_DEVICES", 0)
        avg_tp = throughputs.get("IOT_DEVICES_AVG", 0)

        # Source → IOT_DEVICES
        if "db2-source" in node_ids and "DB2INST1.IOT_DEVICES" in node_ids:
            edges.append({"data": {
                "id": "edge-source-devices",
                "source": "db2-source", "target": node_ids["DB2INST1.IOT_DEVICES"],
                "label": fmt_bytes(dev_tp), "throughput": dev_tp,
            }})

        # IOT_DEVICES → Flink (no lag: Flink uses its own state backend, not __consumer_offsets)
        if "flink" in node_ids and "DB2INST1.IOT_DEVICES" in node_ids:
            edges.append({"data": {
                "id": "edge-devices-flink",
                "source": node_ids["DB2INST1.IOT_DEVICES"], "target": "flink-job",
                "label": fmt_bytes(dev_tp), "throughput": dev_tp,
            }})

        # Flink → IOT_DEVICES_AVG
        if "flink" in node_ids and "IOT_DEVICES_AVG" in node_ids:
            edges.append({"data": {
                "id": "edge-flink-avg",
                "source": "flink-job", "target": node_ids["IOT_DEVICES_AVG"],
                "label": fmt_bytes(avg_tp), "throughput": avg_tp,
            }})

        # Connector sinks
        for sink_name, sink_info in sinks.items():
            group_id = f"connect-{sink_name}"
            for topic in sink_info["topics"]:
                if topic not in node_ids or sink_name not in node_ids:
                    continue
                tp = throughputs.get(topic, 0)
                lag = self.get_lag(group_id, topic, end_offsets.get(topic, {}))
                lag_str = f" | Lag: {lag}" if lag else ""
                edges.append({"data": {
                    "id": f"edge-{topic}-{sink_name}",
                    "source": node_ids[topic], "target": node_ids[sink_name],
                    "label": fmt_bytes(tp) + lag_str,
                    "throughput": tp, "lag": lag,
                }})

        # WebApp reads IOT_DEVICES and IOT_DEVICES_AVG
        for topic, lag in [("DB2INST1.IOT_DEVICES", webapp_lag_devices), ("IOT_DEVICES_AVG", webapp_lag_avg)]:
            if topic in node_ids:
                tp = throughputs.get(topic, 0)
                lag_str = f" | Lag: {lag}" if lag else ""
                edges.append({"data": {
                    "id": f"edge-{topic}-webapp",
                    "source": node_ids[topic], "target": "webapp-consumer",
                    "label": fmt_bytes(tp) + lag_str,
                    "throughput": tp, "lag": lag,
                }})

        return {"nodes": nodes, "edges": edges, "timestamp": time.time()}


_lineage_service: Optional[LineageService] = None


def get_lineage_service() -> LineageService:
    global _lineage_service
    if _lineage_service is None:
        _lineage_service = LineageService()
    return _lineage_service


def get_lineage() -> Dict[str, Any]:
    return get_lineage_service().build_lineage()

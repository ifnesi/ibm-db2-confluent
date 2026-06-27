"""
Data lineage service — aggregates topology and metrics from Kafka, Connectors, and Flink.
"""
import os
import time
import requests

from typing import Dict, List, Any, Optional


KAFKA_CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://connect:8083")
FLINK_URL = os.environ.get("FLINK_URL", "http://flink-jobmanager:9081")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")

TARGET_TOPICS = {"iot_devices_db2", "iot_devices_avg"}


def fmt_bytes(bps: float) -> str:
    if bps >= 1024:
        return f"{bps/1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


class LineageService:

    def get_lag(self, group_id: str, topic: str) -> int:
        """Total consumer lag for group on topic, via Prometheus."""
        promql = (
            f'sum(io_confluent_kafka_server_tenant_consumer_lag_offsets{{'
            f'consumer_group="{group_id}",topic="{topic}"}})'
        )
        value = self._query_prometheus(promql)
        return int(value) if value is not None else 0

    def _query_prometheus(self, promql: str) -> Optional[float]:
        """Run an instant PromQL query and return the first scalar result."""
        try:
            resp = requests.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": promql},
                timeout=5,
            )
            resp.raise_for_status()
            results = resp.json().get("data", {}).get("result", [])
            if results:
                return float(results[0]["value"][1])
        except Exception as e:
            print(f"[lineage] prometheus query '{promql}': {e}")
        return None

    def get_topic_metrics(self, topic: str) -> Dict[str, Any]:
        """Fetch produce/fetch request rates, storage, and partition count for a topic from Prometheus."""
        rate_metrics = {
            "produce_requests_per_sec": "io_confluent_kafka_server_broker_topic_total_produce_requests_rate_1_min",
            "failed_produce_requests_per_sec": "io_confluent_kafka_server_broker_topic_failed_produce_requests_rate_1_min",
            "failed_fetch_requests_per_sec": "io_confluent_kafka_server_broker_topic_failed_fetch_requests_rate_1_min",
        }
        result = {}
        for key, metric in rate_metrics.items():
            value = self._query_prometheus(f'{metric}{{topic="{topic}"}}')
            result[key] = round(value, 2) if value is not None else 0.0

        log_size = self._query_prometheus(f'code:io_confluent_kafka_server_log_size_by_topic:total{{topic="{topic}"}}')
        result["log_size_bytes"] = int(log_size) if log_size is not None else 0

        partitions = self._query_prometheus(f'code:io_confluent_kafka_server_partition_count_by_topic:total{{topic="{topic}"}}')
        result["partition_count"] = int(partitions) if partitions is not None else 0

        return result

    def compute_throughputs(self) -> Dict[str, float]:
        """Bytes/sec per topic from Confluent telemetry (broker_topic_bytes_in_rate_1_min)."""
        result = {}
        for topic in TARGET_TOPICS:
            promql = (
                f'io_confluent_kafka_server_broker_topic_bytes_in_rate_1_min{{'
                f'topic="{topic}"}}'
            )
            value = self._query_prometheus(promql)
            result[topic] = round(value, 2) if value is not None else 0.0
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
            extra = self.get_topic_metrics(topic)
            nodes.append({"data": {
                "id": nid, "label": topic, "type": "topic",
                "detail": {
                    "topic": topic,
                    "throughput_bytes_per_sec": throughputs.get(topic, 0),
                    **extra,
                }
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
                self.get_lag(group_id, t)
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
        webapp_lag_devices = self.get_lag(webapp_group, "iot_devices_db2")
        webapp_lag_avg = self.get_lag(webapp_group, "iot_devices_avg")
        nodes.append({"data": {
            "id": "webapp-consumer", "label": "WebApp\nDashboard", "type": "sink",
            "detail": {
                "group_id": webapp_group,
                "topics": ["iot_devices_db2", "iot_devices_avg"],
                "lag_devices": webapp_lag_devices,
                "lag_avg": webapp_lag_avg,
            }
        }})
        node_ids["webapp"] = "webapp-consumer"

        # --- EDGES ---
        dev_tp = throughputs.get("iot_devices_db2", 0)
        avg_tp = throughputs.get("iot_devices_avg", 0)

        # Source → IOT_DEVICES
        if "db2-source" in node_ids and "iot_devices_db2" in node_ids:
            edges.append({"data": {
                "id": "edge-source-devices",
                "source": "db2-source", "target": node_ids["iot_devices_db2"],
                "label": fmt_bytes(dev_tp), "throughput": dev_tp,
            }})

        # IOT_DEVICES → Flink (no lag: Flink uses its own state backend, not __consumer_offsets)
        if "flink" in node_ids and "iot_devices_db2" in node_ids:
            edges.append({"data": {
                "id": "edge-devices-flink",
                "source": node_ids["iot_devices_db2"], "target": "flink-job",
                "label": fmt_bytes(dev_tp), "throughput": dev_tp,
            }})

        # Flink → iot_devices_avg
        if "flink" in node_ids and "iot_devices_avg" in node_ids:
            edges.append({"data": {
                "id": "edge-flink-avg",
                "source": "flink-job", "target": node_ids["iot_devices_avg"],
                "label": fmt_bytes(avg_tp), "throughput": avg_tp,
            }})

        # Connector sinks
        for sink_name, sink_info in sinks.items():
            group_id = f"connect-{sink_name}"
            for topic in sink_info["topics"]:
                if topic not in node_ids or sink_name not in node_ids:
                    continue
                tp = throughputs.get(topic, 0)
                lag = self.get_lag(group_id, topic)
                lag_str = f" | Lag: {lag}" if lag else ""
                edges.append({"data": {
                    "id": f"edge-{topic}-{sink_name}",
                    "source": node_ids[topic], "target": node_ids[sink_name],
                    "label": fmt_bytes(tp) + lag_str,
                    "throughput": tp, "lag": lag,
                }})

        # WebApp reads IOT_DEVICES and iot_devices_avg
        for topic, lag in [("iot_devices_db2", webapp_lag_devices), ("iot_devices_avg", webapp_lag_avg)]:
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

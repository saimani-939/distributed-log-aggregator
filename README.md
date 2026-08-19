# distributed-log-aggregator
# ⚡ Real-Time Distributed Log Aggregator & SRE Alerting Engine

A high-throughput, dual-path distributed log processing and observability platform built with **Apache Kafka (KRaft)**, **ClickHouse OLAP**, **FastAPI WebSockets**, and **Grafana**. 

The system decouples real-time **hot-path anomaly alerting** (sub-50ms latency via in-memory sliding windows) from **cold-path historical log indexing** (batched micro-writes into columnar storage for sub-second analytical queries).

---

## 🏗️ System Architecture

```text
                     ┌────────────────────────────────────────┐
                     │     Simulated Microservices Stream     │
                     │ (Auth, Payment, Order, Inventory, API) │
                     └───────────────────┬────────────────────┘
                                         │ JSON Logs over TCP
                                         ▼
                     ┌────────────────────────────────────────┐
                     │          Apache Kafka (KRaft)          │
                     │          Topic: `app-logs`             │
                     └──────────┬───────────────────┬─────────┘
                                │                   │
       Hot Path (Sub-50ms)      │                   │      Cold Path (Micro-Batched)
       Group: `alert-engine`    │                   │      Group: `clickhouse-indexer`
                                ▼                   ▼
   ┌─────────────────────────────────────┐   ┌─────────────────────────────────────┐
   │         FastAPI Alert Engine        │   │      ClickHouse Batch Consumer      │
   │  - Sliding Window (deque, 10s)      │   │  - In-memory Buffer (50 records)    │
   │  - Error Threshold Evaluator (>=5)  │   │  - Flush interval (2 seconds)       │
   └──────────────────┬──────────────────┘   └──────────────────┬──────────────────┘
                      │ Full-Duplex                     │ Batched Inserts
                      │ WebSockets                      ▼
                      ▼                     ┌─────────────────────────────────────┐
   ┌─────────────────────────────────────┐   │          ClickHouse DB              │
   │      Real-Time SRE Dashboard        │   │      Table: `logs` (MergeTree)      │
   │  - Instant Incident Cards           │   └──────────────────┬──────────────────┘
   │  - Live Service Anomaly Stream      │                      │ SQL Aggregations
   └─────────────────────────────────────┘                      ▼
                                             ┌─────────────────────────────────────┐
                                             │          Grafana Telemetry          │
                                             │  - Ingestion Throughput (Logs/sec)  │
                                             │  - Error Breakdown & Failure Index  │
                                             └─────────────────────────────────────┘

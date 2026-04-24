"""
Real-Time Customer Event Stream Simulator
==========================================
Simulates Apache Kafka-style real-time streaming
of customer behaviour events for segmentation updates.

Architecture:
    Producer           → generates live customer activity events
    SegmentConsumer    → updates segment assignment in real-time
    MetricsConsumer    → aggregates live CLV and RFM metrics
    AuditConsumer      → logs all events to JSONL

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import queue
import threading
import time
import random
import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("kafka_segmentation.log"),
        logging.StreamHandler()
    ]
)

TOPIC_NAME         = "customer.behaviour.events"
PARTITION_COUNT    = 3
PRODUCER_RATE_HZ   = 10
SIMULATION_SECONDS = 60

REGIONS   = ["Greater Accra","Ashanti","Western","Eastern","Northern","Volta"]
CHANNELS  = ["In-Store","Online","Mobile App","Agent"]
SEGMENTS  = ["Champions","Loyal Customers","At Risk","Lost/Inactive"]

REPORTS_PATH = Path("data/reports/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)


class CustomerTopic:
    def __init__(self, name, partitions=3):
        self.name       = name
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.counter    = 0
        self.lock       = threading.Lock()

    def produce(self, msg):
        with self.lock:
            pid = self.counter % len(self.partitions)
            self.partitions[pid].put(msg)
            self.counter += 1

    def consume(self, pid, timeout=0.1):
        try:
            return self.partitions[pid].get(timeout=timeout)
        except queue.Empty:
            return None


class CustomerEventProducer(threading.Thread):
    def __init__(self, topic, rate_hz, duration_secs):
        super().__init__(name="CustomerProducer", daemon=True)
        self.topic    = topic
        self.rate_hz  = rate_hz
        self.duration = duration_secs
        self.produced = 0
        self.running  = True
        self.logger   = logging.getLogger("CustomerProducer")
        self._counter = 1

    def generate_event(self):
        recency   = random.choices(
            [random.randint(1,30), random.randint(30,90),
             random.randint(90,180), random.randint(180,365)],
            weights=[30,30,25,15]
        )[0]
        frequency = random.choices(range(1,21), weights=[10,12,13,12,11,10,9,8,6,4,2,1,1,1,0,0,0,0,0,0])[0]
        spend     = abs(random.lognormvariate(6.0, 1.2)) * frequency

        r_score = 1 - recency / 365
        f_score = frequency / 20
        m_score = min(spend / 100000, 1.0)
        clv     = round(r_score*0.3 + f_score*0.3 + m_score*0.4, 4)

        if clv >= 0.65:   segment = "Champions"
        elif clv >= 0.45: segment = "Loyal Customers"
        elif clv >= 0.25: segment = "At Risk"
        else:             segment = "Lost/Inactive"

        return {
            "event_id":     f"EVT-{str(self._counter).zfill(8)}",
            "customer_id":  f"CUST{str(random.randint(1,5000)).zfill(7)}",
            "timestamp":    datetime.now().isoformat(),
            "region":       random.choices(REGIONS,  weights=[35,25,15,12,8,5])[0],
            "channel":      random.choices(CHANNELS, weights=[35,30,25,10])[0],
            "recency_days": recency,
            "frequency":    frequency,
            "total_spend":  round(spend, 2),
            "clv_score":    clv,
            "segment":      segment,
            "momo_user":    random.random() < 0.70,
            "is_high_value": spend >= 10000,
        }

    def run(self):
        self.logger.info(f"Producer started on topic '{self.topic.name}' at {self.rate_hz} events/sec")
        end_time   = time.time() + self.duration
        sleep_time = 1.0 / self.rate_hz
        while self.running and time.time() < end_time:
            self.topic.produce(self.generate_event())
            self.produced  += 1
            self._counter  += 1
            time.sleep(sleep_time)
        self.running = False
        self.logger.info(f"Producer finished — published {self.produced:,} events")


class SegmentConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="SegmentConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.alerts   = []
        self.logger   = logging.getLogger("SegmentConsumer")

    def run(self):
        self.logger.info("Consumer started — monitoring Champions on partition 0")
        while self.running:
            msg = self.topic.consume(0)
            if msg is None:
                continue
            if msg["segment"] == "Champions":
                self.alerts.append(msg)
                self.logger.info(
                    f"CHAMPION DETECTED | {msg['customer_id']} | "
                    f"CLV: {msg['clv_score']:.4f} | "
                    f"Spend: GHS {msg['total_spend']:,.2f} | "
                    f"{msg['region']}"
                )


class MetricsConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="MetricsConsumer", daemon=True)
        self.topic   = topic
        self.running = True
        self.logger  = logging.getLogger("MetricsConsumer")
        self.m = {
            "total": 0, "champions": 0, "loyal": 0,
            "at_risk": 0, "lost": 0, "high_value": 0,
            "total_spend": 0.0, "total_clv": 0.0,
            "by_region": {}, "by_segment": {}
        }

    def run(self):
        self.logger.info("Consumer started — aggregating CLV metrics on partition 1")
        while self.running:
            msg = self.topic.consume(1)
            if msg is None:
                continue
            m = self.m
            m["total"]       += 1
            m["total_spend"] += msg["total_spend"]
            m["total_clv"]   += msg["clv_score"]
            if msg["is_high_value"]: m["high_value"] += 1
            seg = msg["segment"]
            if seg == "Champions":       m["champions"] += 1
            elif seg == "Loyal Customers": m["loyal"]   += 1
            elif seg == "At Risk":       m["at_risk"]   += 1
            else:                        m["lost"]      += 1
            m["by_region"][msg["region"]] = \
                m["by_region"].get(msg["region"], 0) + msg["total_spend"]
            m["by_segment"][seg] = m["by_segment"].get(seg, 0) + 1

    def snapshot(self):
        m = self.m
        t = max(m["total"], 1)
        return {
            "total":        m["total"],
            "champions":    m["champions"],
            "loyal":        m["loyal"],
            "at_risk":      m["at_risk"],
            "lost":         m["lost"],
            "high_value":   m["high_value"],
            "avg_clv":      round(m["total_clv"] / t, 4),
            "total_spend":  round(m["total_spend"], 2),
            "top_region":   max(m["by_region"], key=m["by_region"].get, default="N/A"),
        }


class AuditConsumer(threading.Thread):
    def __init__(self, topic):
        super().__init__(name="AuditConsumer", daemon=True)
        self.topic    = topic
        self.running  = True
        self.consumed = 0
        self.logger   = logging.getLogger("AuditConsumer")
        self.log_file = REPORTS_PATH / "customer_events_live.jsonl"

    def run(self):
        self.logger.info(f"Consumer started — logging events to {self.log_file}")
        with open(self.log_file, "w") as f:
            while self.running:
                msg = self.topic.consume(2)
                if msg is None:
                    continue
                self.consumed += 1
                f.write(json.dumps(msg) + "\n")
                f.flush()


def print_live_metrics(producer, metrics, segment, audit, interval=10):
    start = time.time()
    while producer.running:
        time.sleep(interval)
        elapsed = int(time.time() - start)
        snap    = metrics.snapshot()
        print("\n" + "="*65)
        print(f"  CUSTOMER STREAM — LIVE METRICS  [{elapsed}s elapsed]")
        print("="*65)
        print(f"  Events Produced      : {producer.produced:,}")
        print(f"  Total Scored         : {snap['total']:,}")
        print(f"  Champions            : {snap['champions']:,}")
        print(f"  Loyal Customers      : {snap['loyal']:,}")
        print(f"  At Risk              : {snap['at_risk']:,}")
        print(f"  Lost/Inactive        : {snap['lost']:,}")
        print(f"  High Value Customers : {snap['high_value']:,}")
        print(f"  Avg CLV Score        : {snap['avg_clv']:.4f}")
        print(f"  Total Spend Streamed : GHS {snap['total_spend']:,.2f}")
        print(f"  Top Region           : {snap['top_region']}")
        print(f"  Champion Alerts      : {len(segment.alerts):,}")
        print(f"  Events Logged        : {audit.consumed:,}")
        print("="*65)


def run_kafka_segmentation_simulator():
    print("\n" + "="*65)
    print("  CUSTOMER SEGMENTATION — KAFKA STREAM SIMULATOR")
    print("  Architecture: Producer → Topic → 3 Consumer Groups")
    print("="*65)
    print(f"  Topic          : {TOPIC_NAME}")
    print(f"  Partitions     : {PARTITION_COUNT}")
    print(f"  Producer Rate  : {PRODUCER_RATE_HZ} events/sec")
    print(f"  Duration       : {SIMULATION_SECONDS} seconds")
    print(f"  Expected       : ~{PRODUCER_RATE_HZ * SIMULATION_SECONDS:,} events")
    print("="*65 + "\n")

    topic   = CustomerTopic(TOPIC_NAME, PARTITION_COUNT)
    producer = CustomerEventProducer(topic, PRODUCER_RATE_HZ, SIMULATION_SECONDS)
    segment  = SegmentConsumer(topic)
    metrics  = MetricsConsumer(topic)
    audit    = AuditConsumer(topic)

    for t in [producer, segment, metrics, audit]:
        t.start()

    m_thread = threading.Thread(
        target=print_live_metrics,
        args=(producer, metrics, segment, audit, 10),
        daemon=True
    )
    m_thread.start()
    producer.join()
    time.sleep(3)
    for t in [segment, metrics, audit]:
        t.running = False

    final = metrics.snapshot()
    print("\n" + "="*65)
    print("  SEGMENTATION KAFKA SIMULATION — FINAL SUMMARY")
    print("="*65)
    print(f"  Total Events Produced  : {producer.produced:,}")
    print(f"  Champions Detected     : {final['champions']:,}")
    print(f"  Loyal Customers        : {final['loyal']:,}")
    print(f"  At Risk                : {final['at_risk']:,}")
    print(f"  Lost/Inactive          : {final['lost']:,}")
    print(f"  High Value Customers   : {final['high_value']:,}")
    print(f"  Avg CLV Score          : {final['avg_clv']:.4f}")
    print(f"  Total Spend Streamed   : GHS {final['total_spend']:,.2f}")
    print(f"  Champion Alerts        : {len(segment.alerts):,}")
    print(f"  Top Region             : {final['top_region']}")
    print(f"  Events Logged          : {audit.consumed:,}")
    print("="*65 + "\n")

    if segment.alerts:
        import csv
        alerts_path = REPORTS_PATH / "champion_customer_alerts.csv"
        with open(alerts_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=segment.alerts[0].keys())
            writer.writeheader()
            writer.writerows(segment.alerts)
        print(f"  Champion alerts saved: {alerts_path}")


if __name__ == "__main__":
    run_kafka_segmentation_simulator()
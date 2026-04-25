# 👥 Customer Segmentation Analysis Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-336791?style=flat-square&logo=postgresql)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.8-F7931E?style=flat-square&logo=scikit-learn)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat-square&logo=powerbi)
![Tests](https://img.shields.io/badge/Tests-25%2F25%20Passing-brightgreen?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)

A production-grade customer segmentation pipeline that generates **5,000 Ghana retail customer records**, engineers **RFM features**, applies **K-Means clustering** to identify 4 distinct customer segments, and loads results into a live **PostgreSQL** warehouse — with a full **dbt analytical layer**, **Power BI dashboard**, **Airflow DAG**, and **Kafka stream simulator**.

What makes this project unique: it combines **machine learning (K-Means)** with data engineering to deliver actionable marketing intelligence.

---

## 🏗️ System Architecture

```
[Customer Transaction Data Source]
           │
           ▼
     ┌───────────┐
     │  EXTRACT  │  ← Generates 5,000 Ghana retail customer records
     └───────────┘
           │
           ▼
     ┌───────────┐
     │ TRANSFORM │  ← RFM feature engineering + K-Means clustering (k=4)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   LOAD    │  ← PostgreSQL warehouse (customer_dw schema)
     └───────────┘
           │
           ▼
     ┌───────────┐
     │    dbt    │  ← Analytical layer: 1 staging view + 3 mart tables
     └───────────┘
           │
           ▼
     ┌───────────┐
     │  Power BI │  ← 4-page CLV and segmentation dashboard
     └───────────┘
           │
           ▼
     ┌───────────┐
     │   Kafka   │  ← Real-time customer event stream simulator
     └───────────┘
```

---

## ✅ What The Pipeline Does

### Extract
- Generates 5,000 realistic Ghana retail customer profiles
- Fields include: region, channel, gender, age group, purchase history
- Covers all 6 Ghana regions with realistic distribution

### Transform — RFM Feature Engineering + K-Means Clustering

**Step 1: RFM Features**

| Feature | Description |
|---|---|
| `recency_days` | Days since last purchase |
| `rfm_frequency` | Number of purchases |
| `rfm_monetary` | Total spend in GHS |

**Step 2: K-Means Clustering**
- Features scaled with StandardScaler
- K-Means applied with k=4, n_init=10
- Clusters mapped to business segment names

**Step 3: CLV Score**
Each customer receives a Customer Lifetime Value score:
```
CLV = (1/recency × 0.3) + (frequency/max_freq × 0.3) + (spend/max_spend × 0.4)
```

### Customer Segments

| Segment | Action |
|---|---|
| Champions | Reward with VIP loyalty programme |
| Loyal Customers | Upsell premium products |
| At Risk | Re-engage with win-back campaign |
| Lost/Inactive | Send reactivation email with coupon |

---

## 🔁 dbt Analytical Layer

4 models built on top of PostgreSQL:

| Model | Type | Description |
|---|---|---|
| stg_customer_segments | View | Cleaned segments + spend/CLV tiers |
| mart_segments_summary | Table | KPIs per customer segment |
| mart_segments_by_region | Table | Segment distribution by Ghana region |
| mart_clv_analysis | Table | 2,193 CLV breakdowns by demographics |

```bash
cd dbt
dbt run --profiles-dir .    # Run all 4 models
dbt test --profiles-dir .   # Run 4 data quality tests
```

---

## 📊 Power BI Dashboard — 4 Pages

Connected live to PostgreSQL via dbt mart tables:

| Page | Key Metrics |
|---|---|
| Executive Summary | 5K customers, GHS 22.22M revenue, 0.15 avg CLV, 72.49% MoMo adoption |
| Segment Analysis | Champions highest CLV, Lost/Inactive 238 avg recency days |
| Regional Analysis | Greater Accra leads revenue and customer count |
| CLV Analysis | Platinum tier highest CLV, 25-34 age group top CLV, Online leads revenue |

---

## 🌊 Kafka Stream Simulator

Real-time customer behaviour event streaming:

```bash
python kafka_segmentation_simulator.py
```

```
Topic          : customer.behaviour.events
Partitions     : 3
Producer Rate  : 10 events/sec
Duration       : 60 seconds

Producer        → generates live customer behaviour events
SegmentConsumer → monitors and alerts on Champion customers (partition 0)
MetricsConsumer → aggregates real-time CLV and RFM metrics (partition 1)
AuditConsumer   → logs all events to JSONL file (partition 2)

Final Results:
  Total Events Produced  : 594
  Champions Detected     : 1
  Loyal Customers        : 8
  At Risk                : 148
  Lost/Inactive          : 41
  Avg CLV Score          : 0.3158
  Total Spend Streamed   : GHS 765,163.33
  Top Region             : Greater Accra
```

---

## 🧪 Unit Tests — 25/25 Passing

```bash
pytest test_customer_segmentation.py -v
# 25 passed in 62.30s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestExtract | 9 | Row count, columns, valid values, uniqueness |
| TestTransform | 11 | Segments, CLV range, RFM features, clustering |
| TestIntegration | 5 | End-to-end, Champions CLV, revenue totals |

---

## 📋 Airflow DAG

Scheduled pipeline at `dags/segmentation_pipeline_dag.py`:
- Runs **every Sunday at 04:00 AM UTC** (weekly re-segmentation)
- 5 tasks: extract, segment, load, dbt refresh, notify marketing
- XCom passes Champions and At Risk counts to marketing team
- Email alerts on failure with 2 retries

---

## 📊 Sample Pipeline Output

```
====================================================================
   CUSTOMER SEGMENTATION ANALYSIS — RUN SUMMARY
====================================================================
  Total Customers Analysed    : 5,000
  Clustering Algorithm        : K-Means (k=4)
  Features Used               : Recency, Frequency, Monetary
--------------------------------------------------------------------
  SEGMENT BREAKDOWN:
  Champions (95 customers — 1.9%)
    Avg Recency   : 89 days
    Avg Frequency : 10.8 purchases
    Avg Spend     : GHS 54,033.71
    Avg CLV Score : 0.2717
    Action        : Reward with VIP loyalty programme

  Loyal Customers (1,348 customers — 27.0%)
    Avg Recency   : 67 days
    Avg Frequency : 9.5 purchases
    Avg Spend     : GHS 6,262.64
    Avg CLV Score : 0.1679
    Action        : Upsell premium products

  At Risk (2,519 customers — 50.4%)
    Avg Recency   : 56 days
    Avg Frequency : 3.3 purchases
    Avg Spend     : GHS 2,168.31
    Avg CLV Score : 0.0714
    Action        : Re-engage with win-back campaign

  Lost/Inactive (1,038 customers — 20.8%)
    Avg Recency   : 238 days
    Avg Frequency : 4.7 purchases
    Avg Spend     : GHS 3,069.34
    Avg CLV Score : 0.0772
    Action        : Send reactivation email with coupon
====================================================================
```

---

## 🚀 How To Run

```bash
# 1. Clone the repo
git clone https://github.com/lawrykoomson/Customer-Segmentation-Analysis.git
cd Customer-Segmentation-Analysis

# 2. Create virtual environment with Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create PostgreSQL database
psql -U postgres -c "CREATE DATABASE customer_segments;"

# 5. Configure environment
copy .env.example .env
# Edit .env with your PostgreSQL credentials

# 6. Run the pipeline
python customer_segmentation.py

# 7. Run unit tests
pytest test_customer_segmentation.py -v

# 8. Run dbt models
cd dbt
set DBT_POSTGRES_PASSWORD=your_password
dbt run --profiles-dir .
dbt test --profiles-dir .

# 9. Run Kafka stream simulator
cd ..
python kafka_segmentation_simulator.py
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core pipeline language |
| Pandas | Data extraction and transformation |
| NumPy | Numerical operations |
| scikit-learn | K-Means clustering + StandardScaler |
| psycopg2 | PostgreSQL database connector |
| dbt-postgres | Analytical transformation layer |
| Apache Airflow | Pipeline orchestration DAG |
| Power BI | CLV and segmentation dashboard |
| pytest | Unit testing framework |
| python-dotenv | Environment variable management |

---

## 🔮 Roadmap

- [x] RFM feature engineering
- [x] K-Means clustering — 4 segments identified
- [x] PostgreSQL live load
- [x] 25 unit tests — all passing
- [x] dbt analytical layer — 4 models, 4 tests passing
- [x] Apache Airflow DAG — weekly re-segmentation
- [x] Power BI dashboard — 4 pages live
- [x] Kafka stream simulator — 3 consumer groups
- [ ] Docker containerisation
- [ ] DBSCAN or hierarchical clustering comparison

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)

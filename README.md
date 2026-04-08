# 🛒 Customer Segmentation Analysis — RFM + K-Means Clustering

![Python](https://img.shields.io/badge/Python-3.14-blue?style=flat-square&logo=python)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-1.8.0-F7931E?style=flat-square&logo=scikit-learn)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)

A machine learning pipeline that generates 50,000 Ghana retail customer records, engineers RFM (Recency, Frequency, Monetary) features, applies K-Means clustering to identify 4 distinct customer segments, and produces actionable business recommendations per segment.

---

## 🏗️ Pipeline Architecture

```
[Customer Transaction Data — 50,000 records]
              │
              ▼
     ┌─────────────────┐
     │  STEP 1         │  ← Generate realistic Ghana customer data
     │  Data Generation│     RFM metrics, demographics, regions
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │  STEP 2         │  ← Engineer RFM features
     │  Feature Eng.   │     Recency score, log-monetary, StandardScaler
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │  STEP 3         │  ← K-Means++ clustering (k=4)
     │  K-Means        │     Converges in 4 iterations
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │  STEP 4         │  ← Segment profiles + recommendations
     │  Reports        │     6 CSV exports
     └─────────────────┘
```

---

## 📊 RFM Methodology

**RFM** is the gold standard customer segmentation framework used by retail, fintech, and telecom companies worldwide:

| Feature | Description | Signal |
|---|---|---|
| **Recency** | Days since last purchase | Lower = more engaged |
| **Frequency** | Number of purchases per year | Higher = more loyal |
| **Monetary** | Total spend in GHS | Higher = more valuable |

---

## 🎯 Segments Discovered

| Segment | Customers | Revenue Share | Avg Spend | Avg Recency |
|---|---|---|---|---|
| Champions | 5,066 (10.1%) | 32.9% | GHS 8,118 | 15 days |
| Loyal Customers | 14,408 (28.8%) | 50.3% | GHS 4,362 | 53 days |
| At Risk | 13,815 (27.6%) | 11.6% | GHS 1,048 | 200 days |
| Lost / Inactive | 16,711 (33.4%) | 5.1% | GHS 382 | 528 days |

**Key insight:** 10% of customers (Champions) generate 33% of all revenue. Top 2 segments together account for 83% of GHS 124.8M total revenue.

---

## 💡 Business Recommendations Per Segment

**Champions** — Reward and retain. VIP loyalty programme, early product access, exclusive discounts. Never let them churn.

**Loyal Customers** — Upsell and cross-sell. Premium product recommendations, bundle deals, monthly personalised offers.

**At Risk** — Re-engagement campaign. Win-back email with 10–15% discount. Contact within 7 days before they become Lost.

**Lost / Inactive** — Last-chance reactivation. Aggressive 20–25% discount offer. Archive if no response in 30 days.

---

## 📊 Sample Output

```
=================================================================
  CUSTOMER SEGMENTATION ANALYSIS — STARTED
  Method: RFM Feature Engineering + K-Means Clustering
=================================================================
  Customers generated    : 50,000
  Total revenue captured : GHS 124,832,893.73

  Algorithm              : K-Means++ (k=4)
  Iterations to converge : 4
  Inertia (WCSS)         : 23,965.66

  Champions       : 5,066  (10.1%) | Recency:  15d | GHS 8,118/customer
  Loyal Customers : 14,408 (28.8%) | Recency:  53d | GHS 4,362/customer
  At Risk         : 13,815 (27.6%) | Recency: 200d | GHS 1,048/customer
  Lost / Inactive : 16,711 (33.4%) | Recency: 528d | GHS   382/customer
=================================================================
```

---

## 🚀 How To Run

```bash
git clone https://github.com/lawrykoomson/Customer-Segmentation-Analysis.git
cd Customer-Segmentation-Analysis
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python customer_segmentation.py
```

---

## 📁 Output Files

```
data/
├── raw/
│   └── customer_data.csv              ← 50,000 raw customer records
├── processed/
│   └── customer_segments_full.csv     ← Full dataset with segment labels
└── reports/
    ├── segment_profiles.csv           ← KPI summary per segment
    ├── champions_customers.csv        ← 5,066 champion customers
    ├── loyal_customers_customers.csv  ← 14,408 loyal customers
    ├── at_risk_customers.csv          ← 13,815 at-risk customers
    └── lost__inactive_customers.csv   ← 16,711 lost customers
```

---

## 📦 Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.14 | Core language |
| Pandas | Data generation and transformation |
| NumPy | Numerical operations and RFM engineering |
| Scikit-Learn | K-Means++ clustering, StandardScaler |
| Matplotlib / Seaborn | Visualisation (future) |

---

## 🔮 Future Improvements
- [ ] Elbow method plot to validate optimal k
- [ ] Silhouette score analysis
- [ ] Power BI dashboard showing segment distribution maps
- [ ] Automated weekly re-segmentation pipeline with Airflow
- [ ] Upgrade to DBSCAN for density-based segmentation

---

## 👨‍💻 Author

**Lawrence Koomson**
BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
"""
Customer Segmentation Analysis Pipeline
=========================================
Generates 50,000 Ghana retail customer records, engineers
RFM (Recency, Frequency, Monetary) features, applies
K-Means clustering to identify 4 distinct customer segments,
and loads results into PostgreSQL for analytics.

RFM Scoring:
    Recency   → How recently did the customer buy?
    Frequency → How often do they buy?
    Monetary  → How much do they spend?

Segments:
    Champions       → Best customers, buy often, spend most
    Loyal Customers → Buy regularly, good spend
    At Risk         → Used to buy but haven't recently
    Lost/Inactive   → Low recency, frequency and spend

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("segmentation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "customer_segments"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

PROCESSED_PATH = Path("data/processed/")
N_CLUSTERS     = 4
RANDOM_STATE   = 42

REGIONS   = ["Greater Accra","Ashanti","Western","Eastern","Northern","Volta"]
CHANNELS  = ["In-Store","Online","Mobile App","Agent"]
GENDERS   = ["Male","Female"]
AGE_GROUPS = ["18-24","25-34","35-44","45-54","55+"]


def extract() -> pd.DataFrame:
    logger.info("[EXTRACT] Generating synthetic customer transaction data...")
    np.random.seed(RANDOM_STATE)
    n = 50000

    reference_date = datetime(2025, 1, 1)

    customer_ids = [f"CUST{str(i).zfill(7)}" for i in range(1, 5001)]

    records = []
    for cust_id in customer_ids:
        freq = np.random.choice([1,2,3,4,5,6,7,8,9,10,15,20],
                                p=[0.10,0.12,0.13,0.12,0.11,0.10,0.09,0.08,0.06,0.04,0.03,0.02])
        last_purchase_days_ago = np.random.choice(
            [np.random.randint(1,30),
             np.random.randint(30,90),
             np.random.randint(90,180),
             np.random.randint(180,365)],
            p=[0.30, 0.30, 0.25, 0.15]
        )
        last_purchase_date = reference_date - timedelta(days=int(last_purchase_days_ago))
        total_spend        = abs(np.random.lognormal(6.0, 1.2)) * freq

        records.append({
            "customer_id":        cust_id,
            "gender":             np.random.choice(GENDERS, p=[0.48, 0.52]),
            "age_group":          np.random.choice(AGE_GROUPS, p=[0.15,0.30,0.25,0.20,0.10]),
            "region":             np.random.choice(REGIONS, p=[0.35,0.25,0.15,0.12,0.08,0.05]),
            "preferred_channel":  np.random.choice(CHANNELS, p=[0.35,0.30,0.25,0.10]),
            "last_purchase_date": last_purchase_date.date(),
            "frequency":          freq,
            "total_spend_ghs":    round(total_spend, 2),
            "avg_order_value_ghs": round(total_spend / freq, 2),
            "num_complaints":     np.random.choice([0,1,2,3], p=[0.75,0.15,0.07,0.03]),
            "momo_user":          np.random.choice([True, False], p=[0.70, 0.30]),
            "loyalty_points":     np.random.randint(0, 5000),
        })

    df = pd.DataFrame(records)
    logger.info(f"[EXTRACT] Generated {len(df):,} customer records.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] Engineering RFM features and running K-Means clustering...")

    reference_date = datetime(2025, 1, 1).date()

    df["recency_days"] = df["last_purchase_date"].apply(
        lambda x: (reference_date - x).days
    )

    df["rfm_recency"]   = df["recency_days"]
    df["rfm_frequency"] = df["frequency"]
    df["rfm_monetary"]  = df["total_spend_ghs"]

    scaler   = StandardScaler()
    rfm_scaled = scaler.fit_transform(
        df[["rfm_recency","rfm_frequency","rfm_monetary"]]
    )

    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, n_init=10)
    df["cluster_id"] = kmeans.fit_predict(rfm_scaled)

    cluster_profiles = df.groupby("cluster_id").agg(
        avg_recency  = ("rfm_recency",   "mean"),
        avg_frequency= ("rfm_frequency", "mean"),
        avg_monetary = ("rfm_monetary",  "mean"),
    ).reset_index()

    cluster_profiles["segment_name"] = cluster_profiles.apply(
        lambda row: _assign_segment(row["avg_recency"],
                                    row["avg_frequency"],
                                    row["avg_monetary"],
                                    cluster_profiles), axis=1
    )

    segment_map = dict(zip(
        cluster_profiles["cluster_id"],
        cluster_profiles["segment_name"]
    ))
    df["segment_name"] = df["cluster_id"].map(segment_map)

    segment_actions = {
        "Champions":       "Reward with VIP loyalty programme and exclusive offers",
        "Loyal Customers": "Upsell premium products and send personalised recommendations",
        "At Risk":         "Re-engage with win-back campaign and targeted discounts",
        "Lost/Inactive":   "Send reactivation email with high-value coupon",
    }
    df["retention_action"] = df["segment_name"].map(segment_actions)

    df["clv_score"] = (
        (1 / df["rfm_recency"].clip(lower=1)) * 0.3 +
        (df["rfm_frequency"] / df["rfm_frequency"].max()) * 0.3 +
        (df["rfm_monetary"]  / df["rfm_monetary"].max())  * 0.4
    ).round(4)

    df["processed_at"] = datetime.now()

    logger.info(f"[TRANSFORM] Segmentation complete.")
    for seg, count in df["segment_name"].value_counts().items():
        logger.info(f"  {seg}: {count:,} customers ({count/len(df)*100:.1f}%)")

    return df


def _assign_segment(recency, frequency, monetary, profiles):
    """Assign human-readable segment names based on RFM cluster centroids."""
    min_r = profiles["avg_recency"].min()
    max_f = profiles["avg_frequency"].max()
    max_m = profiles["avg_monetary"].max()

    r_score = 1 - (recency   - min_r) / (profiles["avg_recency"].max()   - min_r + 1)
    f_score = frequency  / (max_f + 1)
    m_score = monetary   / (max_m + 1)
    combined = r_score * 0.3 + f_score * 0.3 + m_score * 0.4

    if combined >= 0.65:   return "Champions"
    elif combined >= 0.45: return "Loyal Customers"
    elif combined >= 0.25: return "At Risk"
    else:                  return "Lost/Inactive"


def load(df: pd.DataFrame):
    logger.info("[LOAD] Attempting PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS customer_dw;

                CREATE TABLE IF NOT EXISTS customer_dw.customer_segments (
                    customer_id         VARCHAR(12) PRIMARY KEY,
                    gender              VARCHAR(10),
                    age_group           VARCHAR(10),
                    region              VARCHAR(50),
                    preferred_channel   VARCHAR(20),
                    last_purchase_date  DATE,
                    recency_days        INT,
                    frequency           SMALLINT,
                    total_spend_ghs     NUMERIC(14,2),
                    avg_order_value_ghs NUMERIC(14,2),
                    num_complaints      SMALLINT,
                    momo_user           BOOLEAN,
                    loyalty_points      INT,
                    rfm_recency         NUMERIC(8,2),
                    rfm_frequency       NUMERIC(8,2),
                    rfm_monetary        NUMERIC(14,2),
                    cluster_id          SMALLINT,
                    segment_name        VARCHAR(20),
                    retention_action    TEXT,
                    clv_score           NUMERIC(8,4),
                    processed_at        TIMESTAMP
                );
            """)
            conn.commit()

        load_cols = [
            "customer_id","gender","age_group","region","preferred_channel",
            "last_purchase_date","recency_days","frequency","total_spend_ghs",
            "avg_order_value_ghs","num_complaints","momo_user","loyalty_points",
            "rfm_recency","rfm_frequency","rfm_monetary","cluster_id",
            "segment_name","retention_action","clv_score","processed_at"
        ]

        records = [tuple(r) for r in df[load_cols].itertuples(index=False)]

        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO customer_dw.customer_segments ({','.join(load_cols)}) "
                f"VALUES %s ON CONFLICT (customer_id) DO UPDATE SET "
                f"segment_name=EXCLUDED.segment_name, "
                f"clv_score=EXCLUDED.clv_score, "
                f"processed_at=EXCLUDED.processed_at",
                records, page_size=500
            )
            conn.commit()

        conn.close()
        logger.info(f"[LOAD] Successfully loaded {len(df):,} customer segments into PostgreSQL.")

    except Exception as e:
        logger.warning(f"[LOAD] PostgreSQL unavailable ({e})")
        fallback = PROCESSED_PATH / f"segments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(fallback, index=False)
        logger.info(f"[LOAD] Saved to {fallback}")


def print_summary(df: pd.DataFrame):
    print("\n" + "="*68)
    print("   CUSTOMER SEGMENTATION ANALYSIS — RUN SUMMARY")
    print("="*68)
    print(f"  Total Customers Analysed    : {len(df):,}")
    print(f"  Clustering Algorithm        : K-Means (k={N_CLUSTERS})")
    print(f"  Features Used               : Recency, Frequency, Monetary")
    print("-"*68)
    print("  SEGMENT BREAKDOWN:")
    for seg in ["Champions","Loyal Customers","At Risk","Lost/Inactive"]:
        seg_df = df[df["segment_name"] == seg]
        if len(seg_df) == 0:
            continue
        pct = len(seg_df) / len(df) * 100
        print(f"\n  {seg} ({len(seg_df):,} customers — {pct:.1f}%)")
        print(f"    Avg Recency   : {seg_df['recency_days'].mean():.0f} days")
        print(f"    Avg Frequency : {seg_df['frequency'].mean():.1f} purchases")
        print(f"    Avg Spend     : GHS {seg_df['total_spend_ghs'].mean():,.2f}")
        print(f"    Avg CLV Score : {seg_df['clv_score'].mean():.4f}")
        print(f"    Action        : {seg_df['retention_action'].iloc[0]}")
    print("-"*68)
    print("  REVENUE BY SEGMENT:")
    seg_rev = df.groupby("segment_name")["total_spend_ghs"].sum().sort_values(ascending=False)
    for seg, val in seg_rev.items():
        print(f"    {seg:<20} : GHS {val:,.2f}")
    print("="*68 + "\n")


def run_pipeline():
    logger.info("=" * 62)
    logger.info("  CUSTOMER SEGMENTATION PIPELINE — STARTED")
    logger.info("=" * 62)
    start = datetime.now()
    df    = extract()
    df    = transform(df)
    load(df)
    print_summary(df)
    duration = (datetime.now() - start).total_seconds()
    logger.info(f"PIPELINE COMPLETED in {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()
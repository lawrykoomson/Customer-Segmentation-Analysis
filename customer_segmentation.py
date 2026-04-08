"""
Customer Segmentation Analysis
================================
Generates 50,000 Ghana retail customer records, engineers RFM
features, applies K-Means clustering to identify 4 distinct
customer segments, and produces business recommendations.

RFM = Recency, Frequency, Monetary
    Recency   → How recently did the customer buy?
    Frequency → How often do they buy?
    Monetary  → How much do they spend?

Segments:
    0 → Champions        — best customers, buy often, spend most
    1 → Loyal Customers  — buy regularly, good spend
    2 → At Risk          — used to buy but haven't recently
    3 → Lost/Inactive    — low recency, low frequency, low spend

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
np.random.seed(42)
N_CUSTOMERS  = 50000
N_CLUSTERS   = 4
SNAPSHOT_DATE = datetime(2025, 1, 1)  # Reference date for recency

REPORTS_PATH   = Path("data/reports/")
PROCESSED_PATH = Path("data/processed/")
REPORTS_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

GHANA_REGIONS = [
    "Greater Accra", "Ashanti", "Western",
    "Eastern", "Central", "Northern", "Volta"
]
PRODUCT_CATEGORIES = [
    "Electronics", "Food & Grocery", "Clothing",
    "Home & Kitchen", "Health & Beauty", "Stationery"
]
GENDERS   = ["M", "F"]
AGE_RANGE = (18, 65)


# ─────────────────────────────────────────────
#  STEP 1 — GENERATE DATA
# ─────────────────────────────────────────────
def generate_customer_data() -> pd.DataFrame:
    """
    Generate realistic Ghana retail customer transaction data.
    Each record represents one customer's purchase history summary.
    """
    print("\n" + "="*65)
    print("  STEP 1/4 — GENERATING CUSTOMER DATA")
    print("="*65)

    # Customer demographics
    customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, N_CUSTOMERS + 1)]
    regions      = np.random.choice(GHANA_REGIONS, N_CUSTOMERS,
                                     p=[0.30,0.25,0.15,0.12,0.08,0.06,0.04])
    genders      = np.random.choice(GENDERS, N_CUSTOMERS)
    ages         = np.random.randint(AGE_RANGE[0], AGE_RANGE[1], N_CUSTOMERS)
    join_dates   = [
        datetime(2020, 1, 1) + timedelta(days=int(d))
        for d in np.random.randint(0, 1460, N_CUSTOMERS)
    ]
    fav_category = np.random.choice(PRODUCT_CATEGORIES, N_CUSTOMERS)

    # ── RFM core metrics
    # Recency: days since last purchase (lower = better)
    # Champions buy very recently (1-30 days)
    # Lost customers haven't bought in 300+ days
    recency_days = np.where(
        np.random.rand(N_CUSTOMERS) < 0.15,
        np.random.randint(1, 30, N_CUSTOMERS),      # 15% Champions
        np.where(
            np.random.rand(N_CUSTOMERS) < 0.30,
            np.random.randint(30, 90, N_CUSTOMERS),  # 30% Loyal
            np.where(
                np.random.rand(N_CUSTOMERS) < 0.35,
                np.random.randint(90, 270, N_CUSTOMERS),  # 35% At Risk
                np.random.randint(270, 730, N_CUSTOMERS)  # 20% Lost
            )
        )
    )

    # Frequency: number of purchases in last 12 months
    frequency = np.where(
        recency_days < 30,
        np.random.randint(10, 50, N_CUSTOMERS),   # Champions buy often
        np.where(
            recency_days < 90,
            np.random.randint(4, 15, N_CUSTOMERS), # Loyal: moderate
            np.where(
                recency_days < 270,
                np.random.randint(1, 5, N_CUSTOMERS),  # At risk: declining
                np.random.randint(1, 3, N_CUSTOMERS)   # Lost: rare
            )
        )
    )

    # Monetary: total spend in GHS
    monetary = np.where(
        recency_days < 30,
        np.abs(np.random.normal(8000, 3000, N_CUSTOMERS)),   # Champions: high spend
        np.where(
            recency_days < 90,
            np.abs(np.random.normal(3500, 1500, N_CUSTOMERS)), # Loyal: good spend
            np.where(
                recency_days < 270,
                np.abs(np.random.normal(1200, 600, N_CUSTOMERS)),  # At risk: dropping
                np.abs(np.random.normal(400, 200, N_CUSTOMERS))    # Lost: very low
            )
        )
    ).round(2)

    # Avg order value
    avg_order_value = (monetary / np.maximum(frequency, 1)).round(2)

    # Last purchase date
    last_purchase = [
        SNAPSHOT_DATE - timedelta(days=int(d))
        for d in recency_days
    ]

    df = pd.DataFrame({
        "customer_id":      customer_ids,
        "region":           regions,
        "gender":           genders,
        "age":              ages,
        "join_date":        [d.strftime("%Y-%m-%d") for d in join_dates],
        "last_purchase":    [d.strftime("%Y-%m-%d") for d in last_purchase],
        "fav_category":     fav_category,
        "recency_days":     recency_days,
        "frequency":        frequency,
        "monetary_ghs":     monetary,
        "avg_order_value":  avg_order_value,
    })

    # Save raw data
    raw_path = Path("data/raw/customer_data.csv")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(raw_path, index=False)

    print(f"  Customers generated    : {len(df):,}")
    print(f"  Regions covered        : {df['region'].nunique()}")
    print(f"  Avg recency (days)     : {df['recency_days'].mean():.1f}")
    print(f"  Avg frequency          : {df['frequency'].mean():.1f} purchases")
    print(f"  Avg monetary spend     : GHS {df['monetary_ghs'].mean():,.2f}")
    print(f"  Total revenue captured : GHS {df['monetary_ghs'].sum():,.2f}")
    return df


# ─────────────────────────────────────────────
#  STEP 2 — RFM FEATURE ENGINEERING
# ─────────────────────────────────────────────
def engineer_rfm_features(df: pd.DataFrame) -> tuple:
    """
    Scale RFM features for K-Means clustering.
    Recency is inverted so higher = more recent (better).
    """
    print("\n" + "="*65)
    print("  STEP 2/4 — RFM FEATURE ENGINEERING")
    print("="*65)

    # RFM score matrix
    rfm = df[["customer_id","recency_days","frequency","monetary_ghs"]].copy()

    # Invert recency — lower days = higher score
    rfm["recency_score"] = rfm["recency_days"].max() - rfm["recency_days"]

    # Log-transform monetary to reduce skew
    rfm["monetary_log"] = np.log1p(rfm["monetary_ghs"])

    # Features for clustering
    features = rfm[["recency_score","frequency","monetary_log"]].values

    # Standardise — zero mean, unit variance
    scaler          = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    print(f"  RFM matrix shape       : {features.shape}")
    print(f"  Features               : Recency Score, Frequency, Monetary (log-scaled)")
    print(f"  Scaling                : StandardScaler applied")
    print(f"  Recency range          : {rfm['recency_days'].min()}–{rfm['recency_days'].max()} days")
    print(f"  Frequency range        : {rfm['frequency'].min()}–{rfm['frequency'].max()} purchases")
    print(f"  Monetary range         : GHS {rfm['monetary_ghs'].min():,.2f}–GHS {rfm['monetary_ghs'].max():,.2f}")

    return rfm, features_scaled, scaler


# ─────────────────────────────────────────────
#  STEP 3 — K-MEANS CLUSTERING
# ─────────────────────────────────────────────
def run_kmeans(df: pd.DataFrame, rfm: pd.DataFrame,
               features_scaled: np.ndarray) -> pd.DataFrame:
    """
    Apply K-Means clustering with k=4 segments.
    Labels each customer with their segment.
    """
    print("\n" + "="*65)
    print("  STEP 3/4 — K-MEANS CLUSTERING")
    print("="*65)

    # Fit K-Means
    kmeans = KMeans(
        n_clusters=N_CLUSTERS,
        init="k-means++",
        n_init=10,
        max_iter=300,
        random_state=42
    )
    kmeans.fit(features_scaled)

    print(f"  Algorithm              : K-Means++ (k={N_CLUSTERS})")
    print(f"  Iterations to converge : {kmeans.n_iter_}")
    print(f"  Inertia (WCSS)         : {kmeans.inertia_:,.2f}")

    # Add cluster labels to RFM
    rfm["cluster"] = kmeans.labels_

    # Compute cluster centroids in original scale
    centroids = rfm.groupby("cluster").agg(
        avg_recency_days = ("recency_days", "mean"),
        avg_frequency    = ("frequency", "mean"),
        avg_monetary     = ("monetary_ghs", "mean"),
        count            = ("customer_id", "count"),
    ).round(2)

    # ── Assign meaningful segment names based on centroids
    # Champion = lowest recency days, highest frequency + monetary
    # We rank clusters by monetary spend to assign labels
    centroids["monetary_rank"] = centroids["avg_monetary"].rank(ascending=False)
    centroids["recency_rank"]  = centroids["avg_recency_days"].rank(ascending=True)

    segment_map = {}
    sorted_by_monetary = centroids.sort_values("avg_monetary", ascending=False)

    labels = ["Champions", "Loyal Customers", "At Risk", "Lost / Inactive"]
    for i, (cluster_id, _) in enumerate(sorted_by_monetary.iterrows()):
        segment_map[cluster_id] = labels[i]

    rfm["segment"] = rfm["cluster"].map(segment_map)

    # Merge back to full dataset
    df = df.merge(rfm[["customer_id","cluster","segment"]], on="customer_id", how="left")

    print(f"\n  CLUSTER SUMMARY:")
    for cluster_id in sorted(centroids.index):
        seg   = segment_map[cluster_id]
        row   = centroids.loc[cluster_id]
        pct   = row["count"] / N_CUSTOMERS * 100
        print(f"  Cluster {cluster_id} → {seg:<20} | "
              f"n={int(row['count']):>6,} ({pct:.1f}%) | "
              f"Recency: {row['avg_recency_days']:>6.1f}d | "
              f"Freq: {row['avg_frequency']:>5.1f} | "
              f"GHS {row['avg_monetary']:>8,.2f}")

    return df, kmeans, centroids, segment_map


# ─────────────────────────────────────────────
#  STEP 4 — ANALYSIS & REPORTING
# ─────────────────────────────────────────────
def generate_reports(df: pd.DataFrame, centroids: pd.DataFrame,
                     segment_map: dict):
    """
    Generate full segmentation reports with business recommendations.
    Exports CSV files for each segment and summary reports.
    """
    print("\n" + "="*65)
    print("  STEP 4/4 — SEGMENT ANALYSIS & BUSINESS RECOMMENDATIONS")
    print("="*65)

    total_revenue = df["monetary_ghs"].sum()

    # ── Segment profiles
    segment_profiles = df.groupby("segment").agg(
        Customers        = ("customer_id", "count"),
        Avg_Recency_Days = ("recency_days", "mean"),
        Avg_Frequency    = ("frequency", "mean"),
        Avg_Spend_GHS    = ("monetary_ghs", "mean"),
        Total_Revenue    = ("monetary_ghs", "sum"),
        Avg_Order_Value  = ("avg_order_value", "mean"),
    ).round(2).reset_index()

    segment_profiles["Revenue_Share_%"] = (
        segment_profiles["Total_Revenue"] / total_revenue * 100
    ).round(1)

    # ── Business recommendations per segment
    recommendations = {
        "Champions": (
            "Reward and retain — offer VIP loyalty programme, "
            "early access to new products, and exclusive discounts. "
            "These customers drive the most revenue. Never let them churn."
        ),
        "Loyal Customers": (
            "Upsell and cross-sell — recommend premium products and "
            "bundle deals. Enroll in loyalty points programme. "
            "Target with monthly personalised offers."
        ),
        "At Risk": (
            "Re-engagement campaign — send win-back emails with "
            "time-limited discount (10-15% off next purchase). "
            "Contact within 7 days before they move to Lost."
        ),
        "Lost / Inactive": (
            "Last-chance reactivation — aggressive discount offer "
            "(20-25% off), survey to understand why they left. "
            "If no response in 30 days, archive from active campaigns."
        ),
    }

    # ── Print full report
    print(f"\n  Total Customers Analysed : {len(df):,}")
    print(f"  Total Revenue Captured   : GHS {total_revenue:,.2f}")
    print(f"  Segmentation Method      : K-Means Clustering (k={N_CLUSTERS})")
    print(f"  Features Used            : Recency, Frequency, Monetary (RFM)")

    segment_order = ["Champions","Loyal Customers","At Risk","Lost / Inactive"]
    for seg in segment_order:
        row = segment_profiles[segment_profiles["segment"] == seg]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        print(f"\n  {'─'*62}")
        print(f"  SEGMENT: {seg.upper()}")
        print(f"  {'─'*62}")
        print(f"  Customers        : {int(row['Customers']):,} ({row['Revenue_Share_%']}% of revenue)")
        print(f"  Avg Recency      : {row['Avg_Recency_Days']:.1f} days since last purchase")
        print(f"  Avg Frequency    : {row['Avg_Frequency']:.1f} purchases per year")
        print(f"  Avg Spend        : GHS {row['Avg_Spend_GHS']:,.2f}")
        print(f"  Avg Order Value  : GHS {row['Avg_Order_Value']:,.2f}")
        print(f"  Total Revenue    : GHS {row['Total_Revenue']:,.2f}")
        print(f"  Recommendation   : {recommendations[seg]}")

    # ── Regional breakdown by segment
    print(f"\n  {'─'*62}")
    print(f"  CHAMPIONS BY REGION")
    print(f"  {'─'*62}")
    champions_region = (
        df[df["segment"] == "Champions"]
        .groupby("region")["customer_id"].count()
        .sort_values(ascending=False)
    )
    for region, count in champions_region.items():
        pct = count / (df["segment"] == "Champions").sum() * 100
        print(f"  {region:<20} : {count:,} champions ({pct:.1f}%)")

    # ── Gender breakdown
    print(f"\n  {'─'*62}")
    print(f"  SEGMENT × GENDER BREAKDOWN")
    print(f"  {'─'*62}")
    gender_seg = df.groupby(["segment","gender"])["customer_id"].count().unstack()
    for seg in segment_order:
        if seg in gender_seg.index:
            row = gender_seg.loc[seg]
            total = row.sum()
            print(f"  {seg:<22} : M={int(row.get('M',0)):>5,} ({int(row.get('M',0))/total*100:.0f}%)  "
                  f"F={int(row.get('F',0)):>5,} ({int(row.get('F',0))/total*100:.0f}%)")

    # ── Favourite category by segment
    print(f"\n  {'─'*62}")
    print(f"  TOP PRODUCT CATEGORY PER SEGMENT")
    print(f"  {'─'*62}")
    for seg in segment_order:
        seg_df  = df[df["segment"] == seg]
        top_cat = seg_df["fav_category"].value_counts().index[0]
        top_pct = seg_df["fav_category"].value_counts().iloc[0] / len(seg_df) * 100
        print(f"  {seg:<22} : {top_cat} ({top_pct:.1f}%)")

    # ── Export reports
    print(f"\n  {'─'*62}")
    print(f"  EXPORTING REPORTS")
    print(f"  {'─'*62}")

    # Full segmented dataset
    out = PROCESSED_PATH / "customer_segments_full.csv"
    df.to_csv(out, index=False)
    print(f"  Full dataset saved     : {out}")

    # Summary profile
    out2 = REPORTS_PATH / "segment_profiles.csv"
    segment_profiles.to_csv(out2, index=False)
    print(f"  Segment profiles saved : {out2}")

    # One CSV per segment
    for seg in segment_order:
        seg_df   = df[df["segment"] == seg]
        filename = seg.lower().replace(" ","_").replace("/","") + "_customers.csv"
        out3     = REPORTS_PATH / filename
        seg_df.to_csv(out3, index=False)
        print(f"  {seg:<22} : {out3} ({len(seg_df):,} customers)")

    print(f"\n{'='*65}")
    print(f"  SEGMENTATION COMPLETE")
    print(f"{'='*65}\n")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def run_segmentation():
    start = datetime.now()

    print("\n" + "="*65)
    print("  CUSTOMER SEGMENTATION ANALYSIS — STARTED")
    print("  Method: RFM Feature Engineering + K-Means Clustering")
    print("="*65)

    df                          = generate_customer_data()
    rfm, features_scaled, scaler = engineer_rfm_features(df)
    df, kmeans, centroids, seg_map = run_kmeans(df, rfm, features_scaled)
    generate_reports(df, centroids, seg_map)

    duration = (datetime.now() - start).total_seconds()
    print(f"  Total runtime: {duration:.2f} seconds")


if __name__ == "__main__":
    run_segmentation()
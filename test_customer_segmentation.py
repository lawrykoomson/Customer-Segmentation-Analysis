"""
Unit Tests — Customer Segmentation Analysis Pipeline
======================================================
Run with: pytest test_customer_segmentation.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from customer_segmentation import extract, transform


class TestExtract:

    def test_returns_dataframe(self):
        df = extract()
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = extract()
        assert len(df) == 5000

    def test_required_columns_present(self):
        df = extract()
        required = [
            "customer_id","gender","age_group","region",
            "preferred_channel","last_purchase_date","frequency",
            "total_spend_ghs","avg_order_value_ghs",
            "num_complaints","momo_user","loyalty_points"
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_customer_ids_unique(self):
        df = extract()
        assert df["customer_id"].nunique() == len(df)

    def test_frequency_positive(self):
        df = extract()
        assert (df["frequency"] > 0).all()

    def test_spend_positive(self):
        df = extract()
        assert (df["total_spend_ghs"] > 0).all()

    def test_regions_valid(self):
        df = extract()
        valid = {"Greater Accra","Ashanti","Western","Eastern","Northern","Volta"}
        assert set(df["region"].unique()).issubset(valid)

    def test_channels_valid(self):
        df = extract()
        valid = {"In-Store","Online","Mobile App","Agent"}
        assert set(df["preferred_channel"].unique()).issubset(valid)

    def test_genders_valid(self):
        df = extract()
        assert set(df["gender"].unique()).issubset({"Male","Female"})


class TestTransform:

    @pytest.fixture
    def transformed(self):
        df = extract()
        return transform(df)

    def test_segment_names_valid(self, transformed):
        valid = {"Champions","Loyal Customers","At Risk","Lost/Inactive"}
        assert set(transformed["segment_name"].unique()).issubset(valid)

    def test_four_segments_produced(self, transformed):
        assert transformed["segment_name"].nunique() == 4

    def test_clv_score_range(self, transformed):
        assert transformed["clv_score"].between(0, 1).all()

    def test_recency_days_positive(self, transformed):
        assert (transformed["recency_days"] >= 0).all()

    def test_rfm_columns_exist(self, transformed):
        for col in ["rfm_recency","rfm_frequency","rfm_monetary"]:
            assert col in transformed.columns

    def test_cluster_id_range(self, transformed):
        assert transformed["cluster_id"].between(0, 3).all()

    def test_retention_action_populated(self, transformed):
        assert transformed["retention_action"].isna().sum() == 0

    def test_processed_at_exists(self, transformed):
        assert "processed_at" in transformed.columns

    def test_row_count_preserved(self, transformed):
        df = extract()
        assert len(transformed) == len(df)

    def test_no_null_segment_names(self, transformed):
        assert transformed["segment_name"].isna().sum() == 0

    def test_champions_highest_spend(self, transformed):
        champ_spend = transformed[transformed["segment_name"] == "Champions"]["total_spend_ghs"].mean()
        loyal_spend = transformed[transformed["segment_name"] == "Loyal Customers"]["total_spend_ghs"].mean()
        assert champ_spend > loyal_spend


class TestIntegration:

    def test_full_pipeline_runs(self):
        df     = extract()
        result = transform(df)
        assert len(result) == len(df)

    def test_all_customers_have_segment(self):
        df     = extract()
        result = transform(df)
        assert result["segment_name"].isna().sum() == 0

    def test_no_duplicate_customer_ids(self):
        df     = extract()
        result = transform(df)
        assert result["customer_id"].duplicated().sum() == 0

    def test_champions_have_highest_clv(self):
        df     = extract()
        result = transform(df)
        champ_clv = result[result["segment_name"] == "Champions"]["clv_score"].mean()
        other_clv = result[result["segment_name"] != "Champions"]["clv_score"].mean()
        assert champ_clv > other_clv

    def test_total_revenue_positive(self):
        df     = extract()
        result = transform(df)
        assert result["total_spend_ghs"].sum() > 0
# PROJECT 2 GOLD LAYER - CHARGE ANALYTICS & GAP ANALYSIS
## Silver → Gold: Identify $2M Unbilled Charges, Provider Performance, Charge Gaps

---

## Overview

The Gold layer creates business-ready charge analytics:
- **Fact: Daily Charge Summary** - Charge aggregations by date/patient/provider/procedure
- **Fact: Charge Gap Analysis** - Expected vs actual charges (identifies $2M opportunity)
- **Fact: Provider Performance** - Quality metrics, capture completeness, billing accuracy
- **Fact: Unbilled Charges** - High-value charges pending billing with age tracking
- **Dim: Charge Master** - Reference for CPT codes and pricing

All using incremental MERGE for efficiency.

---

## Complete Gold Layer Code

```python
# file: gold_charge_analytics.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    to_date, date_trunc, lag, current_timestamp, lit, when,
    round as spark_round, floor, coalesce, datediff, row_number,
    collect_list, array_contains
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_charge_analytics")


class GoldChargeAnalytics:
    """
    Gold layer analytics for multi-source charges
    
    Creates:
    - Daily charge summaries
    - Charge gap analysis ($2M recovery opportunity)
    - Provider performance metrics
    - Unbilled charge tracking
    - CPT code reference dimension
    """

    def __init__(self, spark: SparkSession,
                 silver_unified_path: str,
                 gold_base_path: str):

        self.spark = spark
        self.silver_unified_path = silver_unified_path.rstrip("/")
        self.gold_base_path = gold_base_path.rstrip("/")

    # ---------- READ SILVER ----------

    def read_silver_charges(self):
        logger.info(f"Reading Silver charges from {self.silver_unified_path}")
        df = self.spark.read.format("delta").load(self.silver_unified_path)
        logger.info(f"Silver charges count: {df.count()}")
        return df

    # ---------- FACT 1: DAILY CHARGE SUMMARY ----------

    def build_fact_daily_charge_summary(self, df_silver):
        """
        Grain: charge_date, patient_id, provider_id, procedure_code
        Aggregates charges by these dimensions
        """
        logger.info("Building fact_daily_charge_summary...")

        df_summary = (
            df_silver
            .withColumn("charge_date", to_date(col("charge_date")))
            .groupBy(
                col("charge_date"),
                col("patient_id"),
                col("provider_id"),
                col("procedure_code"),
                col("billable_status")
            )
            .agg(
                count("*").alias("charge_count"),
                spark_sum("charge_amount").alias("total_charge_amount"),
                avg("charge_amount").alias("avg_charge_amount"),
                spark_max("charge_amount").alias("max_charge_amount"),
                spark_min("charge_amount").alias("min_charge_amount"),
                collect_list("source_system").alias("source_systems"),
                avg("data_quality_score").alias("avg_quality_score"),
            )

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_daily_charge_summary rows: {df_summary.count()}")
        self._merge_to_gold(
            df_summary,
            table_name="fact_daily_charge_summary",
            merge_keys=["charge_date", "patient_id", "provider_id", "procedure_code", "billable_status"]
        )

    # ---------- FACT 2: CHARGE GAP ANALYSIS ($2M OPPORTUNITY) ----------

    def build_fact_charge_gap_analysis(self, df_silver):
        """
        Grain: charge_date, patient_id
        
        Compares:
        - Expected charges (all procedures in EHR with billable=true)
        - Actual charges captured (in billing system)
        - Gap = Expected - Actual (unbilled revenue at risk)
        
        This is where the $2M opportunity is identified
        """
        logger.info("Building fact_charge_gap_analysis (the $2M discovery)...")

        # Expected charges (from EHR events with billable flag)
        df_expected = (
            df_silver
            .filter((col("source_system") == "JSON_API_EHR") & 
                   (col("billable_status") == "BILLABLE"))
            .withColumn("charge_date", to_date(col("charge_date")))
            .groupBy("charge_date", "patient_id")
            .agg(
                spark_sum("charge_amount").alias("expected_charges"),
                count("*").alias("expected_charge_count"),
            )
        )

        # Actual charges (from billing system)
        df_actual = (
            df_silver
            .filter(col("source_system") == "SQL_SERVER")
            .withColumn("charge_date", to_date(col("charge_date")))
            .groupBy("charge_date", "patient_id")
            .agg(
                spark_sum(when(col("billable_status") == "BILLABLE", 
                             col("charge_amount")).otherwise(0))
                  .alias("actual_charges"),
                count("*").alias("actual_charge_count"),
            )
        )

        # Join and calculate gap
        df_gap = (
            df_expected.join(
                df_actual,
                ["charge_date", "patient_id"],
                "left"
            )
            .fillna(0, subset=["actual_charges"])

            # Gap calculation
            .withColumn("gap_amount",
                        col("expected_charges") - col("actual_charges"))
            .withColumn("gap_percentage",
                        when(col("expected_charges") > 0,
                             spark_round((col("gap_amount") / col("expected_charges")) * 100, 2))
                        .otherwise(lit(0)))

            # Gap classification
            .withColumn("gap_reason",
                        when(col("gap_amount") > 0, lit("UNBILLED_REVENUE"))
                        .when(col("gap_amount") == 0, lit("NO_GAP"))
                        .otherwise(lit("OVERBILLED")))

            # High priority if gap > $100
            .withColumn("high_priority",
                        when(col("gap_amount") > 100, lit(True))
                        .otherwise(lit(False)))

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_charge_gap_analysis rows: {df_gap.count()}")
        logger.info(f"Total gap identified: ${df_gap.agg(spark_sum('gap_amount')).collect()[0][0]}")

        self._merge_to_gold(
            df_gap,
            table_name="fact_charge_gap_analysis",
            merge_keys=["charge_date", "patient_id"]
        )

    # ---------- FACT 3: PROVIDER PERFORMANCE ----------

    def build_fact_provider_performance(self, df_silver):
        """
        Grain: month_date, provider_id
        Provider-level metrics:
        - Billing accuracy
        - Charge capture completeness
        - Days to bill
        - Quality metrics
        """
        logger.info("Building fact_provider_performance...")

        df_provider = (
            df_silver
            .withColumn("month_date", date_trunc("month", col("charge_date")))
            .groupBy(
                col("month_date"),
                col("provider_id")
            )
            .agg(
                count(col("patient_id")).alias("patient_count"),
                count(col("procedure_code")).alias("total_charges"),
                spark_sum(when(col("billable_status") == "BILLABLE", 1).otherwise(0))
                  .alias("billable_count"),
                spark_sum(when(col("billable_status") == "UNBILLED", 1).otherwise(0))
                  .alias("unbilled_count"),
                spark_sum("charge_amount").alias("total_amount"),
                avg("data_quality_score").alias("avg_quality_score"),
            )

            # Calculate KPIs
            .withColumn("billing_accuracy_pct",
                        when(col("total_charges") > 0,
                             spark_round((col("billable_count") / col("total_charges")) * 100, 2))
                        .otherwise(lit(0)))

            .withColumn("charge_capture_completeness",
                        when(col("total_charges") > 0,
                             spark_round((col("billable_count") / col("total_charges")) * 100, 2))
                        .otherwise(lit(0)))

            .withColumn("unbilled_pct",
                        when(col("total_charges") > 0,
                             spark_round((col("unbilled_count") / col("total_charges")) * 100, 2))
                        .otherwise(lit(0)))

            # Provider tier
            .withColumn("provider_tier",
                        when(col("total_charges") > 1000, lit("HIGH_VOLUME"))
                        .when(col("total_charges") > 500, lit("MEDIUM_VOLUME"))
                        .otherwise(lit("LOW_VOLUME")))

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_provider_performance rows: {df_provider.count()}")
        self._merge_to_gold(
            df_provider,
            table_name="fact_provider_performance",
            merge_keys=["month_date", "provider_id"]
        )

    # ---------- FACT 4: UNBILLED CHARGES (REVENUE AT RISK) ----------

    def build_fact_unbilled_charges(self, df_silver):
        """
        High-value fact table for revenue recovery
        Tracks all unbilled charges with:
        - Age of charge (how long pending)
        - Revenue impact
        - Recommended action
        """
        logger.info("Building fact_unbilled_charges (revenue recovery focus)...")

        current_date = to_date(current_timestamp())

        df_unbilled = (
            df_silver
            .filter(col("billable_status") == "UNBILLED")
            .withColumn("charge_date", to_date(col("charge_date")))

            # Age calculation
            .withColumn("days_unbilled",
                        datediff(lit(current_date), col("charge_date")))

            # Aging bucket
            .withColumn("age_bucket",
                        when(col("days_unbilled") <= 30, lit("0-30_days"))
                        .when(col("days_unbilled") <= 90, lit("31-90_days"))
                        .when(col("days_unbilled") <= 180, lit("91-180_days"))
                        .otherwise(lit("180+_days")))

            # Priority (older = higher priority)
            .withColumn("priority_score",
                        floor(col("days_unbilled") / 10) + 
                        (when(col("charge_amount") > 1000, 20).otherwise(0)))

            # Recommended action
            .withColumn("recommended_action",
                        when(col("days_unbilled") > 180, lit("WRITE_OFF_REVIEW"))
                        .when(col("days_unbilled") > 90, lit("FOLLOW_UP_AGGRESSIVE"))
                        .when(col("days_unbilled") > 30, lit("FOLLOW_UP_ROUTINE"))
                        .otherwise(lit("MONITOR")))

            # Next review date
            .withColumn("next_review_days",
                        when(col("days_unbilled") > 90, lit(7))
                        .when(col("days_unbilled") > 30, lit(14))
                        .otherwise(lit(30)))

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_unbilled_charges rows: {df_unbilled.count()}")
        total_at_risk = df_unbilled.agg(spark_sum("charge_amount")).collect()[0][0]
        logger.info(f"Total revenue at risk (unbilled): ${total_at_risk}")

        self._merge_to_gold(
            df_unbilled,
            table_name="fact_unbilled_charges",
            merge_keys=["source_charge_id"]
        )

    # ---------- DIMENSION: CHARGE MASTER (CPT REFERENCE) ----------

    def build_dim_charge_master(self, df_silver):
        """
        CPT code reference dimension
        Unified view of all procedures with pricing and billable status
        """
        logger.info("Building dim_charge_master...")

        df_dim = (
            df_silver
            .groupBy("procedure_code")
            .agg(
                count("*").alias("usage_count"),
                avg("charge_amount").alias("avg_charge"),
                spark_max("charge_amount").alias("max_charge"),
                spark_min("charge_amount").alias("min_charge"),
                avg("data_quality_score").alias("avg_quality"),
                collect_list("billable_status").alias("billable_statuses"),
            )

            # Determine if typically billable
            .withColumn("typically_billable",
                        when(array_contains(col("billable_statuses"), "BILLABLE"), 
                             lit(True))
                        .otherwise(lit(False)))

            .withColumn("_snapshot_date", to_date(current_timestamp()))
            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"dim_charge_master rows: {df_dim.count()}")
        self._merge_to_gold(
            df_dim,
            table_name="dim_charge_master",
            merge_keys=["procedure_code"]
        )

    # ---------- GENERIC MERGE ----------

    def _merge_to_gold(self, df_source, table_name: str, merge_keys: list):
        """Generic incremental MERGE into Gold"""
        if df_source.rdd.isEmpty():
            logger.info(f"No rows for {table_name}")
            return

        target_path = f"{self.gold_base_path}/{table_name}"
        logger.info(f"Merging into Gold: {target_path}")

        cond_parts = [f"t.{k} = s.{k}" for k in merge_keys]
        merge_condition = " AND ".join(cond_parts)

        try:
            target = DeltaTable.forPath(self.spark, target_path)
            (
                target.alias("t")
                .merge(df_source.alias("s"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            logger.info(f"Creating new Gold table: {table_name}")
            writer = df_source.write.format("delta").mode("overwrite")
            
            if "charge_date" in df_source.columns:
                writer = writer.partitionBy("charge_date")
            elif "month_date" in df_source.columns:
                writer = writer.partitionBy("month_date")
            
            writer.save(target_path)

        logger.info(f"Merge completed: {df_source.count()} rows")

    # ---------- RUN ----------

    def run(self):
        """Execute full Gold layer pipeline"""
        df_silver = self.read_silver_charges()
        
        self.build_fact_daily_charge_summary(df_silver)
        self.build_fact_charge_gap_analysis(df_silver)
        self.build_fact_provider_performance(df_silver)
        self.build_fact_unbilled_charges(df_silver)
        self.build_dim_charge_master(df_silver)

        logger.info("Gold layer pipeline completed")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("gold_charge_analytics")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )

    job = GoldChargeAnalytics(
        spark=spark,
        silver_unified_path="abfss://silver@charges.dfs.core.windows.net/charges_unified",
        gold_base_path="abfss://gold@charges.dfs.core.windows.net",
    )

    job.run()
```

---

## Key Analytics Features

### 1. Daily Charge Summary
**Use**: Operational reporting, charge trending
```sql
SELECT 
    charge_date, provider_id,
    COUNT(*) as charge_count,
    SUM(total_charge_amount) as daily_total,
    AVG(avg_quality_score) as quality_score
FROM gold.fact_daily_charge_summary
WHERE charge_date >= CURRENT_DATE - 30
GROUP BY charge_date, provider_id
```

### 2. Charge Gap Analysis ($2M Opportunity)
**Use**: Revenue recovery, gap investigation
```sql
SELECT 
    charge_date, patient_id,
    expected_charges,
    actual_charges,
    gap_amount,
    gap_reason,
    high_priority
FROM gold.fact_charge_gap_analysis
WHERE gap_amount > 0
ORDER BY gap_amount DESC
LIMIT 100
```

### 3. Provider Performance
**Use**: Quality scorecards, network management
```sql
SELECT 
    provider_id, month_date,
    total_charges,
    billing_accuracy_pct,
    charge_capture_completeness,
    provider_tier,
    avg_quality_score
FROM gold.fact_provider_performance
WHERE month_date >= DATE_TRUNC(MONTH, CURRENT_DATE - INTERVAL 12 MONTH)
```

### 4. Unbilled Charges (Revenue at Risk)
**Use**: Revenue recovery actions
```sql
SELECT 
    source_charge_id, patient_id, provider_id,
    charge_amount, days_unbilled,
    age_bucket, priority_score,
    recommended_action
FROM gold.fact_unbilled_charges
WHERE recommended_action != 'MONITOR'
ORDER BY priority_score DESC
```

---

## Databricks Workflow

```yaml
name: "gold_charge_analytics_daily"
schedule:
  quartz_cron_expression: "0 8 * * ? *"  # 8 AM daily
tasks:
  - task_key: "gold_analytics"
    notebook_task:
      notebook_path: "/Users/data_engineer/gold_charge_analytics"
    new_cluster:
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 16
    timeout_seconds: 3600
```


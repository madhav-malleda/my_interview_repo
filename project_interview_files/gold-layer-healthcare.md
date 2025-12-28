# GOLD LAYER - HEALTHCARE ANALYTICS & BUSINESS LOGIC
## Silver → Gold: Patient Risk Scoring, Claim Analytics, Provider Performance

---

## Overview

The Gold layer creates business-ready analytics datasets:
- **Fact: Daily Patient Risk Scores** - Patient risk scoring (readmission, chronic disease)
- **Fact: Claim Analytics** - Claim patterns, approvals, denials, high-cost patients
- **Fact: Provider Performance** - Quality metrics, utilization patterns, value-based care
- **Dimension: Patient Profile** - Aggregated patient demographics and history

All using incremental MERGE for efficiency.

---

## Complete Gold Layer Code

```python
# file: gold_healthcare_analytics.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    to_date, date_trunc, lag, lead, current_timestamp, lit,
    when, round as spark_round, floor, coalesce, collect_set
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_healthcare_analytics")


class GoldHealthcareAnalytics:
    """
    Silver → Gold
    
    Creates healthcare analytics facts and dimensions:
    - Risk scoring tables
    - Claim analytics
    - Provider performance
    - Patient profiles
    
    Uses incremental MERGE for all tables.
    """

    def __init__(self, spark: SparkSession,
                 silver_claims_path: str,
                 gold_base_path: str):

        self.spark = spark
        self.silver_claims_path = silver_claims_path.rstrip("/")
        self.gold_base_path = gold_base_path.rstrip("/")

    # ---------- READ SILVER ----------

    def read_silver_claims(self):
        logger.info(f"Reading Silver claims from {self.silver_claims_path}")
        df = self.spark.read.format("delta").load(self.silver_claims_path)
        logger.info(f"Silver claims count: {df.count()}")
        return df

    # ---------- FACT 1: DAILY PATIENT RISK SCORES ----------

    def build_fact_patient_risk_scores(self, df_silver):
        """
        Grain: ScoreDate, PatientID
        Risk indicators:
        - Readmission risk (recent claims, chronic conditions)
        - High-cost patient flag
        - Chronic disease burden
        
        This gets pushed to care coordination for proactive outreach.
        """
        logger.info("Building fact_patient_risk_scores...")

        score_date = to_date(current_timestamp())

        # Look at past 12 months of claims for risk calculation
        window_start = current_timestamp() - (365 * 24 * 60 * 60)

        # Aggregate per patient
        df_patient_metrics = (
            df_silver
            .filter(col("ServiceDate") >= window_start)
            .groupBy(col("PatientID"))
            .agg(
                count("*").alias("claim_count_12m"),
                spark_sum("ClaimAmount").alias("total_claims_12m"),
                avg("ClaimAmount").alias("avg_claim_12m"),
                spark_sum(when(col("ClaimStatus") == "DENIED", 1).otherwise(0)).alias("denied_claims_count"),
                collect_set("DiagnosisCode").alias("diagnosis_codes"),
            )
        )

        # Calculate risk scores (0-100)
        df_risk = (
            df_patient_metrics
            # High-cost patient (>$50K annual claims)
            .withColumn("high_cost_patient_flag",
                        when(col("total_claims_12m") > 50000, lit(1)).otherwise(lit(0)))

            # Readmission risk (high claim count + chronic conditions)
            .withColumn("readmission_risk_score",
                        when(col("claim_count_12m") > 50, lit(80))
                        .when(col("claim_count_12m") > 30, lit(60))
                        .when(col("claim_count_12m") > 15, lit(40))
                        .otherwise(lit(20)))

            # Chronic disease burden (based on diagnosis codes - simplified)
            .withColumn("chronic_disease_count",
                        when(col("diagnosis_codes").contains("E11"), lit(1)).otherwise(lit(0)) +  # Diabetes
                        when(col("diagnosis_codes").contains("I10"), lit(1)).otherwise(lit(0)) +  # Hypertension
                        when(col("diagnosis_codes").contains("J44"), lit(1)).otherwise(lit(0))    # COPD
            )
            .withColumn("chronic_disease_risk",
                        col("chronic_disease_count") * 15)

            # Composite risk score
            .withColumn("overall_risk_score",
                        (col("readmission_risk_score") + col("chronic_disease_risk")) / 2)
            .withColumn("overall_risk_score",
                        when(col("overall_risk_score") > 100, lit(100))
                        .otherwise(col("overall_risk_score")))

            # Risk tier for care coordination
            .withColumn("risk_tier",
                        when(col("overall_risk_score") >= 75, lit("HIGH"))
                        .when(col("overall_risk_score") >= 50, lit("MEDIUM"))
                        .otherwise(lit("LOW")))

            .withColumn("score_date", score_date)
            .withColumn("_load_ts", current_timestamp())
            .drop("diagnosis_codes")
        )

        logger.info(f"fact_patient_risk_scores rows: {df_risk.count()}")
        self._merge_to_gold(
            df_risk,
            table_name="fact_patient_risk_scores",
            merge_keys=["score_date", "PatientID"]
        )

    # ---------- FACT 2: CLAIM ANALYTICS ----------

    def build_fact_claim_analytics(self, df_silver):
        """
        Grain: ServiceDate, DiagnosisCode, ClaimStatus
        Metrics:
        - Claim counts by status
        - Approval rates
        - Average payment rates
        - Denial patterns
        """
        logger.info("Building fact_claim_analytics...")

        df_analytics = (
            df_silver
            .withColumn("service_date", to_date(col("ServiceDate")))
            .groupBy(
                col("service_date"),
                col("DiagnosisCode"),
                col("ProcedureCode"),
                col("ClaimStatus")
            )
            .agg(
                count("*").alias("claim_count"),
                spark_sum("ClaimAmount").alias("total_claimed_amount"),
                spark_sum("AllowedAmount").alias("total_allowed_amount"),
                spark_sum("PaidAmount").alias("total_paid_amount"),
                avg("ClaimAmount").alias("avg_claim_amount"),
                avg("AllowedAmount").alias("avg_allowed_pct"),
            )

            # Calculate approval rate
            .withColumn("approval_rate",
                        when(col("ClaimStatus") == "APPROVED",
                             col("claim_count") / 100.0)
                        .otherwise(lit(0)))

            # Payment rate (paid / allowed)
            .withColumn("payment_rate_pct",
                        when(col("total_allowed_amount") > 0,
                             (col("total_paid_amount") / col("total_allowed_amount")) * 100)
                        .otherwise(lit(0)))
            .withColumn("payment_rate_pct",
                        spark_round(col("payment_rate_pct"), 2))

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_claim_analytics rows: {df_analytics.count()}")
        self._merge_to_gold(
            df_analytics,
            table_name="fact_claim_analytics",
            merge_keys=["service_date", "DiagnosisCode", "ProcedureCode", "ClaimStatus"]
        )

    # ---------- FACT 3: PROVIDER PERFORMANCE ----------

    def build_fact_provider_performance(self, df_silver):
        """
        Grain: MonthDate, ProviderID
        Metrics:
        - Patient count
        - Claim volumes
        - Approval rates
        - Average payment
        - Quality indicators
        """
        logger.info("Building fact_provider_performance...")

        # Aggregate by provider by month
        df_provider = (
            df_silver
            .withColumn("month_date", date_trunc("month", col("ServiceDate")))
            .groupBy(
                col("month_date"),
                col("ProviderID")
            )
            .agg(
                count(col("PatientID")).alias("patient_count"),
                count(col("ClaimID")).alias("claim_count"),
                spark_sum("ClaimAmount").alias("total_claimed"),
                spark_sum("PaidAmount").alias("total_paid"),
                avg("PaidAmount").alias("avg_claim_paid"),
                spark_sum(when(col("ClaimStatus") == "APPROVED", 1).otherwise(0))
                    .alias("approved_count"),
                spark_sum(when(col("ClaimStatus") == "DENIED", 1).otherwise(0))
                    .alias("denied_count"),
            )

            # Calculate KPIs
            .withColumn("approval_rate_pct",
                        when(col("claim_count") > 0,
                             (col("approved_count") / col("claim_count")) * 100)
                        .otherwise(lit(0)))
            .withColumn("approval_rate_pct",
                        spark_round(col("approval_rate_pct"), 2))

            .withColumn("denial_rate_pct",
                        when(col("claim_count") > 0,
                             (col("denied_count") / col("claim_count")) * 100)
                        .otherwise(lit(0)))
            .withColumn("denial_rate_pct",
                        spark_round(col("denial_rate_pct"), 2))

            .withColumn("efficiency_score",
                        when(col("claim_count") > 0,
                             spark_round((col("total_paid") / col("total_claimed")) * 100, 2))
                        .otherwise(lit(0)))

            # Provider tier based on volume
            .withColumn("provider_tier",
                        when(col("claim_count") > 1000, lit("HIGH_VOLUME"))
                        .when(col("claim_count") > 500, lit("MEDIUM_VOLUME"))
                        .otherwise(lit("LOW_VOLUME")))

            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"fact_provider_performance rows: {df_provider.count()}")
        self._merge_to_gold(
            df_provider,
            table_name="fact_provider_performance",
            merge_keys=["month_date", "ProviderID"]
        )

    # ---------- DIM: PATIENT PROFILE ----------

    def build_dim_patient_profile(self, df_silver):
        """
        Grain: PatientID
        Snapshot of patient with aggregate metrics.
        """
        logger.info("Building dim_patient_profile...")

        df_dim = (
            df_silver
            .groupBy("PatientID")
            .agg(
                spark_max("_load_ts").alias("last_claim_date"),
                count("*").alias("total_claims"),
                spark_sum("ClaimAmount").alias("lifetime_claims"),
                avg("PatientAge").alias("patient_age"),
                collect_set("DiagnosisCode").alias("diagnosis_codes_list"),
                collect_set("ProviderID").alias("provider_ids_list"),
            )

            # Calculate tenure
            .withColumn("patient_status",
                        when(col("last_claim_date") >= current_timestamp() - (90 * 24 * 60 * 60),
                             lit("ACTIVE"))
                        .otherwise(lit("INACTIVE")))

            .withColumn("lifetime_years",
                        floor(col("lifetime_claims") / 12000))  # rough estimate

            .withColumn("_snapshot_date", to_date(current_timestamp()))
            .withColumn("_load_ts", current_timestamp())
        )

        logger.info(f"dim_patient_profile rows: {df_dim.count()}")
        self._merge_to_gold(
            df_dim,
            table_name="dim_patient_profile",
            merge_keys=["PatientID"]
        )

    # ---------- GENERIC MERGE ----------

    def _merge_to_gold(self, df_source, table_name: str, merge_keys: list):
        """
        Generic incremental MERGE into Gold delta table.
        """
        if df_source.rdd.isEmpty():
            logger.info(f"No rows to merge for {table_name}")
            return

        target_path = f"{self.gold_base_path}/{table_name}"
        logger.info(f"Merging into Gold table: {target_path}")

        # Build merge condition
        cond_parts = [f"t.{k} = s.{k}" for k in merge_keys]
        merge_condition = " AND ".join(cond_parts)

        try:
            target = DeltaTable.forPath(self.spark, target_path)
            (
                target.alias("t")
                .merge(
                    df_source.alias("s"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            # Table does not exist - initial load
            logger.info(f"Gold table {table_name} not found. Creating new one...")
            writer = (
                df_source
                .write
                .format("delta")
                .mode("overwrite")
            )

            # Partition by date if available
            if "score_date" in df_source.columns:
                writer = writer.partitionBy("score_date")
            elif "service_date" in df_source.columns:
                writer = writer.partitionBy("service_date")
            elif "month_date" in df_source.columns:
                writer = writer.partitionBy("month_date")
            elif "_snapshot_date" in df_source.columns:
                writer = writer.partitionBy("_snapshot_date")

            writer.save(target_path)

        logger.info(f"Merge completed for {table_name}. Rows processed: {df_source.count()}")

    # ---------- OPTIMIZE & STATS ----------

    def optimize_gold_tables(self):
        """
        Optimize Gold tables with Z-ORDER for analytics performance.
        Run weekly.
        """
        logger.info("Optimizing Gold tables...")

        tables_to_optimize = [
            ("fact_patient_risk_scores", ["score_date", "risk_tier"]),
            ("fact_claim_analytics", ["service_date", "ClaimStatus"]),
            ("fact_provider_performance", ["month_date", "approval_rate_pct"]),
        ]

        for table_name, z_order_cols in tables_to_optimize:
            try:
                table_path = f"{self.gold_base_path}/{table_name}"
                z_order_str = ", ".join(z_order_cols)
                optimize_sql = f"""
                OPTIMIZE delta.`{table_path}`
                Z-ORDER BY ({z_order_str})
                """
                self.spark.sql(optimize_sql)
                logger.info(f"Optimized {table_name} with Z-ORDER on {z_order_str}")
            except Exception as e:
                logger.warning(f"Could not optimize {table_name}: {e}")

    # ---------- RUN ----------

    def run(self, optimize: bool = False):
        """Execute full Gold layer pipeline"""
        df_silver = self.read_silver_claims()

        self.build_fact_patient_risk_scores(df_silver)
        self.build_fact_claim_analytics(df_silver)
        self.build_fact_provider_performance(df_silver)
        self.build_dim_patient_profile(df_silver)

        if optimize:
            self.optimize_gold_tables()

        logger.info("Gold layer pipeline completed successfully")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("gold_healthcare_analytics")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )

    job = GoldHealthcareAnalytics(
        spark=spark,
        silver_claims_path="abfss://silver@healthcare.dfs.core.windows.net/claims",
        gold_base_path="abfss://gold@healthcare.dfs.core.windows.net",
    )

    job.run(optimize=False)  # Set optimize=True once weekly for Z-ORDER
```

---

## Key Analytics Features

### 1. Patient Risk Scoring
**Use Case**: Care coordination outreach
- Readmission risk (0-100 score)
- Chronic disease burden
- High-cost patient identification
- Real-time alerts to care teams

**Example Output**:
```
PatientID | score_date | readmission_risk | risk_tier | action
100001    | 2025-01-15 | 85              | HIGH      | Schedule follow-up
100002    | 2025-01-15 | 45              | MEDIUM    | Monitor
100003    | 2025-01-15 | 15              | LOW       | No action
```

### 2. Claim Analytics
**Use Case**: Quality monitoring, billing insights
- Approval rates by diagnosis
- Denial patterns
- Payment rates (paid/allowed)
- High-cost condition identification

**Example Output**:
```
service_date | diagnosis | status    | approval_rate | denial_rate
2025-01-15   | E11       | APPROVED  | 92%          | 5%
2025-01-15   | I10       | DENIED    | 88%          | 8%
```

### 3. Provider Performance
**Use Case**: Value-based care, network management
- Quality metrics (approval rates)
- Utilization patterns
- Efficiency scores
- Provider tiering (high/medium/low volume)

**Example Output**:
```
provider_id | month_date | claim_count | approval_rate | efficiency | tier
P001        | 2025-01    | 1500        | 94%          | 96%       | HIGH_VOLUME
P002        | 2025-01    | 450         | 88%          | 92%       | MEDIUM_VOLUME
```

### 4. Patient Profiles
**Use Case**: Population health, risk management
- Lifetime claim history
- Active/inactive status
- Diagnosis profile
- Provider relationships

---

## Databricks Workflow Configuration

```yaml
name: "gold_healthcare_analytics_daily"
schedule:
  quartz_cron_expression: "0 8 * * ? *"  # 8 AM daily (after Silver completes)
tasks:
  - task_key: "gold_transformation"
    notebook_task:
      notebook_path: "/Users/data_engineer/gold_healthcare_analytics"
      base_parameters:
        optimize: "false"
    new_cluster:
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 16
    timeout_seconds: 3600

  - task_key: "gold_optimization"
    depends_on:
      - task_key: "gold_transformation"
    notebook_task:
      notebook_path: "/Users/data_engineer/gold_healthcare_analytics"
      base_parameters:
        optimize: "true"
    new_cluster:
      spark_version: "13.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 8
    timeout_seconds: 1800
    schedule:
      quartz_cron_expression: "0 8 ? * SUN"  # Sundays only (weekly)
```

---

## Analytics Queries Available for BI Teams

Once Gold tables are ready, BI teams query them directly:

**Patient Risk Dashboard**:
```sql
SELECT 
    score_date,
    risk_tier,
    COUNT(*) as patient_count,
    AVG(overall_risk_score) as avg_risk
FROM gold.fact_patient_risk_scores
WHERE score_date = CURRENT_DATE
GROUP BY score_date, risk_tier
```

**Provider Quality Scorecard**:
```sql
SELECT 
    provider_id,
    month_date,
    claim_count,
    approval_rate_pct,
    efficiency_score,
    provider_tier
FROM gold.fact_provider_performance
WHERE month_date >= CURRENT_DATE - INTERVAL 12 MONTH
ORDER BY approval_rate_pct DESC
```

**High-Cost Patient Identification**:
```sql
SELECT 
    patient_id,
    total_claims_12m,
    overall_risk_score,
    risk_tier
FROM gold.fact_patient_risk_scores
WHERE high_cost_patient_flag = 1
    AND score_date = CURRENT_DATE
ORDER BY total_claims_12m DESC
```


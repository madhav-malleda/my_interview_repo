# PROJECT 2 - MULTI-SOURCE CHARGE CAPTURE DATA LAKE
## Interview Explanation Guide - Industry-Standard Medallion Architecture

---

## 30-SECOND ELEVATOR PITCH

**"I co-architected and deployed a scalable multi-source healthcare charge data lake using Medallion Architecture. Ingested patient and physician charge data from multiple sources (SQL Server database, JSON APIs, CSV exports, text files) via Azure Data Factory orchestration. Developed PySpark transformations across Bronze, Silver, and Gold layers on Databricks, with final curated analytics layer on Delta Lake in ADLS Gen2. This architecture identified $2M in unbilled charges, unified 4 disparate data sources into single source of truth, and improved overall data processing speed by 30%, reducing monthly charge processing from 4 hours to 2.8 hours."**

---

## 2-MINUTE BUSINESS EXPLANATION

### Problem Statement
"Our healthcare organization had a critical challenge: charge capture was fragmented across 4 different systems with no unified view:

1. **SQL Server Database** - Legacy billing system with patient demographics and charge history
2. **JSON APIs** - Real-time clinical event feeds from EHR system (procedures, diagnoses, orders)
3. **CSV Exports** - Manual exports from physician fee schedules and CPT code mappings
4. **Text Files** - Unstructured lab results and clinical notes with embedded charge indicators

**The Business Impact**:
- Charges were being missed because data was siloed
- $2M in unbilled charges discovered (went undetected for months)
- Billing analysts spent 4+ hours monthly doing manual reconciliation across systems
- No real-time visibility into charge completeness
- Difficult to identify which charges were billable vs non-billable
- Unable to audit charge capture accuracy

The organization needed a unified data lake that could:
- Ingest heterogeneous data sources (DB, APIs, files with different formats)
- Apply consistent data quality rules
- Create single source of truth for charge data
- Enable analytics on charge patterns and gaps"

### Solution Overview
"We designed and deployed a cloud-native Medallion Architecture data lake:

**1. Data Ingestion (Azure Data Factory)**
- Orchestrated extraction from 4 diverse sources:
  - SQL Server: Direct query with incremental watermarks (like Project 1)
  - JSON APIs: REST API connections pulling real-time clinical events
  - CSV Exports: Scheduled file uploads from fee schedules
  - Text Files: Unstructured file monitoring (using AutoLoader for automatic detection)
- ADF pipelines scheduled to run at different intervals based on source freshness requirements

**2. Bronze Layer - Raw Data Landing**
- Preserved all data exactly as received (no transformations)
- Stored in ADLS Gen2 partitioned by source system and load date
- Maintained separate folders for each source:
  - `/bronze/charges_sql/2025/01/15/`
  - `/bronze/charges_json/2025/01/15/`
  - `/bronze/charges_csv/2025/01/15/`
  - `/bronze/charges_text/2025/01/15/`
- Parquet format for structured data, JSON for nested APIs
- AutoLoader monitored text file folder for automatic ingestion

**3. Silver Layer - Unified & Cleaned**
- Applied source-specific transformations:
  - SQL charges: Type conversion, null handling
  - JSON events: Flattened nested structures, extracted billable indicators
  - CSV fee schedules: Parsed CPT codes, standardized pricing
  - Text files: NLP extraction of charge-related keywords, structured parsing
- Data quality validation:
  - Required field checks (charge amount, procedure code, patient)
  - Business rule validation (charges > $0, valid CPT codes)
  - Completeness checks (no orphaned charges without patient link)
- Unified schema across all sources
- Incremental MERGE into Delta tables by source
- Created unified patient-charge view (deduplication if same charge in multiple sources)

**4. Gold Layer - Analytics Ready**
- **Fact: Daily Charge Summary** - Grain: charge_date, patient_id, provider_id, cpt_code
  - Charge amounts, status (billed/unbilled/denied)
  - Source system lineage (which system reported this charge)
- **Fact: Charge Gap Analysis** - Grain: charge_date, patient_id
  - Expected charges (from orders/procedures) vs actual charges captured
  - Missing charges identification
  - Gap root cause (missed entry, system lag, billing hold)
- **Fact: Provider Charge Performance** - Grain: month, provider_id, department
  - Charges captured per provider
  - Billing accuracy rate
  - Unbilled charge trends
- **Dim: Charge Master** - Unified CPT/procedure code reference
  - Billable status, pricing, revenue cycle mapping
- **Fact: Unbilled Charges** - High-value fact table
  - All charges with billing status = UNBILLED
  - Age of unbilled charge (how long pending)
  - Revenue impact (amount at risk)
  - Recommended action (bill, write-off, correction)

**5. Analytics & BI Integration**
- BI teams query Gold layer via Databricks SQL endpoints
- Real-time dashboards showing charge capture completeness
- Alerts for newly identified unbilled charges
- Provider performance scorecards
- Charge gap investigation tools

---

### Business Impact
"The multi-source data lake delivered significant business value:

- **Revenue Recovery**: Identified $2M in unbilled charges (ROI in first quarter)
- **Operational Efficiency**: Reduced monthly billing reconciliation from 4 hours to 2.8 hours (30% improvement)
- **Data Unification**: Single source of truth replacing 4 fragmented systems
- **Real-time Visibility**: Billing team now sees charge capture issues same day vs weeks later
- **Charge Accuracy**: 99% data quality - reduced billing denials from charge errors
- **Analytics Capability**: Now can analyze charge patterns, identify systemic gaps, forecast revenue
- **Scalability**: Architecture supports 10x more sources without redesign"

---

## 5-MINUTE TECHNICAL EXPLANATION

### Multi-Source Architecture

**"Let me walk you through how we unified 4 disparate data sources into one data lake.**

### **1. Data Sources & Extraction Challenges**

**Source 1: SQL Server (Legacy Billing System)**
- Database of historical charges, patient demographics, billing status
- Challenge: Large table, need incremental extraction only
- Solution: ADF watermark pattern (LastModifiedDate) like Project 1
- Frequency: Daily at 2 AM
- Volume: 50-100K records daily

**Source 2: JSON APIs (Real-time EHR Events)**
- REST API endpoints returning clinical events (procedures ordered, diagnoses entered, lab results)
- Challenge: Real-time data, rate-limited API, nested JSON structure
- Solution: ADF configured for:
  - Scheduled pulls every 2 hours (near real-time)
  - Pagination handling (API returns 1000 records per page)
  - Error retry with exponential backoff
  - Stored in ADLS as JSON files
- Volume: 5-10K events daily
- Example payload:
```json
{
  "event_id": "E123456",
  "patient_id": "P001",
  "event_type": "PROCEDURE_ORDERED",
  "procedure_code": "99213",
  "timestamp": "2025-01-15T10:30:00Z",
  "billable": true,
  "estimated_charge": 150.00
}
```

**Source 3: CSV Fee Schedules (Physician Pricing)**
- Uploaded weekly by billing department
- Contains CPT code → procedure name → standard price mappings
- Challenge: Manual uploads, filename variations, occasional data quality issues
- Solution: ADF monitors SFTP folder, detects new files, stores in ADLS
- Volume: 5-10K CPT codes, static reference data
- Format:
```
CPT_Code,Procedure_Name,StandardPrice,IsActive
99213,Office visit established patient,150.00,true
99214,Office visit established patient 25min,200.00,true
```

**Source 4: Text Files (Unstructured Clinical Notes)**
- Lab results, physician notes exported from legacy system
- Challenge: Completely unstructured, embedded charge indicators
- Solution: AutoLoader monitors text file folder for new files
- Volume: 500-1000 text files daily (~1-2 MB each)
- Example content: "Patient underwent CT Scan (CPT 71020) on 01/15, estimated cost $500"

### **2. Azure Data Factory - Multi-Source Orchestration**

```
ADF Pipeline Structure:

Source 1: SQL Server
   ├─ Watermark lookup activity
   ├─ Copy activity (16 parallel reads)
   └─ Watermark update stored procedure
        ↓
Source 2: JSON APIs
   ├─ For each API endpoint (pagination loop)
   ├─ HTTP activity (REST call)
   └─ Store response as JSON file
        ↓
Source 3: CSV Uploads
   ├─ List files in folder
   ├─ For each file
   │   ├─ Copy to ADLS
   │   └─ Rename with load timestamp
        ↓
Source 4: Text Files
   ├─ Upload to monitoring folder
   └─ AutoLoader detects automatically
        ↓
All → Bronze Layer (partitioned by source + date)
```

**Scheduling**:
- SQL Server: 2 AM daily (incremental)
- JSON APIs: Every 2 hours (near real-time)
- CSV uploads: Weekly Monday 8 AM
- Text files: Continuous (AutoLoader)

### **3. Bronze Layer - Source-Specific Raw Storage**

```
ADLS Gen2 Structure:

/bronze/charges_sql/2025/01/15/
  ├─ charges_2025_01_15_001.parquet
  ├─ charges_2025_01_15_002.parquet
  └─ _metadata (load timestamp, row count)

/bronze/charges_json/2025/01/15/
  ├─ events_2025_01_15_0000.json
  ├─ events_2025_01_15_0100.json  (every 2 hours)
  └─ _metadata

/bronze/charges_csv/2025/01/15/
  ├─ fee_schedule_20250115.csv
  └─ _metadata (weekly updates)

/bronze/charges_text/2025/01/15/
  ├─ lab_notes_20250115_001.txt
  ├─ lab_notes_20250115_002.txt
  └─ _metadata (AutoLoader tracking)
```

**Key Characteristics**:
- Immutable (append-only)
- Encrypted at rest
- Partitioned by source + date for efficient queries
- 2-year retention
- No transformations applied

### **4. Databricks Silver Layer - Source-Specific Transformations**

Different transformation logic for each source:

**SQL Source Transform**:
- Type conversions (charges to decimal, dates normalized)
- Null handling (amount defaults to 0.00)
- Deduplication (same charge not appearing twice)
- Incremental MERGE on charge_id

**JSON Source Transform**:
- Flatten nested JSON structure:
```python
# Nested: {"procedure": {"code": "99213", "name": "Office Visit"}}
# Flattened: procedure_code = "99213", procedure_name = "Office Visit"
```
- Extract billable indicator
- Parse timestamp to charge_date
- Incremental MERGE on event_id

**CSV Source Transform**:
- Parse CSV (handle quoted fields)
- Validate CPT codes (numeric, 5 digits)
- Standardize pricing (ensure decimal format)
- Load as reference table (full refresh, not incremental)

**Text Source Transform**:
- Extract structured charge info using regex/NLP:
  - Pattern: `CPT\s+(\d{5})` → extract procedure code
  - Pattern: `cost\s+\$(\d+\.?\d*)` → extract amount
- Split text into sentences for NLP processing
- Extract patient identifiers (medical record number, name)
- Incremental append (new files only)

### **5. Silver Layer - Unified Charge Schema**

After source-specific transforms, create unified schema:

```python
# All sources → unified table
charge_id (unique identifier)
source_system (SQL / JSON_EHR / CSV_FEES / TEXT_NOTES)
charge_date
patient_id
provider_id
procedure_code (CPT code)
charge_amount
estimated_charge
billable_status (BILLABLE / NON_BILLABLE / PENDING_REVIEW)
data_quality_score (0-100)
_load_ts
```

**Data Quality Checks**:
- Required fields: patient_id, procedure_code, charge_amount
- Business rules:
  - Charge amount > $0 and < $1M
  - Procedure code is valid CPT (exists in fee schedule)
  - Patient exists in patient master
  - Provider assigned to facility
- Source-specific rules:
  - JSON: billable_status must be populated
  - SQL: charge_date <= current_date
  - CSV: Only active procedures (IsActive = true)
  - Text: Extracted confidence score > 0.8

**Deduplication**:
- If same charge appears in multiple sources (e.g., SQL + JSON both report same procedure)
- Keep charge from source with highest quality score
- Log that duplicate was found (revenue integrity check)

**Incremental MERGE**:
- Each source has its own Silver table
- MERGE by source_charge_id
- Then union all sources into unified Silver

### **6. Databricks Gold Layer - Analytics-Ready Facts**

Multiple fact tables for different analytics needs:

**Fact 1: Daily Charge Summary**
```
Grain: charge_date, patient_id, provider_id, procedure_code
Metrics:
  - charge_count
  - total_charge_amount
  - avg_charge_amount
  - billable_charge_amount
  - unbilled_charge_amount
  - denied_charge_amount
```

**Fact 2: Charge Gap Analysis** (the $2M opportunity)
```
Grain: charge_date, patient_id
Metrics:
  - expected_charges (from orders/procedures in EHR)
  - actual_charges (captured in billing)
  - missing_charges (expected - actual)
  - gap_amount (revenue at risk)
  - gap_percentage
  - gap_reason (billing hold, system lag, missed entry)
```

**Fact 3: Provider Performance**
```
Grain: month_date, provider_id, department
Metrics:
  - total_charges
  - billable_charges
  - unbilled_charges
  - billing_accuracy_rate
  - charge_capture_completeness (actual/expected %)
  - avg_days_to_bill (age of charges)
```

**Dimension: Charge Master** (reference table)
```
Grain: procedure_code (CPT)
Attributes:
  - procedure_name
  - standard_price
  - is_billable
  - revenue_cycle_status
  - last_updated_date
```

**Fact 4: Unbilled Charges** (critical for revenue recovery)
```
Grain: charge_id
Metrics:
  - patient_id, provider_id, procedure_code
  - charge_amount
  - days_unbilled
  - revenue_impact
  - billing_hold_reason
  - recommended_action
  - next_review_date
```

All loaded incrementally via MERGE.

### **7. Performance Metrics**

Before and after medallion architecture:

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data processing time | 4 hours/month | 2.8 hours | 30% faster |
| Charge capture | 4 disparate systems | 1 unified source | Single source of truth |
| Unbilled charge identification | Manual, reactive | Automated, proactive | $2M recovered |
| Data freshness | Weekly | Real-time (2-hour freshness) | Hours vs days |
| Billing accuracy | 95% | 99% | 4% improvement |
| Query response | 5+ minutes | Seconds (Z-ORDER) | 300x faster |

### **8. Key Technical Decisions**

**Why Medallion Architecture (Bronze/Silver/Gold)?**
- Bronze: Preserves all raw data as-is (audit trail)
- Silver: Centralized data quality and standardization across all sources
- Gold: Business-ready, optimized for analytics
- Allows reprocessing if logic changes

**Why AutoLoader for Text Files?**
- Text files arrive unpredictably (not scheduled)
- AutoLoader monitors folder and detects new files automatically
- Handles schema variation (different text formats)
- Checkpoints prevent reprocessing same file twice
- Perfect for unstructured data ingestion

**Why Incremental MERGE (not full refresh)?**
- SQL + JSON sources are large (full refresh would be slow)
- Incremental is idempotent (safe to retry)
- Cost-efficient (only process changed data)
- Maintains historical lineage

**Why 4 separate Bronze folders (not 1)?**
- Different source formats require different parsers
- Partitioning by source enables targeted troubleshooting
- Maintains source lineage for compliance
- Easy to reload single source if needed

**Why Delta Lake?**
- ACID transactions (charge accuracy guaranteed)
- Time travel (can recover old data if errors found)
- Schema evolution (new charges fields can be added)
- Compression (reduces storage costs)
- Unified analytics (single format for all data)

---

## COMMON INTERVIEW QUESTIONS & ANSWERS

### Q1: "How did you handle 4 completely different data sources?"

**Answer**: "Architecture was source-agnostic at Bronze, source-aware at Silver, unified at Gold.

**Bronze Level**:
- Stored each source in its own folder with its native format
- SQL → Parquet files
- JSON → JSON files  
- CSV → CSV files
- Text → text files with AutoLoader

**Silver Level**:
- Applied source-specific transformation logic
- SQL: watermark incremental extraction, type conversion
- JSON: flatten nested structure, extract billable flag
- CSV: parse CPT codes, validate pricing
- Text: NLP extraction of charge indicators
- Each source had own validation rules

**Gold Level**:
- Unified all sources into common schema
- Deduplication if same charge appeared in multiple sources
- Single source of truth for analytics
- BI teams never worry about source differences

Key insight: Don't try to force different sources into same schema immediately. Let them be different in Bronze/Silver, then unify at Gold."

### Q2: "How did you identify the $2M in unbilled charges?"

**Answer**: "We built the Charge Gap Analysis fact table that compared expected vs actual charges:

**Expected Charges** (from EHR events):
- Query JSON API events: all procedures ordered and diagnoses entered
- Group by patient, sum up what SHOULD be charged
- Example: Patient had 50 procedures ordered but only 48 charges captured

**Actual Charges** (from billing system):
- Query SQL and JSON charge data
- Sum up what WAS charged
- Example: Only 48 charges were actually billed

**The Gap**:
- Expected (50) - Actual (48) = 2 missing charges
- Across all patients over 12 months: $2M in unbilled charges

**Root Causes Found**:
- Billing holds (charge being reviewed)
- System lag (charge ordered but not yet captured)
- Missed entry (order placed but charge never entered)
- Non-billable procedures (marked but amount still calculated)

Once identified, billing team could take action:
- Follow up on held charges
- Fix process gaps
- Recover revenue"

### Q3: "Why use AutoLoader for text files instead of just ADF?"

**Answer**: "Great question. Text files were our edge case:

**Why AutoLoader worked better**:
1. **Unpredictable arrival**: Files landed throughout the day, not scheduled
2. **File monitoring**: AutoLoader automatically detects new files (ADF would need polling)
3. **Schema variation**: Different text formats → AutoLoader's checkpoint tracking avoids reprocessing
4. **NLP parsing**: Databricks notebooks run NLP logic on streaming text more naturally than ADF

**If we used ADF**:
- Would need polling loop to check folder every N minutes
- Would reprocess old files (inefficient)
- Would be harder to extract structured data from unstructured text

**AutoLoader was perfect** because:
- Once configured, it works autonomously
- Checkpoints ensure files processed exactly once
- Integrates with Databricks jobs naturally
- Scales to handle file volume spikes

Trade-off: AutoLoader is streaming (continuous), while other sources are batch. But Delta Lake handles mixed batch/streaming well."

### Q4: "How do you handle when same charge appears in multiple sources?"

**Answer**: "Deduplication was critical for $2M recovery accuracy.

**Scenario**: 
- Procedure entered in EHR (JSON) → charge captured: $500
- Same procedure also in billing SQL DB → charge recorded: $500
- Without deduplication: double-count = $1000 (revenue overstated)

**Our Approach**:

Step 1: Identify potential duplicates
- Match on: patient_id + procedure_code + charge_date (within 1 day)
- Find all charges matching this combination

Step 2: Quality scoring
- SQL source score: 0.9 (high confidence, official billing system)
- JSON source score: 0.85 (real-time but might have timing lag)
- CSV source score: 0.7 (reference data, less reliable)
- Text source score: 0.6 (extracted via NLP, lowest confidence)

Step 3: Keep highest score, log others
```python
# In Silver layer
keep_charge = charges.groupBy("patient_id", "procedure_code", "charge_date") \
    .agg(
        max_by(struct("*"), "data_quality_score").alias("best_charge"),
        collect_list("source_system").alias("other_sources")
    )
# Log that duplicates were found for auditing
```

Step 4: Document lineage
- Gold table includes source_system field
- Billing team knows which system was authoritative
- Can audit any $2M charge back to source

**Result**: No double-counting, complete audit trail, revenue accurate"

### Q5: "How do you ensure 99% data quality with 4 sources?"

**Answer**: "Multi-layer validation + quarantine approach:

**Layer 1: Source-level validation** (before Bronze)
- ADF validates rows before writing to ADLS
- Check for nulls in required fields
- Validate data types
- Log failures and count

**Layer 2: Silver layer validation**:
- Charge amount > $0 and < $1M
- CPT code exists in fee schedule
- Patient ID found in patient master
- Provider in provider master
- Charge date not in future
- Billable flag is boolean (true/false)

**Layer 3: Cross-source validation**:
- If charge appears in 2+ sources, validate consistency
- Check for orphaned charges (no patient/provider)

**Quarantine Process**:
- Invalid records go to quarantine table (separate Delta)
- Not removed (compliance requires keeping failed records)
- Billing team investigates and either:
  - Fixes source data and reprocesses
  - Marks as deliberately excluded
  - Documents reason for failure

**Monitoring**:
- Daily dashboard: DQ pass rate by source
- Alert if pass rate drops below 99%
- Track which validations fail most
- Use insights to improve upstream systems

**Result**: 99% quality = 1% of charges investigated and fixed"

### Q6: "How do you handle late-arriving data from different sources?"

**Answer**: "Late arrivals are common in healthcare - orders entered weeks after service. We handle per source:

**SQL Source (watermark-based)**:
- Look back 7 days by default (not just since last load)
- Catches charges updated/corrected recently
- Query: `WHERE LastModifiedDate > @{watermark} - 7 days`

**JSON Source (real-time API)**:
- API returns recent events even if created earlier
- Handles naturally because polling frequently (every 2 hours)
- Eventual consistency: charge appears when system records it

**CSV Source (weekly reference)**:
- Static reference data (CPT codes), not really late-arriving
- Full refresh each week (not incremental)

**Text Source (AutoLoader)**:
- Monitors folder continuously
- Files processed as soon as they appear
- Checkpoint tracking: if resubmitted file, not reprocessed

**Gold Layer Handling**:
- Fact tables are SCD Type 1 (upsert existing, insert new)
- If charge arrives late, updates existing charge record
- Gap analysis recalculates (will show charge when it arrives)

**Example**:
- Day 1: Charge expected but not yet captured → gap identified
- Day 8: Charge finally captured late
- MERGE updates Gold table
- Gap closes automatically

No manual correction needed - system handles it."

### Q7: "What was your biggest optimization?"

**Answer**: "Two major optimizations:

**#1: Incremental MERGE architecture (30% processing speed improvement)**
- Before: Rebuild Gold tables daily from scratch (expensive)
- After: Only process new/changed charges incrementally
- Massive time savings on large historical datasets

Example:
- Historical charges: 100M records
- Daily new charges: 10K records
- Before: Process 100M daily = 60 minutes
- After: MERGE only 10K incrementally = 2 minutes
- Scaling: adding more historical data doesn't slow down processing

**#2: Source-specific optimizations**:
- SQL: Parallel reads (16 partitions), only pull changed data
- JSON: Pagination handling, batch requests instead of one-by-one
- CSV: Single table load (not processing line by line)
- Text: Batch file processing via AutoLoader (not individual files)

**Result**: Total monthly charge processing: 4 hours → 2.8 hours (30% faster)"

### Q8: "How would you handle 10x more data sources?"

**Answer**: "Architecture scales beautifully:

**At Scale**:
- Bronze: Add new folder per source (modular)
- Silver: Add new transformation job per source (independent)
- Gold: Unified schema already handles multiple sources
- Doesn't require architectural redesign

**Example: Adding 36 more sources** (now 40 total):
- Add 36 new ADF pipelines
- Add 36 new Silver transformation jobs
- Gold layer stays the same (just ingests more data)
- Cost scales linearly, not exponentially

**Performance Considerations**:
- Current: 4 sources, 4 Silver jobs
- At 40 sources: 40 Silver jobs (would parallelize easily)
- Databricks auto-scales (add more workers for parallel processing)
- Incremental MERGE keeps per-job time consistent

**Key design decision**: Making sources independent (not combined) was crucial for scalability"

### Q9: "How do you prevent data loss if a source fails?"

**Answer**: "Multiple resilience layers:

**ADF Level**:
- Retry logic (3 attempts with exponential backoff)
- Staging area for failed transfers (cleaned up on retry)
- Watermark not updated if load fails (retry gets same data)

**Bronze Level**:
- Immutable storage (data once written cannot be deleted)
- If transformation logic wrong downstream, can reprocess from Bronze
- Historical data preserved for compliance

**Silver Level**:
- MERGE is idempotent (safe to retry)
- Checkpoints track progress
- If job fails mid-execution, resume from checkpoint on retry
- Quarantine table doesn't lose data (just flags as invalid)

**Monitoring**:
- Alerts if any source hasn't loaded in 48 hours
- Email if data quality drops below threshold
- Dashboard shows load status per source

**Last Resort**:
- Time travel in Delta Lake (can go back to prior day's state)
- Full audit trail in Silver audit table
- Recovery procedure documented

Example: If JSON API fails, yesterday's data still in Delta. Retry tomorrow, catch up."

### Q10: "What would you do differently now?"

**Answer**: "Lessons learned:

1. **Schema Registry Earlier**: Should have built centralized schema registry upfront (defines charge schema once, enforced everywhere). Would catch schema drift issues faster.

2. **Data Governance Framework**: Initially didn't have clear data ownership (which team owns which source). Now would assign data owners for each source.

3. **Cost Attribution**: Didn't track cost per source initially. Now would monitor compute cost of each source (some more expensive to transform than others).

4. **Test Data Earlier**: Should have tested all 4 sources simultaneously earlier. Found integration issues late.

5. **Stakeholder Communication**: Initially, billing team didn't understand why $2M wasn't immediately recoverable. Now would have communication plan explaining gap analysis findings.

But overall: Architecture is solid. Multi-source medallion pattern works well. Would build similar way again."

---

## TALKING POINTS BY COMPANY TYPE

### For Data/Analytics Companies
- Emphasize: Multi-source integration, AutoLoader streaming, medallion pattern
- Metrics: 4 sources unified, 30% performance improvement
- Technical depth: MERGE operations, Delta Lake time travel, schema handling

### For Healthcare IT Companies
- Emphasize: Revenue capture, charge accuracy, real-time billing analytics
- Metrics: $2M unbilled charges identified, 99% data quality, 2.8 hour processing
- Business: Billing team efficiency, BI integration, provider scorecards

### For Large Enterprises
- Emphasize: Scalability, source independence, fault tolerance
- Metrics: Can handle 10x more sources, 99%+ uptime, seamless BI integration
- Architecture: Medallion, incremental MERGE, cost efficiency

### For Financial/Banking
- Emphasize: Data quality, audit trails, compliance, revenue accuracy
- Metrics: 99% data quality, complete lineage tracking, zero data loss
- Controls: Deduplication validation, reconciliation, error quarantine

---

## KEY METRICS TO MEMORIZE

**Performance**:
- Processing time: 4 hours → 2.8 hours (**30% faster**)
- Query response: minutes → seconds (Z-ORDER optimization)
- Data freshness: Weekly → **Real-time** (2-hour API polling)

**Business Impact**:
- Unbilled charges identified: **$2M recovered**
- Data sources unified: **4 → 1** (single source of truth)
- Billing accuracy: 95% → **99%**
- Processing efficiency: 4 hours → **2.8 hours/month**

**Technical**:
- Sources: SQL Server + JSON APIs + CSV + Text files
- Bronze folders: 4 (one per source)
- Silver jobs: 4 (one per source)
- Gold facts: 4 (summary, gap analysis, provider performance, unbilled)

**Data Quality**:
- Pass rate: **99%+**
- Deduplication accuracy: **100%** (no double-counting)
- Data lineage: Complete (traces back to source)

---

## ARCHITECTURE DIAGRAM

```
┌─────────────────────────────────────────────────────────────┐
│ DATA SOURCES (4 Disparate Systems)                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │  SQL Server      │  │  JSON EHR APIs   │                │
│  │  (Billing DB)    │  │  (Real-time)     │                │
│  │  50-100K/day     │  │  5-10K/day       │                │
│  └────────┬─────────┘  └────────┬─────────┘                │
│           │                     │                          │
│  ┌────────▼─────────┐  ┌───────▼──────────┐               │
│  │  CSV Uploads     │  │  Text Files      │               │
│  │  (Fee Schedule)  │  │  (Clinical Notes)│               │
│  │  5-10K weekly    │  │  500-1K daily    │               │
│  └────────┬─────────┘  └───────┬──────────┘               │
│           │                     │                          │
└───────────┼─────────────────────┼──────────────────────────┘
            │                     │
            ▼                     ▼
    ┌──────────────────────────────────────┐
    │ Azure Data Factory Orchestration     │
    │ - ADF watermark (SQL)                │
    │ - REST API calls (JSON)              │
    │ - File monitoring (CSV)              │
    │ - AutoLoader setup (Text)            │
    └──────────────┬───────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────┐
    │ BRONZE LAYER (Raw Data Landing)      │
    │ - /bronze/charges_sql/               │
    │ - /bronze/charges_json/              │
    │ - /bronze/charges_csv/               │
    │ - /bronze/charges_text/              │
    │ (Parquet/JSON, partitioned by date)  │
    └──────────────┬───────────────────────┘
                   │
        ┌──────────┼──────────┐
        │          │          │
        ▼          ▼          ▼
    ┌─────────┬────────┬──────────┐
    │ Silver  │ Silver │ Silver   │
    │ SQL Job │ JSON   │ CSV Job  │
    │         │ Job    │          │
    └─────────┴────────┴──────┬───┘
              │               │
              │    ┌──────────┘
              │    │
              ▼    ▼
    ┌──────────────────────────────────────┐
    │ SILVER LAYER (Unified Schema)        │
    │ - Source-specific transforms         │
    │ - Data quality validation            │
    │ - Deduplication                      │
    │ - MERGE incremental                  │
    │ - Unified charge table               │
    │ - Quarantine (invalid records)       │
    └──────────────┬───────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────┐
    │ GOLD LAYER (Analytics Ready)         │
    │ - Fact: Daily Charge Summary         │
    │ - Fact: Charge Gap Analysis ($2M)    │
    │ - Fact: Provider Performance         │
    │ - Dim: Charge Master (CPT codes)     │
    │ - Fact: Unbilled Charges (alerts)    │
    └──────────────┬───────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────┐
    │ ANALYTICS & BI LAYER                 │
    │ - Power BI / Tableau dashboards      │
    │ - Real-time charge completeness      │
    │ - Provider scorecards                │
    │ - Revenue recovery tracking          │
    │ - Gap investigation tools            │
    └──────────────────────────────────────┘
```

---

## NEXT STEPS FOR INTERVIEWS

1. **Memorize** 30-second pitch
2. **Review** 2-minute business explanation
3. **Understand** 5-minute technical deep-dive
4. **Practice** Q&A section out loud
5. **Customize** for company type (data vs healthcare vs enterprise)
6. **Cite metrics**: $2M recovery, 30% faster, 99% quality
7. **Show architecture diagram** if whiteboarding

**You've got this!** 🏥💪


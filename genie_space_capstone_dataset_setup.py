# Databricks notebook source
# MAGIC %md
# MAGIC # NorthWave Telecom — Genie Space Capstone Dataset Setup
# MAGIC
# MAGIC This notebook creates 5 demo tables:
# MAGIC - `tc_plans`
# MAGIC - `tc_customers`
# MAGIC - `tc_usage`
# MAGIC - `tc_tickets`
# MAGIC - `tc_payments`
# MAGIC
# MAGIC **How to use**
# MAGIC 1. Import this `.py` file into Databricks
# MAGIC 2. Update `CATALOG` and `SCHEMA`
# MAGIC 3. Run all cells

# COMMAND ----------

# ── USER CONFIG — set via widgets at the top of the notebook ──────
dbutils.widgets.text("catalog", "your_catalog", "Catalog")
dbutils.widgets.text("schema", "genie_capstone", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
# ─────────────────────────────────────────────────────────────────

# COMMAND ----------

from datetime import date, timedelta
import random
from pyspark.sql import Row

random.seed(42)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
print(f"Using: {CATALOG}.{SCHEMA}")

def _d(start: date, end: date) -> date:
    """Return a random date between start and end (inclusive)."""
    return start + timedelta(days=random.randint(0, (end - start).days))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1 — `tc_plans`
# MAGIC
# MAGIC Ambiguities:
# MAGIC - `plan_tier`: `1=Basic`, `2=Standard`, `3=Premium`, `4=Enterprise`
# MAGIC - `contract_type`: `M=Monthly`, `A=Annual`, `B=Biennial`
# MAGIC - `data_gb = 0` means unlimited
# MAGIC - `voice_min = 0` means unlimited

# COMMAND ----------

plans_rows = [
    ("PLN001", "Starter 4G",      1,   5,  200,  24.99, "M", True ),
    ("PLN002", "Basic 5G",        1,  10,  500,  34.99, "M", True ),
    ("PLN003", "Standard 5G",     2,  25,    0,  49.99, "M", True ),
    ("PLN004", "Standard 5G+",    2,  50,    0,  59.99, "A", True ),
    ("PLN005", "Premium 5G",      3, 100,    0,  79.99, "M", True ),
    ("PLN006", "Premium 5G Max",  3,   0,    0,  99.99, "A", True ),
    ("PLN007", "Enterprise 5G",   4,   0,    0, 149.99, "B", True ),
    ("PLN008", "Legacy 3G",       1,   2,  100,  14.99, "M", False),
]

plans_cols = [
    "plan_id", "plan_name", "plan_tier", "data_gb", "voice_min",
    "monthly_fee", "contract_type", "is_active"
]

df_plans = spark.createDataFrame(
    [Row(**dict(zip(plans_cols, r))) for r in plans_rows]
)

df_plans.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.tc_plans")

print(f"tc_plans:     {df_plans.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2 — `tc_customers`
# MAGIC
# MAGIC Ambiguities:
# MAGIC - `status`: `A=Active`, `S=Suspended`, `C=Churned`, `T=Trial`
# MAGIC - `acq_channel`: `ONL=Online`, `RET=Retail`, `REF=Referral`, `TEL=Telesales`
# MAGIC - `region`: `NW`, `SW`, `NE`, `SE`, `MW`
# MAGIC - `churn_risk_score`: `0–100`, where `>70 = high risk`

# COMMAND ----------

FIRST = [
    "James","Maria","Robert","Patricia","Michael","Jennifer","William","Linda",
    "David","Barbara","Richard","Susan","Joseph","Jessica","Thomas","Sarah",
    "Charles","Karen","Christopher","Lisa","Daniel","Nancy","Matthew","Betty",
    "Anthony","Margaret","Mark","Sandra","Donald","Ashley","Steven","Dorothy",
    "Paul","Kimberly","Andrew","Emily","Kenneth","Donna","Joshua","Michelle",
    "Kevin","Carol","Brian","Amanda","George","Melissa","Timothy","Deborah"
]

LAST = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
    "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker",
    "Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
    "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell"
]

ACTIVE_PLAN_IDS = ["PLN001","PLN002","PLN003","PLN004","PLN005","PLN006","PLN007"]

PLAN_FEES = {
    "PLN001": 24.99,
    "PLN002": 34.99,
    "PLN003": 49.99,
    "PLN004": 59.99,
    "PLN005": 79.99,
    "PLN006": 99.99,
    "PLN007": 149.99,
}

REGIONS = ["NW", "SW", "NE", "SE", "MW"]
CHANNELS = ["ONL", "RET", "REF", "TEL"]
STATUS_POOL = ["A"] * 63 + ["S"] * 10 + ["C"] * 20 + ["T"] * 7

customers_rows = []
for i in range(1, 601):
    plan_id = random.choice(ACTIVE_PLAN_IDS)
    status = random.choice(STATUS_POOL)
    acq_date = _d(date(2020, 1, 1), date(2025, 6, 30))
    lt_months = max(1, (date(2026, 3, 1) - acq_date).days // 30)

    if status == "C":
        risk = random.randint(71, 100)
        last_change = _d(date(2024, 1, 1), date(2026, 2, 28))
    elif status == "S":
        risk = random.randint(48, 83)
        last_change = _d(date(2024, 6, 1), date(2026, 2, 28))
    elif status == "T":
        risk = random.randint(22, 65)
        last_change = acq_date
    else:
        risk = random.randint(0, 100)
        last_change = _d(acq_date, date(2025, 12, 31))

    monthly_spend = round(PLAN_FEES[plan_id] * random.uniform(0.95, 1.28), 2)

    customers_rows.append((
        f"CUST{i:05d}",
        f"{random.choice(FIRST)} {random.choice(LAST)}",
        random.choice(REGIONS),
        plan_id,
        status,
        random.choice(CHANNELS),
        acq_date,
        last_change,
        lt_months,
        risk,
        monthly_spend,
    ))

custs_cols = [
    "customer_id", "full_name", "region", "plan_id", "status", "acq_channel",
    "acq_date", "last_status_change", "lifetime_months", "churn_risk_score",
    "monthly_spend"
]

df_custs = spark.createDataFrame(
    [Row(**dict(zip(custs_cols, r))) for r in customers_rows]
)

df_custs.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.tc_customers")

print(f"tc_customers: {df_custs.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3 — `tc_usage`
# MAGIC
# MAGIC Ambiguities:
# MAGIC - `usage_type`: `V=Voice`, `D=Data`, `S=SMS`, `R=Roaming`
# MAGIC - `unit`: `MIN`, `MB`, `CNT`
# MAGIC - `direction`: `I=Inbound`, `O=Outbound`, `NA=Not applicable`

# COMMAND ----------

UNIT_MAP = {"V": "MIN", "D": "MB", "S": "CNT", "R": "MB"}
non_churned_ids = [r[0] for r in customers_rows if r[4] in ("A", "S", "T")]

usages_rows = []
for i in range(1, 7001):
    cid = random.choice(non_churned_ids)
    utype = random.choices(["V", "D", "S", "R"], weights=[25, 50, 20, 5])[0]
    udate = _d(date(2024, 1, 1), date(2026, 2, 28))
    unit = UNIT_MAP[utype]

    if utype == "V":
        qty, direction = round(random.uniform(0.5, 90.0), 1), random.choice(["I", "O"])
    elif utype == "D":
        qty, direction = round(random.uniform(5.0, 9000.0), 1), "NA"
    elif utype == "S":
        qty, direction = float(random.randint(1, 30)), random.choice(["I", "O"])
    else:
        qty, direction = round(random.uniform(1.0, 400.0), 1), "NA"

    roaming = 1 if utype == "R" else (1 if random.random() < 0.02 else 0)

    usages_rows.append((
        f"USG{i:07d}",
        cid,
        udate,
        utype,
        qty,
        unit,
        direction,
        roaming
    ))

usages_cols = [
    "usage_id", "customer_id", "usage_date", "usage_type",
    "quantity", "unit", "direction", "roaming_flag"
]

df_usages = spark.createDataFrame(
    [Row(**dict(zip(usages_cols, r))) for r in usages_rows]
)

df_usages.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.tc_usage")

print(f"tc_usage:     {df_usages.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4 — `tc_tickets`
# MAGIC
# MAGIC Ambiguities:
# MAGIC - `ticket_type`: `TEC`, `BIL`, `SVC`, `GEN`
# MAGIC - `priority`: `1=Critical`, `2=High`, `3=Medium`, `4=Low`
# MAGIC - `resolution_code`: `R1`, `R2`, `R3`, `R4`
# MAGIC - SLA targets in days: `1→1`, `2→2`, `3→3`, `4→5`

# COMMAND ----------

SLA_DAYS = {1: 1, 2: 2, 3: 3, 4: 5}
all_cust_ids = [r[0] for r in customers_rows]

tickets_rows = []
for i in range(1, 2001):
    cid = random.choice(all_cust_ids)
    created = _d(date(2023, 1, 1), date(2026, 2, 15))
    priority = random.choices([1, 2, 3, 4], weights=[5, 20, 45, 30])[0]
    ttype = random.choices(["TEC", "BIL", "SVC", "GEN"], weights=[40, 30, 15, 15])[0]

    if random.random() < 0.18:
        resolved, rcode, sat = None, None, None
    else:
        sla = SLA_DAYS[priority]
        if random.random() < 0.74:
            offset = random.randint(0, sla)
        else:
            offset = random.randint(sla + 1, sla * 4)

        resolved = min(created + timedelta(days=offset), date(2026, 3, 10))
        rcode = random.choice(["R1", "R2", "R3", "R4"])
        sat = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 18, 37, 32])[0]

    tickets_rows.append((
        f"TKT{i:06d}",
        cid,
        created,
        resolved,
        ttype,
        priority,
        rcode,
        sat
    ))

tickets_cols = [
    "ticket_id", "customer_id", "created_date", "resolved_date",
    "ticket_type", "priority", "resolution_code", "satisfaction_score"
]

df_tickets = spark.createDataFrame(
    [Row(**dict(zip(tickets_cols, r))) for r in tickets_rows]
)

df_tickets.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.tc_tickets")

print(f"tc_tickets:   {df_tickets.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5 — `tc_payments`
# MAGIC
# MAGIC Ambiguities:
# MAGIC - `payment_type`: `MRC`, `OTC`, `ADJ`
# MAGIC - `pmt_method`: `CC`, `DD`, `BT`, `WT`
# MAGIC - `pmt_status`: `S`, `F`, `P`, `R`
# MAGIC - Revenue should only include successful `MRC` and `OTC`
# MAGIC - `ADJ` amounts are negative and excluded from revenue totals

# COMMAND ----------

PMT_STATUS_POOL = ["S"] * 78 + ["F"] * 9 + ["P"] * 8 + ["R"] * 5
PMT_METHODS = ["CC", "DD", "BT", "WT"]

payments_rows = []
pid = 1

mrc_custs = [(r[0], PLAN_FEES[r[3]]) for r in customers_rows if r[4] in ("A", "T", "S")]
for cid, fee in mrc_custs:
    for mo in range(6):
        pay_date = date(2025, 9, 1) + timedelta(days=30 * mo + random.randint(-2, 2))
        bm = f"{pay_date.year}-{pay_date.month:02d}"
        amount = round(fee * random.uniform(0.99, 1.01), 2)

        payments_rows.append((
            f"PMT{pid:07d}",
            cid,
            pay_date,
            amount,
            "MRC",
            random.choice(PMT_METHODS),
            random.choice(PMT_STATUS_POOL),
            bm
        ))
        pid += 1

active_ids = [r[0] for r in customers_rows if r[4] == "A"]
for _ in range(280):
    cid = random.choice(active_ids)
    pdate = _d(date(2025, 1, 1), date(2026, 2, 28))
    bm = f"{pdate.year}-{pdate.month:02d}"
    amount = round(random.uniform(15.0, 180.0), 2)

    payments_rows.append((
        f"PMT{pid:07d}",
        cid,
        pdate,
        amount,
        "OTC",
        random.choice(PMT_METHODS),
        random.choice(PMT_STATUS_POOL),
        bm
    ))
    pid += 1

for _ in range(110):
    cid = random.choice(active_ids)
    pdate = _d(date(2025, 1, 1), date(2026, 2, 28))
    bm = f"{pdate.year}-{pdate.month:02d}"
    amount = round(random.uniform(-45.0, -5.0), 2)

    payments_rows.append((
        f"PMT{pid:07d}",
        cid,
        pdate,
        amount,
        "ADJ",
        random.choice(PMT_METHODS),
        "S",
        bm
    ))
    pid += 1

payments_cols = [
    "payment_id", "customer_id", "payment_date", "amount",
    "payment_type", "pmt_method", "pmt_status", "billing_month"
]

df_payments = spark.createDataFrame(
    [Row(**dict(zip(payments_cols, r))) for r in payments_rows]
)

df_payments.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.tc_payments")

print(f"tc_payments:  {df_payments.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("\n✅ All 5 tables created successfully")
print(f"   {CATALOG}.{SCHEMA}.tc_plans")
print(f"   {CATALOG}.{SCHEMA}.tc_customers")
print(f"   {CATALOG}.{SCHEMA}.tc_usage")
print(f"   {CATALOG}.{SCHEMA}.tc_tickets")
print(f"   {CATALOG}.{SCHEMA}.tc_payments")

# Optional quick checks
display(spark.table(f"`{CATALOG}`.`{SCHEMA}`.tc_plans"))
display(spark.table(f"`{CATALOG}`.`{SCHEMA}`.tc_customers").limit(10))
display(spark.table(f"`{CATALOG}`.`{SCHEMA}`.tc_usage").limit(10))
display(spark.table(f"`{CATALOG}`.`{SCHEMA}`.tc_tickets").limit(10))
display(spark.table(f"`{CATALOG}`.`{SCHEMA}`.tc_payments").limit(10))

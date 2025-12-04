import time
import pandas as pd
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# ---- Read arguments from workflow ----
args = sys.argv

model_name = args[args.index("--model_name") + 1]
model_version = args[args.index("--model_version") + 1]
run_id = args[args.index("--run_id") + 1]

# Your Google Sheet CSV URL
GOOGLE_SHEET_CSV = (
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vTa_8A_mZwDsAkkQeU2RSBLhQZ-lDsHj1uE_tv2QPvBigC40AogVhQOGsKcG_dm5WfQB9RAqi_j8vFM/pub?gid=0&single=true&output=csv"
)

def fetch_sheet():
    try:
        return pd.read_csv(GOOGLE_SHEET_CSV)
    except Exception as e:
        print("⚠️ Could not read sheet:", e)
        return pd.DataFrame()

print(f"🔍 Waiting for approval: run_id={run_id}")

while True:
    df = fetch_sheet()

    if df.empty:
        print("📭 Sheet empty... waiting...")
        time.sleep(10)
        continue

    # REQUIRE COLUMNS: run_id, approved_flag
    expected_cols = {"run_id", "approved_flag"}
    if not expected_cols.issubset(df.columns):
        print("⚠️ Sheet missing required columns: run_id, approved_flag")
        print("Found columns:", df.columns)
        time.sleep(10)
        continue

    # Match run_id row
    rows = df[df["run_id"].astype(str) == str(run_id)]
    flag = str(rows.iloc[-1]["approved_flag"]).strip().upper() 

    if rows.empty:
        print(f"⏳ run_id {run_id} not found yet... waiting...")
        time.sleep(10)
        continue

    print(f"🔎 Found approval flag for {run_id}: {flag}")

    if flag == "TRUE":
        print("✅ Model APPROVED — moving forward...")
        dbutils.jobs.taskValues.set(key="approval", value="APPROVED")
        break

    if flag == "FALSE":
        print("❌ Model REJECTED — stopping workflow...")
        dbutils.jobs.taskValues.set(key="approval", value="REJECTED")
        break

    print("⏳ Waiting for approval flag to become TRUE/FALSE...")
    time.sleep(10)

print("🏁 Approval check finished.")

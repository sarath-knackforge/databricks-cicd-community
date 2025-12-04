
#!/usr/bin/env python3
import argparse
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from model_deploy.serving.model_serving import ModelServing

parser = argparse.ArgumentParser()

parser.add_argument("--root_path", required=True, type=str)
parser.add_argument("--env", required=True, type=str)
parser.add_argument("--model_name", required=True)    # UC model name

args = parser.parse_args()

# ---------------------------------------------------------
# Load model_version from check_model_version task
# ---------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

model_version = dbutils.jobs.taskValues.get(
    taskKey="check_model_version",
    key="model_version"
)
if not model_version:
    logger.error("❌ No model version found from check_model_version task.")
    exit(1)

logger.info(f"Model Version to Deploy: {model_version}")


# ---------------------------------------------------------
# READ APPROVAL DECISION
# ---------------------------------------------------------
approval_status = dbutils.jobs.taskValues.get(
    taskKey="wait_for_approval",
    key="approval"
)

logger.info(f"Approval Status from Email: {approval_status}")

if approval_status != "APPROVED":
    logger.warning("⛔ Deployment aborted — Model NOT approved.")
    exit(0)


logger.info("✔ Approval confirmed. Proceeding with deployment...")


# ---------------------------------------------------------
# CLEAN SERVING ENDPOINT NAME (Required by Databricks)
# ---------------------------------------------------------

# 1. Lowercase
clean_name = args.model_name.lower()

# 2. Replace invalid chars
clean_name = clean_name.replace(".", "-").replace("_", "-")

# 3. Append env
endpoint_name = f"{clean_name}-serving-{args.env}"

# 4. Ensure <= 63 chars
endpoint_name = endpoint_name[:63]

logger.info(f"Using endpoint name: {endpoint_name}")

# ---------------------------------------------------------
# Initialize Serving Manager
# ---------------------------------------------------------
model_serving = ModelServing(
    model_name=args.model_name,     # UC model name (not cleaned)
    endpoint_name=endpoint_name     # Clean serving name
)

# Deploy endpoint
model_serving.deploy_or_update_serving_endpoint(version=model_version)

logger.info("Deployment/update triggered successfully.")

# ---------------------------------------------------------
# PRINT SERVING ENDPOINT URL
# ---------------------------------------------------------
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

serving_url = f"https://{workspace_url}/serving-endpoints/{endpoint_name}/invocations"

logger.info("🔥 Model successfully deployed!")
logger.info(f"🚀 Serving Endpoint URL:\n{serving_url}")

spark.sql("""
UPDATE mlops_dev.model_test.training_control
SET allow_training = false, updated_on = current_timestamp()
""")

logger.info("🔒 Training disabled until manually reset (allow_training = false).")

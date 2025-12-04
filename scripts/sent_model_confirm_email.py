# --- IMPORTS ---
import mlflow
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
client = mlflow.tracking.MlflowClient()


# ---------------------------------------------------------
# 1. GET LATEST EXPERIMENT RUN ID
# ---------------------------------------------------------
experiment = mlflow.get_experiment_by_name("/Shared/mlops_exp")

if experiment is None:
    mlflow.create_experiment("/Shared/mlops_exp")
    experiment_id = mlflow.get_experiment_by_name("/Shared/mlops_exp").experiment_id
else:
    experiment_id = experiment.experiment_id

runs = client.search_runs(
    experiment_ids=[experiment_id],
    order_by=["start_time DESC"],
    max_results=1
)

if len(runs) == 0:
    raise Exception("❌ No MLflow runs found in any experiment. Cannot send approval email.")

latest_run = runs[0]
experiment_run_id = latest_run.info.run_id
print("🔥 experiment_run_id =", experiment_run_id)

# ---------------------------------------------------------
# 2. REGISTER MODEL FROM EXPERIMENT RUN
# ---------------------------------------------------------
# below one is for github CICD standard command
mlflow.set_registry_uri("databricks-uc")

model_name = "mlops_prod.model_test.credit_risk_model"

# below one is for databricks standard bundle command
# mlflow.set_registry_uri("databricks-uc")  # ✅ REQUIRED

# model_name = "credit_risk_model"
model_uri = f"runs:/{experiment_run_id}/model"

mv = mlflow.register_model(model_uri, model_name)
model_version = mv.version
registered_run_id = mv.run_id     # <-- THIS IS THE ONE YOU MUST USE EVERYWHERE

print("📌 Dynamic model_version =", model_version)
print("📌 Registered model run_id =", registered_run_id)

dbutils.jobs.taskValues.set(key="registered_run_id", value=registered_run_id)
print("🚀 Saved registered_run_id for pipeline:", registered_run_id)

# in your train script after you log model to MLflow and have run_id variable
dbutils.jobs.taskValues.set(key="candidate_run_id", value=registered_run_id)


# ---------------------------------------------------------
# 3. CREATE SCHEMA + TABLE FOR APPROVALS
# ---------------------------------------------------------
# Ensure schema exists
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS default")
except Exception as e:
    print("⚠️ Schema default already exists:", e)

# Create table in DEFAULT schema
spark.sql("""
CREATE TABLE IF NOT EXISTS mlops_prod.model_test.model_approvals (
  run_id STRING,
  model_name STRING,
  model_version STRING,
  status STRING,
  requested_on TIMESTAMP
)
""")

# Insert pending approval
spark.sql(f"""
INSERT INTO mlops_prod.model_test.model_approvals
VALUES ('{registered_run_id}', '{model_name}', '{model_version}', 'PENDING', current_timestamp())
""")


# ---------------------------------------------------------
# 4. CREATE APPROVAL URL (FULLY DYNAMIC)
# ---------------------------------------------------------
# approval_url = (
#     f"https://databricks-approval-backend.onrender.com/approve"
#     f"?run_id={registered_run_id}"
#     f"&model_name={model_name}"
#     f"&model_version={model_version}"
# )

approve_url = (
    f"https://databricks-approval-backend.onrender.com/approve"
    f"?run_id={registered_run_id}"
    f"&model_name={model_name}"
    f"&model_version={model_version}"
    f"&action=APPROVE"
)

reject_url = (
    f"https://databricks-approval-backend.onrender.com/approve"
    f"?run_id={registered_run_id}"
    f"&model_name={model_name}"
    f"&model_version={model_version}"
    f"&action=REJECT"
)



# ---------------------------------------------------------
# 5. SEND EMAIL
# ---------------------------------------------------------
# used for databrick trail gmail version
# sender_email = dbutils.secrets.get("mlops", "email_user")
# sender_password = dbutils.secrets.get("mlops", "email_password")

# used for databrick community version
sender_email = dbutils.secrets.get("mlops_community", "email_user")
sender_password = dbutils.secrets.get("mlops_community", "email_password")
receiver_email = "sarathkumar.r@knackforge.com"

msg = MIMEMultipart("alternative")
msg["Subject"] = f"Approval Needed: {model_name} v{model_version}"
msg["From"] = sender_email
msg["To"] = receiver_email

# html = f"""
# <html>
# <body>
# <h3>New Model Ready for Deployment</h3>
# <p><b>Model:</b> {model_name}</p>
# <p><b>Version:</b> {model_version}</p>
# <p><b>Run ID:</b> {registered_run_id}</p>

# <a href="{approval_url}">
# <button style="background-color:green;color:white;padding:10px 20px;border:none;border-radius:6px;font-size:16px;">
# Approve Model Deployment
# </button>
# </a>
# </body>
# </html>
# """
html = f"""
<html>
<body>
<h3>New Model Ready for Deployment</h3>

<p><b>Model:</b> {model_name}</p>
<p><b>Version:</b> {model_version}</p>
<p><b>Run ID:</b> {registered_run_id}</p>

<div style="margin-top:20px;">

    <a href="{approve_url}" style="text-decoration:none;">
        <button style="background-color:green;color:white;padding:10px 20px;
                       border:none;border-radius:6px;font-size:16px;margin-right:10px;">
            APPROVE
        </button>
    </a>

    <a href="{reject_url}" style="text-decoration:none;">
        <button style="background-color:red;color:white;padding:10px 20px;
                       border:none;border-radius:6px;font-size:16px;">
            CANCEL
        </button>
    </a>

</div>

</body>
</html>
"""


msg.attach(MIMEText(html, "html"))

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(sender_email, sender_password)
server.sendmail(sender_email, receiver_email, msg.as_string())
server.quit()

print("📩 Approval email sent successfully.")

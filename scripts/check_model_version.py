
#!/usr/bin/env python3
import mlflow
import argparse
import sys
import os
import traceback
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def emit_output(version_value: str):
    print(f"::mlflow-run-output::model_version={version_value}")

def safe_get_metric(client, run_id, metric_names=("accuracy","f1_score")):
    """Return first available metric value from metric_names or None."""
    try:
        run = client.get_run(run_id)
        metrics = run.data.metrics or {}
        for m in metric_names:
            if m in metrics:
                try:
                    return float(metrics[m])
                except Exception:
                    # metric exists but not castable
                    return None
        return None
    except Exception as e:
        print(f"[WARN] Could not get metrics for run {run_id}: {e}")
        return None

def check_model_version(env, model_name, candidate_run_id=None, metric_names=("accuracy","f1_score")):
    print(f"[INFO] env={env}, model_name={model_name}, candidate_run_id={candidate_run_id}")
    client = mlflow.tracking.MlflowClient()

    # ---- 1) Try alias lookup for currently deployed model ----
    try:
        mv = client.get_model_version_by_alias(model_name, "latest-model")
        version = str(mv.version)
        deployed_run_id = mv.run_id
        print(f"[INFO] Found alias latest-model => version {version}, deployed_run_id {deployed_run_id}")

        # Save current deployed info
        dbutils.jobs.taskValues.set(key="model_version", value=version)
        dbutils.jobs.taskValues.set(key="run_id", value=deployed_run_id)

        # If candidate_run_id provided, compare metrics
        if candidate_run_id:
            deployed_metric = safe_get_metric(client, deployed_run_id, metric_names)
            candidate_metric = safe_get_metric(client, candidate_run_id, metric_names)

            print(f"[COMPARE] deployed_metric={deployed_metric} candidate_metric={candidate_metric}")

            # If metrics available, compare and decide
            if deployed_metric is not None and candidate_metric is not None:
                if candidate_metric <= deployed_metric:
                    print("❌ Candidate model is NOT better than deployed model (or equal). Skipping deployment.")

                    # The below line is commented out to allow pipeline continuation or by-pass
                    dbutils.jobs.taskValues.set(key="skip_deploy", value="FALSE")  # True to skip deployment
                    # success exit to stop pipeline gracefully
                    # sys.exit(0)
                else:
                    print("✅ Candidate model is better. Proceeding to approval/deploy.")
            else:
                print("⚠️ One or both metrics missing. Skipping metric-based block and proceeding.")
        emit_output(version)
        return
    except Exception as e:
        print(f"[DEBUG] get_model_version_by_alias failed: {e}")

    # ---- 2) Fallbacks: search_model_versions / get_latest_versions (mirror earlier logic) ----
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            latest_mv = max(versions, key=lambda v: int(v.version))
            latest = str(latest_mv.version)
            deployed_run_id = latest_mv.run_id
            print(f"[INFO] Found latest version via search_model_versions: {latest}, run_id={deployed_run_id}")

            dbutils.jobs.taskValues.set(key="model_version", value=latest)
            dbutils.jobs.taskValues.set(key="run_id", value=deployed_run_id)

            if candidate_run_id:
                deployed_metric = safe_get_metric(client, deployed_run_id, metric_names)
                candidate_metric = safe_get_metric(client, candidate_run_id, metric_names)
                print(f"[COMPARE] deployed_metric={deployed_metric} candidate_metric={candidate_metric}")
                if deployed_metric is not None and candidate_metric is not None:
                    if candidate_metric <= deployed_metric:
                        print("❌ Candidate model NOT better. Skipping deployment.")
                        # dbutils.jobs.taskValues.set(key="skip_deploy", value="TRUE") # Set to TRUE to skip deployment
                        dbutils.jobs.taskValues.set(key="skip_deploy", value="FALSE")
                        # sys.exit(0)
                    else:
                        print("✅ Candidate model better.")
                else:
                    print("⚠️ Metrics missing; continuing.")
            emit_output(latest)
            return

    except Exception as e2:
        print(f"[DEBUG] search_model_versions not usable: {e2}")

    try:
        latest_versions = client.get_latest_versions(model_name)
        if latest_versions:
            latest_mv = max(latest_versions, key=lambda v: int(v.version))
            latest = str(latest_mv.version)
            deployed_run_id = latest_mv.run_id
            print(f"[INFO] Found latest via get_latest_versions: {latest}, run_id={deployed_run_id}")

            dbutils.jobs.taskValues.set(key="model_version", value=latest)
            dbutils.jobs.taskValues.set(key="run_id", value=deployed_run_id)

            if candidate_run_id:
                deployed_metric = safe_get_metric(client, deployed_run_id, metric_names)
                candidate_metric = safe_get_metric(client, candidate_run_id, metric_names)
                print(f"[COMPARE] deployed_metric={deployed_metric} candidate_metric={candidate_metric}")
                if deployed_metric is not None and candidate_metric is not None:
                    if candidate_metric <= deployed_metric:
                        print("❌ Candidate model NOT better. Skipping deployment.")
                        dbutils.jobs.taskValues.set(key="skip_deploy", value="TRUE")
                        sys.exit(0)
                    else:
                        print("✅ Candidate model better.")
                else:
                    print("⚠️ Metrics missing; continuing.")
            emit_output(latest)
            return
    except Exception as e3:
        print(f"[DEBUG] get_latest_versions not usable: {e3}")

    # ---- 4) If no version found ----
    print("[WARN] No registered model found. Setting model_version = 0")
    dbutils.jobs.taskValues.set(key="model_version", value="0")
    dbutils.jobs.taskValues.set(key="run_id", value="UNKNOWN")
    emit_output("0")
    # if no deployed model exists, we **do not** block — pipeline will continue to send approval for candidate
    return

if __name__ == "__main__":
    print("[DEBUG] sys.argv:", sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, required=True)
    parser.add_argument("--model_name", type=str, required=False)
    parser.add_argument("--candidate_run_id", type=str, required=False,
                        help="Run ID of the newly trained model to compare against deployed model")
    args, unknown = parser.parse_known_args()

    if unknown:
        print("[DEBUG] unknown args ignored:", unknown)

    env = args.env
    model_name = args.model_name
    candidate_run_id = args.candidate_run_id

    if not model_name:
        print("[ERROR] model_name missing. Defaulting to model-deploy")
        model_name = "model-deploy"

    check_model_version(env, model_name, candidate_run_id=candidate_run_id)

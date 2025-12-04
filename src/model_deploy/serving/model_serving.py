"""Model serving module for Marvel characters."""

import time
import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
from databricks.sdk.errors import ResourceAlreadyExists


class ModelServing:
    """Manages model serving in Databricks for Marvel characters."""

    def __init__(self, model_name: str, endpoint_name: str) -> None:
        """Initialize the Model Serving Manager.

        :param model_name: Name of the model to be served
        :param endpoint_name: Name of the serving endpoint
        """
        self.workspace = WorkspaceClient()
        self.endpoint_name = endpoint_name
        self.model_name = model_name

    def get_latest_model_version(self) -> str:
        """Retrieve the latest version of the model.

        :return: Latest version of the model as a string
        """
        client = mlflow.MlflowClient()
        latest_version = client.get_model_version_by_alias(
            self.model_name,
            alias="latest-model"
        ).version
        print(f"Latest model version: {latest_version}")
        return latest_version

    # ✅ ✅ ✅ NEW: WAIT UNTIL ENDPOINT IS FREE BEFORE UPDATING
    def wait_until_endpoint_ready(self, timeout: int = 600) -> None:
        print("⏳ Waiting for serving endpoint to be READY before updating...")

        start = time.time()

        while True:
            ep = self.workspace.serving_endpoints.get(self.endpoint_name)
            state = ep.state.config_update

            print(f"Current serving state: {state}")

            # ✅ This means Databricks is NOT updating anymore
            if state == "NOT_UPDATING":
                print("✅ Endpoint is free for update")
                return

            # ⛔ Safety timeout
            if time.time() - start > timeout:
                raise TimeoutError("❌ Timeout waiting for endpoint to be ready")

            time.sleep(15)

    def deploy_or_update_serving_endpoint(
        self,
        version: str = "latest",
        workload_size: str = "Small",
        scale_to_zero: bool = True,
    ) -> None:

        entity_version = self.get_latest_model_version() if version == "latest" else version

        served_entities = [
            ServedEntityInput(
                entity_name=self.model_name,
                scale_to_zero_enabled=scale_to_zero,
                workload_size=workload_size,
                entity_version=entity_version,
            )
        ]

        try:
            # ✅ SAFE CREATE (FIRST TIME)
            self.workspace.serving_endpoints.create(
                name=self.endpoint_name,
                config=EndpointCoreConfigInput(served_entities=served_entities),
            )
            print(f"✅ Serving endpoint CREATED: {self.endpoint_name}")

        except ResourceAlreadyExists:
            # ✅ SAFE UPDATE (ALL REDEPLOYS)
            print(f"⚠️ Serving endpoint EXISTS — updating: {self.endpoint_name}")

            # ✅ ✅ ✅ CRITICAL FIX: WAIT BEFORE UPDATING
            self.wait_until_endpoint_ready()

            self.workspace.serving_endpoints.update_config(
                name=self.endpoint_name,
                served_entities=served_entities,
            )

            print(f"✅ Serving endpoint UPDATED: {self.endpoint_name}")

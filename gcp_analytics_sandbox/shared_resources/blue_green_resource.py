# resources.py
import yaml
from pathlib import Path
from datetime import datetime, timezone
from pydantic import PrivateAttr
import dagster as dg
from dagster_gcp import BigQueryResource
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import Forbidden, GoogleAPICallError

from typing import Union

class BlueGreenDeploymentResource(dg.ConfigurableResource):
    bigquery: BigQueryResource
    config_path: str
    # can also just load your own YAML fields to initialize your datasets instead of passing them as arguments
    default_live : str
    default_audit : str
    status_label_key: str = "deployment_status"
    
    # Private state attributes using Pydantic's PrivateAttr, which must start with an underscore
    # and won't be included in the resource's config
    _state_cache: dict = PrivateAttr(default_factory=dict)

    # --- private functions
    def _load_state_from_yaml(self) -> dict:
        """Loads state from the YAML config."""
        try:
            with open(Path(self.config_path), "r") as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
                return config
        except FileNotFoundError:
            return {}

    # write state to yaml
    def _persist_state_to_yaml(self):
        """Writes the state back to the YAML config."""
        try:
            with open(Path(self.config_path), "r") as f:
                config = yaml.load(f, Loader=yaml.SafeLoader) or {}
        except FileNotFoundError:
            config = {
                "default_live": self.default_live, 
                "default_audit": self.default_audit
                }
        # merges the dictionary
        self._state_cache["local_write_status_at"] = datetime.now(timezone.utc).isoformat()
        config.update(self._state_cache)
        with open(Path(self.config_path), "w") as f:
            yaml.dump(config, f, sort_keys=False)

    def _set_label(self, context: Union[dg.InitResourceContext, dg.AssetExecutionContext], dataset_id: str, status: str):
        context.log.info(f"Setting label '{self.status_label_key}: {status}' on {dataset_id}")
        try:
            with self.bigquery.get_client() as client:
                dataset = client.get_dataset(dataset_id)
                if dataset.labels is None:
                    dataset.labels = {}
                dataset.labels[self.status_label_key] = status
                dataset.labels["labels_updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d-%Hh%Mm%Ss-%Z").lower()
                client.update_dataset(dataset, ["labels"])
        except (NotFound, Forbidden, GoogleAPICallError) as e:
            raise dg.Failure(
                description=f"Failed to set label on BigQuery dataset '{dataset_id}'. Error: {e}"
            )

    def setup_for_execution(self, context: dg.InitResourceContext):
        """Dagster's lifecycle function, ran before resource is used for execution"""
        context.log.info("Fetching WAP dataset status from BigQuery labels...")
        self.update_state_from_bq(context)
        self._persist_state_to_yaml()

        # can also just load from the local state, instead of fetching from BQ at startup, e.g: 
        
        """ 
        config = self._load_state_from_yaml()
        if not config:
            self.update_state_from_bq(context)
        self._persist_state_to_yaml()
        """

    def teardown_for_execution(self, context: dg.InitResourceContext):
        """Dagster's lifecycle function, ran after execution"""
        self._persist_state_to_yaml()
        context.log.info("Blue-green resource teardown, writing state to YAML.")


    # public resource methods
    def get_live_dataset(self) -> str:
        return self._state_cache.get("live_dataset")
    def get_audit_dataset(self) -> str:
        return self._state_cache.get("audit_dataset")
    
    # main function for dbt's CLI
    def get_live_env_suffix(self) -> str:
        return self._state_cache.get("live_env_suffix")
    def get_audit_env_suffix(self) -> str:
        return self._state_cache.get("audit_env_suffix")
    

    def promote_audit_to_live(self, context: dg.AssetExecutionContext):
        old_live = self.get_live_dataset()
        old_audit = self.get_audit_dataset()

        context.log.info(f"Starting promotion of '{old_audit}' to live.")
        
        # This try/except block is still valuable because it adds specific,
        # critical context to a failure from the lower-level _set_label function.
        try:
            self._set_label(context, old_live, "audit")
            self._set_label(context, old_audit, "live")
            self._state_cache.update(
                {
                    "label_set_at": datetime.now(timezone.utc).isoformat(), # or strftime("%Y-%m-%d %H:%M:%S %Z")
                    "label_set_reason": f"Promoted {old_audit} to live, demoted {old_live} to next audit dataset"
                })
        except (NotFound, Forbidden, GoogleAPICallError) as e:
            raise dg.Failure(
                description=f"Failed to swap labels during promotion. BigQuery state may be inconsistent. Error: {e}",
                metadata={
                    "error_message": e,
                    "warning": "Check BigQuery labels for datasets immediately.",
                    "current_live": old_live,
                    "current_audit": old_audit,
                    "attempted_swap_to_live": old_audit,
                    "attempted_swap_to_audit": old_live,
                }
            )
        
        self.update_state_from_bq(context)
        if self.get_live_dataset() != old_audit or self.get_audit_dataset() != old_live:
            raise dg.Failure(
                description=f"Updated state labels don't align with previous state labels. BigQuery state may be inconsistent. Error: {e}",
                metadata={
                    "old_live": old_audit,
                    "old_audit": old_live,
                    "new_live": self.get_live_dataset(),
                    "new_audit": self.get_audit_dataset(),
                }
            )
            
        self._persist_state_to_yaml()
        context.log.info(f"Promotion complete. New live dataset: {self.get_live_dataset()}")
            
    def update_state_from_bq(self, context: Union[dg.InitResourceContext, dg.AssetExecutionContext]):
        live_ds = None
        audit_ds = None
        with self.bigquery.get_client() as client:

            datasets_to_check = [self._state_cache.get("live_dataset"), self._state_cache.get("audit_dataset")]
            if None in datasets_to_check:
                datasets_to_check = [self.default_live, self.default_audit]

            for dataset_name in datasets_to_check:
                dataset = client.get_dataset(dataset_name) # Can raise NotFound, Forbidden, etc.
                # if you allow for minor problems, such as no labels but a default handle
                if not dataset.labels:
                    context.log.info(f"Dataset {dataset_name} has no labels.")
                    # raise Exception(f"Dataset {dataset_name} has no labels.")
                status = dataset.labels.get(self.status_label_key, None)
                if not status:
                    context.log.info(f"Dataset {dataset_name} has no '{self.status_label_key}' label.")
                    # raise Exception(f"Dataset {dataset_name} has no '{self.status_label_key}' label.")
                
                if status == "live":
                    # major potential problems
                    if live_ds: 
                        raise dg.Failure(f"Found >1 live dataset: {live_ds} and {dataset.dataset_id}")
                    live_ds = dataset.dataset_id
                elif status == "audit":
                    if audit_ds: 
                        raise dg.Failure(f"Found >1 audit dataset: {audit_ds} and {dataset.dataset_id}")
                    audit_ds = dataset.dataset_id

            # in the above, we handle if dataset has no labels, otherwise if you can ensure datasets have 
            # a label then you could change the above filter bigquery datasets by label immediately:

            # live_label_filter = "labels.deployment_status:live"
            # live_datataset = list_datasets(filter=live_label_filter)

        if not live_ds or not audit_ds:
            live_ds = self.default_live
            audit_ds = self.default_audit
            context.log.info(f"Could not determine roles from labels. Setting labels to defaults. Live: {live_ds}, Audit: {audit_ds}")
            self._set_label(context, live_ds, "live")
            self._set_label(context, audit_ds, "audit")
            self._state_cache.update(
                {
                    "label_set_at": datetime.now(timezone.utc).isoformat(), # or strftime("%Y-%m-%d %H:%M:%S %Z")
                    "label_set_reason": f"Could not determine roles from labels. Setting labels to defaults. Live: {live_ds}, Audit: {audit_ds}"
                })

        self._state_cache.update({
            "live_dataset": live_ds,
            "live_env_suffix": live_ds.split('_')[-1],
            "audit_dataset": audit_ds,
            "audit_env_suffix": audit_ds.split('_')[-1],
            "fetched_bigquery_status_at": datetime.now(timezone.utc).isoformat(), # or strftime("%Y-%m-%d %H:%M:%S %Z")
        })

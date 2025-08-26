from dagster_gcp import GCSResource, BigQueryResource
from dagster import EnvVar

#assuming we have our 'GOOGLE_APPLICATION_CREDENTIALS' set up as an environment variable

#for use with the google cloud storage client to ingest data to GCS for a given project
gcs_resource = GCSResource(project=EnvVar("GOOGLE_CLOUD_PROJECT"))
bq_resource = BigQueryResource(project=EnvVar("GOOGLE_CLOUD_PROJECT"))



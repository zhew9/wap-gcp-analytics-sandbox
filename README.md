# BlueGreenPrint: A Write-Audit-Publish and Blue-Green-Deployment Example for BigQuery and End-to-End, ELT Dagster Pipeline and GCP Analytics Sandbox 
![image](/images/pipeline_img.png)

## Introduction

This repo is a dev/local-first, end-to-end ELT pipeline orchestrated in Dagster that sets up an analytics sandbox for GCP/BigQuery. It aims to be an example blueprint for implementing an incremental, no-downtime, Write-Audit-Publish deployment for `BigQuery` and also includes a general example for building a full-lifecycle ELT pipeline in `Dagster` for data warehouses that support `dbt` - and as a ready-to-use, baseline, analytics sandbox for quickly booting up analytics or pipeline related Proof-of-Concepts in `GCP`.

It's has examples of more realistic & comprehensive OLTP to OLAP model transformations and as well as examples of some production-grade patterns like setting up your general data architecture to support more efficient `Write-Audit-Publish` and incremental approaches to reduce cost & time for writes to your audit environment and how to implement no-downtime deployment to production with a generalizable Blue Green Deployment approach (hence the project name) - it's not only useful for adapting WAP for `BigQuery`, but this blueprint is useful not only for adapting WAP for BigQuery, but the blue-green view promotion method is also a robust approach for other data warehouses, especially those lacking more direct Publish steps. (This breaks up a long sentence)..

---

### Who is this for?

This project is a ready-to-use/plug-and-play development pipeline which you can use to set up your own proof-of-concepts on GCP, (just swap in your own GCP credentials).
  - The base pipeline is designed to fit entirely within **GCP's free tier**, so anyone can run it for free (while being mindful of usage limits).

This project may also be a useful reference for data professionals or enthusiasts interested in:

- **Advanced Dagster Orchestration**: See a comprehensive, end-to-end example (~160 assets) of Dagster's asset-centric orchestration: from setting up assets for both scheduled runs and backfills, custom resource/IO management, to building asset factories for modular & config-defined pipelines. It especially may be a helpful example for people transitioning from Airflow/similar task-centric orchestrators, and appeal to people who are interested in smooth-running orchestrator for local dev environments (from my experience, it's a much smoother setup than local Airflow & Astronomer, and definitely smoother than their CEO & HRs reaction at that Coldplay concert).
- **Deeper dbt & Dagster Integration**: Explore an integration that includes asset-to-model and model-to-asset lineage-mapping and dependencies, going beyond simple CLI execution. Including a couple of current workarounds for Dagster, for features that aren't supported yet.
- **Real-World dbt Sandbox Workflow**: See examples of full incremental workflows, moving beyond a 3-layer medallion architecture to adapt to WAP or more complex transformations, some different SCD examples, approaches for handling late-arriving data, and more complex intermediate transformations like proration of data across different levels of granularity
- **Customizable and Realistic OLTP Source Data**: Work with a sandbox that generates its own incremental & backfill data, which you can extend and customize, to mimic more realistic OLTP source data.
- **An ELT Workflow in Python**: A full Python, ELT pipeline example using core adapters and libraries, without incorporating third-party services.
- **Production-Ready Deployment Patterns**: Understand how to implement the **Write-Audit-Publish (WAP)** pattern, especially for scalable/efficient (incremental/no full-rebuilds) writes and no-downtime deployment strategies (Blue Green deployment strategies for publishing data), which are available to data warehouses that offer metadata-based table clones/copies (table clones in BigQuery, zero-copy clones in Snowflake, shallow clones in Databricks, branches in Iceberg - seemingly everyone except Redshift?) but with some differences for the Publish step depending on what warehouse you use (the Publish step accounts for most of the differences between different warehouses' WAP support):

### My brief comparison of Write-Audit-Publish options on different data warehouses (as of 2025-08) 
  * For **`BigQuery`**: for the approach actually shown in the project: the Publish step involves changing your downstream views to point to one of two datasets that alternate as our prod and audit environments. Each WAP cycle is as follows: we clean/drop the audit environment at the start of each cycle, we then use the live dataset as a source to `TABLE CLONE` over incremental tables/non-full-rebuild writes to the audit env, then we write our transformations and audit the results, once they pass we just change where the downstream views point to and drop the old dataset
  * For **`Snowflake`**: they probably have the most straightforward and smooth Publish approach, no extra views layer or table merges, after doing similar cloning and auditing -> just swap out the dataset/schema names with an atomic `ALTER SCHEMA ... SWAP WITH`
  * For **`Iceberg`** catalogs: create audit env and 'clone' using table branches, then after auditing the transformations on your branch, merge your branch to the main with a `.fastForwardBranch(... , ...)` call - which is just behind Snowflake in terms of simplicity (it's a metadata/pointer based merge)
  * For **`Databricks`**: their documents are pretty unclear on what WAP implementation options they officially support (so I have to do a bit of guess work) and the main method has a potentially less robust and more restrictive Publish workflow than the above: 
    * Firstly, we can't use methods that involve dropping base tables after cloning - their docs say dropping the base tables of their `SHALLOW CLONE` will break the clones, so that restricts a few Publish workflows 
    * The main option presented seems to be shallow cloning live tables to the audit environment and then after transforming/auditing, merge the shallow clones back into their original base table as the Publish step w/ a [potential example here](https://docs.databricks.com/aws/en/delta/clone#use-clone-for-short-term-experiments-on-a-production-table). However, it's unclear if the `MERGE INTO` operation is like that of Iceberg branches (where the merge is only a metadata/pointer operation) or a standard, data-moving merge operation - if it's data-moving, then even after auditting on your clones for transformation logic -> there will still be a physical data transformation that you'd still be performing outside of your audit environment (which if it allows potential failures entering production, won't fit a strict WAP implementation)
      * I'm not trying to falsely criticize Databricks and hopefully someone with more experience implementing WAP on Databricks can clear up if the `MERGE INTO` for `SHALLOW CLONE`s is just a metadata/pointer operation or not, and if there is another WAP option in Databricks
      * The potential data-moving `MERGE INTO` isn't so much of a problem for cost: depending on their shallow clone implemention, their combined clone transformations + merge cost might be equivalent to the clone transformation costs on BigQuery/Snowflake/Iceberg - but it's mainly about having a trailing, data-moving transformation on the original/production table instead of having every physical transformations be done in the audit environment
  * For **other data warehouses** that don't support metadata-based table clones/copies then you can still do full-rebuild WAP -> you just have to pay for more expensive operations when building your audit environment
    * Also for data warehouses that don't support the better Publish methods but still support fast schema renames, you could still go for a 3-step rename Publish approach but it doesn't guarantee zero-downtime availability and is less robust than the view promotion or atomic swap/metadata merge approaches, especially for failures:
    * For example, if some view promotions or atomic swap/metadata merges failed, your live views/tables still have access to old data and you can limit active usage to old data until failures are resolved whereas if 3-step rename fails then your live views/tables are just down until the failures are resolved
  

---

### Original Motivations:

I originally built this pipeline for my own Proof-of-Concepts on GCP, most recent of which was for testing a form of Write-Audit-Publish using Blue Green Deployment approach for View layer promotion for BigQuery* (hence the project name BlueGreenPrint).

But I thought it might be useful to other data professionals or enthusiasts in the community, either as a ready-to-go usable dev pipeline or as a learning resource.

  >*BigQuery for its other upsides, has a slightly less straightforward (but not more costly) method for implementing Write-Audit-Publish than some data warehouses (namely Snowflake and Iceberg catalogs), because of it's dataset/schema renaming restrictions and thus doesn't support two main WAP methods: doesn't support atomic dataset/schema swaps (gold standard for efficient, zero-downtime WAP) found in Snowflake (and of sort in Iceberg) and doesn't effectively support the older 3-step dataset/schema rename (less robust, but very simple and solid for most cases). So the view promotion approach for Blue Green Deployment is the available option for zero-downtime or always-available WAP, not that it requires much change (as it's just an additional layer of views) or architectural investment since many organizations already use additional view layer(s) to support access control & data governance tools.

## Getting Started

#### Prerequisites:
- A Google Cloud account and project
- A GCS bucket and the BigQuery API enabled for this project
- A service account credential with the following IAM roles**:
    - BigQuery Data Editor
    - BigQuery User
    - Storage Object Admin
- Python version >= 3.9 (3.12 is recommended for Dagster)

> **Note**: These roles are for a development environment for simplicity's sake. GCP normally recommends you split these permissions up and use more fine-grained, custom roles.

#### Installation and Setup:

1.  **Clone the repo**:
    ```bash
    git clone [https://github.com/zhew9/gcp-analytics-sandbox.git](https://github.com/zhew9/gcp-analytics-sandbox.git)
    ```
2.  **Create and activate a virtual environment**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # depends on your OS
    ```
3.  **Install dependencies**:
    ```bash
    pip install -e ".[dev]"
    ```
4.  **Set up your environment variables**:
    * Download your GCP service account credentials JSON file.
    * Create a `.env` file in the root of the project with the following variables:
        ```
        GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
        LOCAL_DUCKDB="path/to/create/local/database.duckdb"       
        GCS_INGESTION_BUCKET="your_gcs_bucket_name"
        GOOGLE_CLOUD_PROJECT="your_gcp_project_id"          # can also be inferred from credentials
        BQ_DATASET_LOCATION="US"                            # multi-region (US, EU) or single region
        DEFAULT_PARTITION_START="2025-08-01"
        ```
    * Set your ENV VARIABLES accordingly
      - if you change `DEFAULT_PARTITION_START`, make sure to generate new static tables with your new date, requires rerunning the notebook: (initialization/data/generate_retail_catalog.ipynb) which also requires you to download a source dataset (~100 MB, link in the notebook - requires Kaggle account)
      - optional partition variables: `DEFAULT_PARTITION_END` takes a date like "YYYY-MM-DD", `DEFAULT_PARTITION_TIMEZONE` takes a TZ identifier like "America/Vancouver"
5.  **(Optional) Persist Dagster's state**: If you want to persist Dagster's run history across sessions, set the `DAGSTER_HOME` environment variable to a directory where you want to store this data. Optionally the directory can contain a `dagster.yaml` config ([more on config](https://docs.dagster.io/deployment/oss/oss-instance-configuration)).
    ```bash
    export DAGSTER_HOME="/path/to/your/dagster_home"
    ```
6.  **Run the Dagster UI**:
    ```bash
    dagster dev
    ```
    You can now access the Dagster UI at `http://localhost:3000` or a similar port.

---

### How to Run the Pipeline

Once you are in the Dagster UI, the recommended way to run the pipeline is by using the pre-defined **Jobs**. Navigate to the `Jobs` tab and execute them in the specified order.

Note: I recommend running the contents of the `step_3_part2_dynamic_data_ingestion_job` Job manually through the UI or Assets and not through Jobs. Also if you encounter problems, you may want to run the partitions one at a time:
  - Currently Dagster handles partition order logic differently for backfills through Jobs and non-Jobs/directly through Assets, which can be challenging ... - currently you'd likely want to make 2 versions of backfill assets (1 for regular Asset backfills, which can respect order for multi-run backfills, and 1 for Job-selected backfills, which wants backfills to be single-run to respect ordering) as their logic is not consistent atm (but I only have 1 version)

**Concurrency Control**:
By default, this project uses a local, file-based DuckDB instance. Because this can lead to concurrency issues with simultaneous reads and writes, it's recommended to set a concurrency limit: In the Dagster UI, go to the **Deployment** page, select the **Concurrency** tab, and click the add a pool limit button:
* **Pool Name**: `write_duckdb`
* **Limit**: `1`
---

## Pipeline Overview/Summary:

This project is organized as a single Dagster code location containing three main modules, each representing a stage of the ELT lifecycle and comprising approximately 150 Dagster assets:

### 1. Initialization 
This is the first stage where the foundational setup and data generation (customizable) for scheduled runs and/or backfills occurs.

![image](/images/generation_img.png)

  - **BigQuery Dataset Creation**: Creates the necessary datasets in BigQuery for a multi-layered data architecture: `raw (bronze)`, `clean (bronze)`, `snapshots (bronze)`, `staging (silver)`, `prep (silver)`, `mart (gold)`, and `live (gold)`. Our ingestion jobs can also creates a dataset if necessary, but to prevent potential ingestion jobs trying to concurrently create the same dataset -> we're going to create our datasets up front.

  - **Static Data Initialization**: Pre-generates static data as Parquet files, such as product catalogs, categories, suppliers, and promotions and load them into a local DuckDB instance - mimicing a source OLTP (Online Transaction Processing) system.
    - Some static data is included as already generated parquet files, but the generation code is provided and can be customized and re-run

  - **Dynamic Data Generation:** Generates mostly-realistic, dynamic data for OLTP sales (customers and transactions). This mock data is created incrementally, either for mock daily workloads or for large mock backfills, and also loaded into the local DuckDB instance - which simulates an OLTP source system.

  - **Date Dimension Generation**: Creates a Date Dimension table in Python to include more comprehensive data like localized data such as holidays, which would be beyond the capabilities of something simpler (e.g. a dbt date spine), and writes the table to Google Cloud Storage (GCS) as a Parquet file.

### 2. Ingestion
In this stage, the data generated in the initialization stage is moved from its source to the data warehouse:

![image](/images/ingestion_img.png)

  - **DuckDB to GCS**: Uses a Dagster asset factory to create modular assets that extract data from the DuckDB tables and load it into GCS (Python libraries with server-side cursors and GCP clients). The assets are configured through a YAML file (gcs_load_config.yaml) which define the job's details like table names, date fields for partitioning, GCS paths, etc. - which once set up, offers a low-code & modular way to add new assets/jobs.

  - **GCS to BigQuery**: Once the data is in GCS, another asset factory is used to load the Parquet files into the corresponding tables in the BigQuery raw dataset. This is also configured via a YAML file (bq_load_config.yaml), which can define BigQuery settings from the table schema, type conversion, rounding scheme, partitioning scheme, clustering scheme, time-travel implementation, etc..

### 3. Transformation
This is where the raw data is transformed and deployed to user-facing analytics.

![wap_setup_image](/images/transformation_img.png) 

- **Dagster dbt Integration**: Dagster parses `dbt`'s `manifest.json` to translate dbt models into dagster assets and dependencies, and also we add Dagster assets as dummy sources to dbt models to add asset to model mapping (and vice-versa). These assets orchestrates dbt (dbt-core) through Dagster's dbt CLI resource to run dbt models, snapshots, and tests in our warehouse.

- **Multi-layered Architecture**: The dbt models are organized into a multi-layered data architecture, to better suit WAP/blue green deployment workflows. Data architectures with more layers are definitely not new but are becoming more popular - [Tim Hiebenthal wrote a great article on this](https://open.substack.com/pub/handsondata/p/how-to-structure-your-data-transformations?utm_campaign=post&utm_medium=web):
  
    - **`raw`**: The source data ingested as-is into BigQuery (append-only)

    - **`clean`**: This layer handles the deduplication of raw data that is bound for batch CDC (dbt snapshots, in this case) where we still want the data as close-to-source as possible, but incremental snapshotting require deduplication

    - **`snapshots`**: This layer creates 'dbt snapshots' which are used to capture changes in mutable source data over time, creating basic SCD2s - which we use for later SCD tables.

    - **`staging`**: This layer performs basic transformations like renaming columns and casting data types.

    - **`prep`**: This layer performs more complex, or frequent compute-heavy transformations and joins to prepare the data for the final dimensional models. If we have to rebuild marts for auditing (for Write-Audit-Publish), we want to reduce the amount of transformations being rebuilt/re-run where possible - one way is to move more compute to upstream layers like prep, where applicable.

    - **`mart`**: The final dimensional models (fact and dimension tables) are built in this layer, creating a star schema for analytics. The project mostly includes core fct/dim tables atm but I will probably add a few examples of extended tables like different date-spined SCDs, customer churn, lifetime value tables, and the like in the future.

    - **`live`**: This layer consists of views that point to the current production data marts, providing a stable interface for downstream consumers like BI tools or ML workflows.

- **Write-Audit-Publish (WAP) and Blue-Green Deployment**: Our pipeline implements WAP using a Blue Green deployment strategy for our audit environments, where we swap between two datasets to act as our `live/production` environment and our `audit/pre-prod` environments, for WAP we we write our transformations to the `audit` environment then after auditing, we promote the `audit` dataset to `live` by changing the source dataset of our downstream Views.

  - We use an orchestrated process to store our Blue Green deployment state (in our case, a custom Dagster Resource reading from/writing to from BigQuery dataset labels/metadata and also to a local YAML file) to determine which of the two datasets is the logical `live/prod` dataset that our current downstream views point to, and which is the logical `audit/pre-prod` dataset that we will use to write our transformations and audit/test our data before publishing
  
  - **(Pre-Write)**: After determining the desired `audit/pre-prod` environemnt, we need to prepare the `audit/pre-prod` dataset:
    - **(Pre-Write)**: Make sure the `audit` dataset is empty (either delete & recreate the dataset or empty it's contents, deleting the whole dataset is usually recommended)
    - **(Partial-Write)**: We then zero-copy `clone` over our incremental tables from `live` to the now empty `audit` dataset (which is merely a metadata operation, letting us avoid slow and costly full-rebuilds of large tables) and then we're ready to run transformations on our incremental models/tables (non-incremental tables ofc don't need this setup)
  - **(Write)**: We run our transformations and build our tables to the `audit/pre-prod` environment/dataset
  - **(Audit)**: We run the test on the results of our tables in the `audit` dataset
    - If we fail, then depending on where we failed we might have to repeat our setup of deleting the dataset/emptying it's contents and `zero-copy clone` our incremental tables again
  - **(Publish)**: If they pass the audit then we run the transformations that change the tables, that the downstream `live_views` point to, from the current `live_mart` to the current `audit_mart`
  - **(Publish)**: We then we swap the logical `live/audit` status of the two physical mart datasets (the physical datasets `..._mart_blue` or `..._mart_green` respectively)
    - **(Post-Publish)**: After a delayed period post-publish/swap, you may want to or have to drop the `old live` aka the `current audit` dataset to prevent storage costs from from accruing from the differences between your original table and it's clones before the start of the next WAP cycle - where we clear the `current audit` dataset again. Most data warehouses charge for data differences between clones, but usually tables don't diverge  that much between WAP cycles/if your pipeline is least daily, but maybe your tables are gigantic, or the budget is really tight and you really need to save storage spend.

## Customization & Extended Examples

I have it setup for a transaction-based, retail sales example with basic seasonality, but you can easily modify and extend both the data generation and orchestration examples and orchestrate your modifications immediately and locally. A smooth local development workflow is one of the great strengths of using Dagster, especially with DuckDB.

And if you're looking for more things to do after setting up and running the pipeline then here are some potential suggestions, from past personal PoCs, that might potentially interest you:

> *Note: many of these suggestions require services/resouces out of the GCP free tier, and you will need to enable these APIs/services on GCP and add additional IAM permissions/roles.

- **Try out BQ w/Apache Iceberg**: Explore lakehouse interoperability and cross-cloud features of BigQuery/GCP. (e.g. try reading your BQ Iceberg tables from AWS Athena or other engines)
- **Try out streaming CDC**: If you want to use a GCP service, setup Datastream, or setup your own source connector (e.g. Debezium), message queue (Kafka, Pub/Sub, etc) and BigQuery sink connector (Dataflow, Flink, or w/e streaming engine)
- **Experiment with different models**: Test the performance/cost of different degrees of denormalization in your data marts to better match known downstream query patterns and cost-saving priorities.
- **Explore GCP's ML Features**: Use BigQuery ML with classic models, multi-modal embeddings, or even setup BigQuery tables to be a vector store for RAG (Retrieval Augmented Generation) to use with the latest versions of Gemini.

## Future Roadmap

Short to Medium-term:

- Will try to fix potential bugs that crop up.
- Will try to add to the README, flesh/edit sections that were written hastily, add/improve images?, and fix grammar errors and typos.
- Will probably add/edit code comments or Dagster descriptions and add dbt model docs

Medium to Long-term:

- I'll revisit and keep the repo up to date if parts of Dagster/dbt/BQ/GCP become deprecated/replaced and add links to potential source documentation changes.
- I will eventually run through the core pipeline for an article or something to add more detailed explanations.
- I will probably expand the sandbox examples to add update generation to mutable tables (currently most functionally act as SCD1s atm even though they are modeled as SCD2s).
- Will probably try to add additional data quality checks and tests to other layers besides basic audit tests.
- Might refactor some areas, for this sandbox version/before the next 

Long-term:
- If there is enough interest in this type of pipeline/sandbox, then probably next year I will make a similar pipeline for either Snowflake (probably w/ AWS), Databricks (probably w/ Azure), or Iceberg (probably AWS w/ Athena) - depending on feedback/circumstances. 

>Feel free to leave your feedback, and even if I am busy for the next while, I'll at least try to incorporate any constructive feedback into the next sandbox pipeline.
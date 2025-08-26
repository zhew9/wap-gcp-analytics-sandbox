from faker import Faker
from datetime import date, datetime, time, timedelta
from dagster_duckdb import DuckDBResource
from pathlib import Path
from collections import defaultdict
from decimal import Decimal
from typing import Iterator
import polars as pl
import gc
import random
import dagster as dg
import duckdb as db
import uuid
#constants for many generation cases
import gcp_analytics_sandbox.initialization.assets.constants as constants
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition

@dg.asset(
    name="generate_customers",
    kinds={"python","duckdb"},
    group_name="dynamic_OLTP_generation",
    partitions_def=default_partition_definition,
    pool="write_duckdb",
    deps=[
        dg.AssetDep(
            "generate_customers",
            partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)
        ),
        dg.AssetDep("create_bq_datasets")
    ],
    automation_condition=dg.AutomationCondition.eager(),
    backfill_policy=dg.BackfillPolicy.single_run()
)
def generate_mock_customers(context: dg.AssetExecutionContext, write_duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    Generate new mock customer/user data for the following OLTP tables:
        customers
    
    For now, customization (e.g. number of customers and rate of growth) requires changing the values 
    in the code, but I may change it to take values from a config file down the line

    """

    context.log.info(f"Running for partition range: {sorted(context.partition_keys)}")

    with write_duckdb.get_connection() as conn:
        
        records = context.instance.fetch_materializations(context.asset_key,limit=1).records
        prev_customers = None
        
        if records:
            latest_record = records[0]
            materialization = latest_record.asset_materialization
            prev_customers = materialization.metadata.get("base_customers").value
            context.log.info(f"previous base_customers is: {prev_customers}")
        else:
            context.log.info(f"No materialization record found for previous runs of '{context.asset_key}'.")
        
        base_customers = prev_customers if prev_customers else 2000

        backfill_range = []
        seasonality_range = []
        total_customers_generated = 0

        for partition_key in sorted(context.partition_keys):

            context.log.info(f"Running for partition: {partition_key}")

            # for generating customers, by default I am basing how many customers I generate each time on how many I've 
            # previously generated, multiplied by a growth_rate, and a basic date seasonality weight

            # fetching the metadata from the last materialization of this asset, only one of many ways in Dagster
            # to pass data between assets and/or across sessions (e.g. explicit external storage, etc.)
            # for more details on using materializations's metadata - check out the comments in: asset_factory_bq_load.py
            
            # if context.asset_partition_key_range.start == partition_key:
            #     context.log.info(f"First partition run for '{context.asset_key}'.")
            #     context.log.info(f"Current Partition: {partition_key} of range {context.asset_partition_key_range.start} to {context.asset_partition_key_range.end}.")
            #     base_customers = 2000
            # else:

            base_date = datetime.strptime(partition_key, "%Y-%m-%d").date()
            seasonality_weight = constants.get_seasonality_weights(base_date)
            growth_rate = 1.002
            base_customers = round(base_customers * growth_rate)
            customers_this_run = round(base_customers * seasonality_weight)

            conn.execute(f"CREATE TABLE IF NOT EXISTS customers {constants.CUSTOMERS_SCHEMA};")

            prev_max_customer_id = conn.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
            if prev_max_customer_id:
                context.log.info(f"previous max(customer_id) is: {prev_max_customer_id}")
                max_user_id = prev_max_customer_id
            else: 
                context.log.info("customers table is empty")
                max_user_id = 0
        
            fake = Faker("en_CA")
            fake.seed_instance(42)
            # or whatever value might be fitting for your memory, e.g. dg.EnvVar('MAX_CHUNK_SIZE').get_value() 
            max_chunk_size = dg.EnvVar('MAX_CHUNK_SIZE').get_value() or 32768
            customers = defaultdict(list)
            for i in range(customers_this_run):
                if "customer_id" in customers and len(customers["customer_id"]) >= max_chunk_size:
                    customers_df = pl.DataFrame(customers)
                    conn.register("customers_df", customers_df)
                    try:
                        conn.execute("INSERT INTO customers BY NAME SELECT * FROM customers_df;")
                    except db.Error as e:
                        raise dg.Failure(
                            description=f"A DuckDB error occurred while inserting polars dataframe to table: 'customers'.",
                            metadata={
                                "failed_table": "customers",
                                "num_entries": len(customers_df),
                                "error_type": type(e).__name__,
                                "error": str(e),
                            }
                        ) from e
                    
                    conn.unregister("customers_df")
                    del(customers_df)
                    customers.clear()
                    gc.collect()   

                # customers = [customer_id, customer_uuid, first_name, last_name, email, province, gender, 
                # date_of_birth, signup_platform, registration_date, date_modified]
                max_user_id += 1
                cust_uuid = str(uuid.uuid5(constants.NAMESPACE_CUSTOMERS, str(max_user_id) ))
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = f"{first_name.lower()}{last_name.lower()}{max_user_id}@example.com"
                # can also use fake.province_abbr()
                province = fake.random_element(elements=constants.PROVINCES)
                gender = fake.random_element(elements=["M", "F"])
                date_of_birth = fake.date_of_birth(minimum_age=16, maximum_age=100)
                signup_platform = fake.random_element(elements=constants.PLATFORMS)

                # can't use random_timestamp if generating transactions for customers created on the same day, 
                # will cause SCD2 tables not to recognize the customer surrogate key for the time 
                # period if the transaction is given an earlier timestamp than the customer creation:

                # so either, we lag transaction generation behind 1 day or we have to use
                # strict bounds for when we create customers, happening before we create transactions
                # for them, to simplify for now I will make customer generation start in the morning
                # and have the timestamps generated for transactions guaranteed to be after 

                registration_date = constants.generate_timestamp_full_day(base_date)
                date_modified = registration_date

                customers["customer_id"].append(max_user_id)
                customers["customer_uuid"].append(cust_uuid)
                customers["first_name"].append(first_name)
                customers["last_name"].append(last_name)
                customers["email"].append(email)
                customers["province"].append(province)
                customers["date_of_birth"].append(date_of_birth)
                customers["gender"].append(gender)
                customers["status"].append("Active")
                customers["signup_platform"].append(signup_platform)
                customers["registration_date"].append(registration_date)
                customers["date_modified"].append(date_modified)
                customers["date_deleted"].append(None)

            if "customer_id" in customers and len(customers["customer_id"]) > 0:
                customers_df = pl.DataFrame(customers)
                conn.register("customers_df", customers_df)
                
                try:
                    conn.execute("INSERT INTO customers BY NAME SELECT * FROM customers_df;")
                except db.Error as e:
                    raise dg.Failure(
                        description=f"A DuckDB error occurred while inserting polars dataframe to table: 'customers'.",
                        metadata={
                            "failed_table": "customers",
                            "num_entries": len(customers_df),
                            "error_type": type(e).__name__,
                            "error": str(e),
                        }
                    ) from e

            backfill_range.append(partition_key)
            total_customers_generated += customers_this_run
            seasonality_range.append(seasonality_weight)

            # this is if you have want to have multi-run backfills (better for UI ran/non-job backfills)
            # if so, change the function's return type to : Iterator[dg.MaterializeResult] instead of dg.MaterializeResult

            # yield dg.MaterializeResult(
            #     metadata={
            #         "partition_key": partition_key,
            #         "customers_generated": customers_this_run,
            #         "base_customers": base_customers,
            #         "user_growth_rate": growth_rate,
            #         "seasonality_weight": seasonality_weight,
            #     }
            # )

        return dg.MaterializeResult(
                metadata={
                    "partition_range": backfill_range,
                    "total_customers_generated": total_customers_generated,
                    "base_customers": base_customers,
                    "user_growth_rate": growth_rate,
                    "seasonality_weights": seasonality_range,
                }
            )



#asset for generation transactions, which feed 4 different OLTP tables (transaction: headers, lines, discounts, and payments)
@dg.asset(
    name="generate_transactions",
    kinds={"python","duckdb"},
    group_name="dynamic_OLTP_generation",
    partitions_def=default_partition_definition,
    pool="write_duckdb",
    deps=[
        dg.AssetDep("load_pregenerated_catalog"),
        dg.AssetDep("generate_customers",
            partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)
        ),
        dg.AssetDep(
            "generate_transactions",
            partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)
        ),
    ],
    automation_condition=dg.AutomationCondition.eager(),
    backfill_policy=dg.BackfillPolicy.single_run(),
) 
def generate_transactions(context: dg.AssetExecutionContext, write_duckdb: DuckDBResource) -> dg.MaterializeResult:
    """
    Generate mock transaction data for the following OLTP tables:
    [transction_headers, transaction_lines, transaction_payments, transaction_discounts]
    
    For the DuckDB schemas for theses tables, refer to constants.py.
    """

    context.log.info(f"Running for partition range: {sorted(context.partition_keys)}")

    with write_duckdb.get_connection() as conn:

        partition_range = []
        total_transactions_generated = 0
        seasonality_range = []

        for partition_key in sorted(context.partition_keys):

            context.log.info(f"Running for partition: {partition_key}")

            # if metadata from previous materializations/runs exist, use the previous materializations metadata
            # else set base case
            # if context.asset_partition_key_range.start == partition_key:
            #     context.log.info(f"First partition run for '{context.asset_key}'.")
            #     context.log.info(f"Current Partition: {partition_key} of range {context.asset_partition_key_range.start} to {context.asset_partition_key_range.end}.")
            #     base_transactions = 4000
            # else:
            records = context.instance.fetch_materializations(context.asset_key,limit=1).records
            prev_transactions = None
            
            if records:
                latest_record = records[0]
                materialization = latest_record.asset_materialization
                prev_transactions = materialization.metadata.get("base_transactions").value
            else:
                context.log.info(f"No materialization record found for previous runs of '{context.asset_key}'.")
                context.log.info(f"Current Partition: {partition_key} of range {context.asset_partition_key_range.start} to {context.asset_partition_key_range.end}.")
            
            base_transactions = prev_transactions if prev_transactions else 4000

            base_date = datetime.strptime(partition_key, "%Y-%m-%d").date()
            seasonality_weight = constants.get_seasonality_weights(base_date)
            growth_rate = 1.002

            noon_datetime = datetime.combine(base_date, time(12, 0))

            valid_customers_query = f"""
                SELECT customer_id
                FROM (
                    SELECT customer_id
                    FROM customers
                    WHERE registration_date < '{noon_datetime}'
                ) AS filtered_customers
                TABLESAMPLE 100000 ROWS
            """

            customer_ids_df = conn.execute(valid_customers_query).pl()
            customer_ids = customer_ids_df["customer_id"].to_list()

            if not customer_ids:

                context.log.info(f"Skipping this partition: No customers exist before this date {base_date}")
                # only for multirun backfills

                # yield dg.MaterializeResult(
                #     metadata={
                #         "partition_key": partition_key,
                #         "transactions_generated": 0,
                #         "base_transactions": base_transactions,
                #         "transactions_growth_rate": growth_rate,
                #         "seasonality_weight": seasonality_weight,
                #     }
                # )
                continue
            
        
            # create tables if they don't exist
            initial_tables = ["employees", "transaction_headers", "transaction_lines", "transaction_payments", "transaction_discounts"]
            for table_name in initial_tables:
                try:
                    constant_name = f"{table_name.upper()}_SCHEMA"
                    # Use getattr to safely get the constant from the 'schemas' module.
                    # The third argument to getattr is a default value if the constant is not found.
                    table_schema = getattr(constants, constant_name, None)      
                    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" {table_schema};')
                    context.log.info(f"Table '{table_name}' was created successfully.")
                except db.Error as e:
                    # Failure Point 1: DuckDB specific error (SQL syntax, type mismatch, etc.)
                    raise dg.Failure(
                        description=f"A DuckDB error occurred creating table: '{table_name}'.",
                        metadata={
                            "failed_table": table_name,
                            "error_type": type(e).__name__,
                            "error": str(e),
                        }
                    ) from e

            # fetch supporting data for generating transactions
            header_promotions_df = conn.execute("SELECT * FROM promotions WHERE discount_level = 'ORDER' AND promotion_group_id IS NULL").pl()
            single_promotions_df = conn.execute("SELECT * FROM promotions WHERE discount_level = 'LINE_ITEM' AND promotion_group_id IS NULL").pl()
            # add row indexes, for use in random selection
            single_promotions_df = single_promotions_df.with_row_index()
            header_promotions_df = header_promotions_df.with_row_index()
            group_promotions_df = conn.execute("SELECT * FROM promotions WHERE discount_level = 'LINE_ITEM' AND promotion_group_id IS NOT NULL").pl()
            suppliers_products_df = conn.execute("SELECT products.product_id, suppliers_products.supplier_id, products.unit_price FROM products JOIN suppliers_products ON products.product_id = suppliers_products.product_id").pl()
            # split products into groups to emulate customer preference types/groupings
            product_suppliers = suppliers_products_df["supplier_id"].unique().to_list()
            product_groupings = {
                group: suppliers_products_df.filter(pl.col("supplier_id") == group) for group in product_suppliers
            }
            valid_promotional_products_query = """
            -- Query 1: Get product_ids from categories in the promotion
            SELECT
                product_id, promotion_group_id
            FROM
                categories_products
            JOIN
                promotion_groups ON categories_products.category_id = promotion_groups.category_id
            WHERE
                promotion_groups.group_type = 'Category'
            UNION
            -- Query 2: Get product_ids directly from the promotion products table
            SELECT
                product_id, promotion_group_id
            FROM
                promotion_groups_products;
            """
            valid_promotional_products = conn.execute(valid_promotional_products_query).pl()
            # channels_df = conn.execute("SELECT channel_id FROM channels").pl()
            # channels = channels_df["channel_id"].unique.to_list()
            # hard-coding channels to ignore (in-store/type 2) for our example is digital-only 
            channels = [1,3,4]
            payment_types_df = conn.execute("SELECT payment_type_id, type_name FROM payment_types").pl()
            
            # max_id = conn.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
            # max_customer_id = max_id if max_id else 0

            max_id = conn.execute("SELECT MAX(transaction_id) FROM transaction_headers").fetchone()[0]
            max_transaction_id = max_id if max_id else 0
            max_id = conn.execute("SELECT MAX(transaction_line_id) FROM transaction_lines").fetchone()[0]
            max_transaction_line_id = max_id if max_id else 0
            max_id = conn.execute("SELECT MAX(transaction_discount_id) FROM transaction_discounts").fetchone()[0]
            max_transaction_discount_id = max_id if max_id else 0
            max_id = conn.execute("SELECT MAX(transaction_payment_id) FROM transaction_payments").fetchone()[0]
            max_transaction_payment_id = max_id if max_id else 0
            
            context.log.info(f"max_transaction_id: {max_transaction_id}, max_transaction_line_id: {max_transaction_line_id}, max_transaction_discount_id: {max_transaction_discount_id}, max_transaction_payment_id: {max_transaction_payment_id}")

            transaction_headers = defaultdict(list)
            transaction_lines = defaultdict(list)
            transaction_payments = defaultdict(list)
            transaction_discounts = defaultdict(list)
            
            chunk_size = 0
            # or whatever value makes sense for your compute's memory
            max_chunk_size = dg.EnvVar('MAX_CHUNK_SIZE').get_value() or 32768
            table_names = ["transaction_headers", "transaction_lines", "transaction_payments", "transaction_discounts"]
            # total transactions will be affected by seasonlity
            # context.log.info(f"seasonality: {seasonality}")
            context.log.info(f"starting loop for generating transactions")

            base_transactions = round(base_transactions * growth_rate)
            transactions_this_run = round(base_transactions * seasonality_weight)
            context.log.info(f"transactions_this_run: {transactions_this_run}")
            
            for i in range(transactions_this_run):
                if chunk_size >= max_chunk_size:
                    context.log.info(f"chunk size: {chunk_size}")
                    chunk_size = 0
                    #commit all the dictionaries to duckdb
                    list_dicts = [transaction_headers, transaction_lines, transaction_payments, transaction_discounts]
                    for i, batch_dict in enumerate(list_dicts):
                        try:
                            batch_df = pl.DataFrame(batch_dict)
                            conn.register("batch_df", batch_df)
                            conn.execute(f"INSERT INTO {table_names[i]} BY NAME SELECT * FROM batch_df")
                            conn.unregister("batch_df")
                            del(batch_df)
                            batch_dict.clear()
                            gc.collect()
                            # We need to free the memory from this batch before loading the next one
                            # to keep the asset's overall memory footprint low.
                        except db.Error as e:
                            raise dg.Failure(description=f"A DuckDB error occurred while loading table: '{table_names[i]}'.",
                                metadata={
                                    "failed_table": table_names[i],
                                    "error_type": type(e).__name__,
                                    "error": str(e),
                                }
                            ) from e                     
                    
                # create transaction header but don't populate the aggregate fields yet
                # select a random: customer, channel, etc. for use for other parts of the transaction
                max_transaction_id += 1
                if i == 0:
                    context.log.info(f"first entry for max_transaction_id: {max_transaction_id}")
                transaction_uuid = str(uuid.uuid5(constants.NAMESPACE_TRANSACTIONS, str(max_transaction_id) ))
                channel_id = random.choices(channels, weights=constants.CHANNEL_WEIGHTS, k=1)[0]
                transaction_date = constants.generate_timestamp_second_half(base_date)
                customer_id = random.choice(customer_ids)
                customer_type = customer_id % len(constants.CUSTOMER_TYPE_WEIGHTS)

                # transaction lines will also be affected by seasonality
                # create 1..n transaction lines which will select a random: product, discount (weighted, relatively low)

                num_lines = 1 + max(1, round(seasonality_weight * random.choices(constants.NUM_LINES, weights=constants.NUM_LINES_WEIGHTS, k=1)[0]))
                gross_sub_total = Decimal(0.0000)
                total_discounts = Decimal(0.0000)

                for i in range(1,num_lines):
                    max_transaction_line_id += 1
                    line_number = i
                    product_group_number = random.choices(product_suppliers, weights=constants.CUSTOMER_TYPE_WEIGHTS[customer_type], k=1)[0]
                    product = product_groupings[product_group_number].sample(n=1)
                    product_id = product.select("product_id").item()
                    pre_discount_unit_price = product.select("unit_price").item()
                    quantity = random.choices(constants.QUANTITY, weights=constants.QUANTITY_WEIGHTS, k=1)[0]
                    
                    gross_line_total = pre_discount_unit_price * quantity
                    if gross_line_total < 0:
                        # shouldn't be possible unless the product catalog contain's a negative price
                        raise dg.Failure(
                            description=f"ERROR negative gross line_total: {gross_line_total}, unit_price: {pre_discount_unit_price}, product_id: {product_id} "
                        )
                    gross_sub_total += gross_line_total
                    # create a transaction line entry for a transaction line's gross sales

                    transaction_lines["transaction_line_id"].append(max_transaction_line_id)
                    transaction_lines["transaction_id"].append(max_transaction_id)
                    transaction_lines["line_number"].append(line_number)
                    transaction_lines["product_id"].append(product_id)
                    transaction_lines["quantity"].append(quantity)
                    transaction_lines["unit_price"].append(pre_discount_unit_price)
                    transaction_lines["gross_line_total"].append(gross_line_total)
                    transaction_lines["transaction_date"].append(transaction_date)
                    transaction_lines["date_modified"].append(transaction_date)

                    chunk_size +=1
                    # now to check if we generate any discounts entries for the products sold
                    
                    # first check if the product belongs to a promotion                
                    promo_row = valid_promotional_products.filter(pl.col("product_id") == product_id)
                    discount_row = None

                    # if the product is not part of a promotion, randomly pick a discount or no discount (50:50)
                    if promo_row.is_empty():
                        # create a row index so we can use a random number to select entries/rows
                        discount_index = random.randrange(2*len(single_promotions_df))
                        if discount_index < len(single_promotions_df):
                            discount_row = single_promotions_df.filter(pl.col("index") == discount_index)
                    else:
                        # simplified: select only one/the first promotion to apply if it belongs to multiple groups
                        promotion_group_id = promo_row.head(1).select("promotion_group_id").item()
                        discount_row = group_promotions_df.filter(pl.col("promotion_group_id") == promotion_group_id)

                    if discount_row is not None and isinstance(discount_row, pl.DataFrame):
                        discount_amount = Decimal(0.0000)
                        discount_quantity = quantity
                        if discount_row.select("discount_pool").item() == "FIXED":
                            discount_price = discount_row.select("discount_value").item()
                            discount_price = max(Decimal(0.0000), discount_price)
                            discount_amount = (pre_discount_unit_price - discount_price) * discount_quantity
                        elif discount_row.select("discount_pool").item() == "PERCENT":
                            discount_price = pre_discount_unit_price * (1 - (discount_row.select("discount_value").item()/100))
                            discount_price = max(Decimal(0.0000), discount_price)
                            discount_amount = (pre_discount_unit_price - discount_price) * discount_quantity
                        elif discount_row.select("discount_pool").item() == "FLAT":
                            discount_price = pre_discount_unit_price - discount_row.select("discount_value").item()
                            discount_price = max(Decimal(0.0000), discount_price)
                            discount_amount = (pre_discount_unit_price - discount_price) * discount_quantity
                        elif discount_row.select("discount_pool").item() == "BOGO":
                            discount_price = pre_discount_unit_price * (1 - (discount_row.select("discount_value").item() /100))
                            discount_price = max(Decimal(0.0000), discount_price)
                            discount_quantity = discount_quantity//2
                            quantity = quantity - discount_quantity
                            discount_amount = (pre_discount_unit_price - discount_price) * discount_quantity
                        else:
                            # optionally, raise an error
                            context.log.info(f"ERROR: discount with unrecognized 'discount_pool' type: {discount_row.select("discount_pool").item()}")
                        
                        #create a transction_discount entry
                        max_transaction_discount_id += 1
                        promotion_id = discount_row.select("promotion_id").item()
                        
                        transaction_discounts["transaction_discount_id"].append(max_transaction_discount_id)
                        transaction_discounts["promotion_id"].append(promotion_id)
                        transaction_discounts["transaction_line_id"].append(max_transaction_line_id)
                        transaction_discounts["transaction_id"].append(None)
                        # might be unnecessary besides BOGOs, otherwise this is equal transaction line's quantity
                        transaction_discounts["discounted_units"].append(discount_quantity)
                        transaction_discounts["discount_amount"].append(discount_amount)
                        # not generating stacking mock discounts, but it's possible to model them
                        transaction_discounts["discount_sequence"].append(1)
                        transaction_discounts["date_applied"].append(transaction_date)
                        transaction_discounts["date_modified"].append(transaction_date)

                        # add discounts to total discounts, for transaction header use later
                        total_discounts += discount_amount
                        chunk_size+=1

                # end transaction_lines for loop
            
                temp_net_total = gross_sub_total - total_discounts
                # back to the header, out of loop for lines
                # if header level discounts exist
                header_discount_row = None
                if header_promotions_df.is_empty() == False:
                    header_discount_amount = 0
                    # randomly generate if we should apply header_discount
                    header_discount_index = random.randrange(10*len(header_promotions_df))
                    if header_discount_index < len(header_promotions_df):
                        # needs a row index inserted
                        header_discount_row = header_promotions_df.filter(pl.col("index") == header_discount_index)
                        if header_discount_row.select("discount_pool").item() == "FLAT":
                            header_discount_amount = header_discount_row.select("discount_value").item()
                        elif header_discount_row.select("discount_pool").item() == "PERCENT":
                            header_discount_amount = temp_net_total * min(1, (header_discount_row.select("discount_value").item() /100))
                        else:
                            raise dg.Failure(
                                description=f"ERROR: header discount with unrecognized 'discount_pool' type: {header_discount_row.select("discount_pool").item()}"
                                )
                        
                        # create a transaction_discount entry for the header discount
                        max_transaction_discount_id += 1
                        promotion_id = header_discount_row.select("promotion_id").item()

                        transaction_discounts["transaction_discount_id"].append(max_transaction_discount_id)
                        transaction_discounts["promotion_id"].append(promotion_id)
                        # transaction_line_id is None for headers
                        transaction_discounts["transaction_line_id"].append(None)
                        transaction_discounts["transaction_id"].append(max_transaction_id)
                        transaction_discounts["discounted_units"].append(None)
                        transaction_discounts["discount_amount"].append(header_discount_amount)
                        # default to 1, if we apply multiple at the header-level then we'd increment
                        # sequence isn't used to separate line discounts from header discounts
                        transaction_discounts["discount_sequence"].append(1)
                        transaction_discounts["date_applied"].append(transaction_date)
                        transaction_discounts["date_modified"].append(transaction_date)

                        total_discounts += header_discount_amount
                        chunk_size += 1

                # back to the header, calculate the aggregate fields

                # simplifying tax amount to 12%, but different provinces can have different tax rates
                # and for some retail catalogs, different items may have different tax rates according
                # to localized laws
                net_total = max(Decimal(0.0000), gross_sub_total - total_discounts)
                net_tax_amount = max(Decimal(0.0000), net_total * Decimal(0.1200))
                total_amount = max(Decimal(0.0000), net_total + net_tax_amount)

                # a holdover from when I was testing other custom logic, left here in case in case anyone wants to use it
                if net_tax_amount is None:
                    context.log.info(f"ERROR: net_tax_amount: {net_tax_amount}, gross_sub_total: {gross_sub_total}, total_amount: {total_amount}")
                    discounts_troubleshoot = []
                    if header_discount_row is not None and isinstance(header_discount_row, pl.DataFrame):
                        discounts_troubleshoot.append(header_discount_row.row)
                    raise dg.Failure(
                    description=f"An error occurred when generating entries. Tax amount is Null",
                            metadata={
                                "net_tax_amount": net_tax_amount,
                                "gross_sub_total": gross_sub_total,
                                "total_amount": total_amount,
                                "discounts_list": discounts_troubleshoot
                            }
                    )

                #create a header entry
                transaction_headers["transaction_id"].append(max_transaction_id)
                transaction_headers["transaction_uuid"].append(transaction_uuid)
                transaction_headers["channel_id"].append(channel_id)
                transaction_headers["employee_id"].append(None)
                transaction_headers["customer_id"].append(customer_id)
                transaction_headers["gross_subtotal_amount"].append(gross_sub_total)
                transaction_headers["total_tax_amount"].append(net_tax_amount)
                transaction_headers["total_amount"].append(total_amount)
                transaction_headers["transaction_type"].append("Purchase")
                transaction_headers["original_transaction_id"].append(None)
                transaction_headers["transaction_date"].append(transaction_date)
                transaction_headers["date_modified"].append(transaction_date)
                
                chunk_size+= 1

                #create payment methods
                if total_amount > 0:         
                    num_payments = random.choices(constants.NUM_PAYMENTS, weights=constants.NUM_PAYMENTS_WEIGHTS, k=1)[0]
                else:
                    num_payments = 1
                payment_options = set(payment_types_df["payment_type_id"].to_list())
                for i in range(num_payments):
                    max_transaction_payment_id += 1
                    payment_type_id = payment_options.pop()
                    # we're just going for a simplified example here for different payment types
                    # but otherwise you could add customizable logic to model more complex mock behaviour
                    amount_paid = (total_amount / num_payments)

                    # context.log.info(f"amount paid: {amount_paid}, total amount: {total_amount}, num payments: {num_payments}")

                    transaction_payments["transaction_payment_id"].append(max_transaction_payment_id)
                    transaction_payments["transaction_id"].append(max_transaction_id)
                    transaction_payments["payment_type_id"].append(payment_type_id)
                    transaction_payments["amount_paid"].append(amount_paid)
                    transaction_payments["payment_provider_reference"].append(None)
                    transaction_payments["payment_date"].append(transaction_date)
                    transaction_payments["date_modified"].append(transaction_date)
                    
                    chunk_size += 1
            # end of outer for loop
            context.log.info(f"exitting loop with chunk_size: {chunk_size}")
            # commit any remaining entries, out of the for loop, to duckdb
            if chunk_size > 0:
                list_dicts = [transaction_headers, transaction_lines, transaction_payments, transaction_discounts]
                for i, batch_dict in enumerate(list_dicts):
                    batch_df = pl.DataFrame(batch_dict)
                    try:
                        conn.register("batch_df", batch_df)
                        conn.execute(f"INSERT INTO {table_names[i]} BY NAME SELECT * FROM batch_df")
                        conn.unregister("batch_df")
                        del(batch_df)
                        batch_dict.clear()
                        gc.collect()
                    except db.Error as e:
                        raise dg.Failure(description=f"A DuckDB error occurred while loading table: '{table_names[i]}'.",
                            metadata={
                                "failed_table": table_names[i],
                                "error_type": type(e).__name__,
                                "error": str(e),
                            }
                        ) from e
                    
            # end connection
            partition_range.append(partition_key)
            total_transactions_generated += transactions_this_run
            seasonality_range.append(seasonality_weight)

        return dg.MaterializeResult(
            metadata={
                "partition_range": partition_range,
                "total_transactions_generated": total_transactions_generated,
                "base_transactions": base_transactions,
                "transactions_growth_rate": growth_rate,
                "seasonality_weights": seasonality_range,
            }
        )